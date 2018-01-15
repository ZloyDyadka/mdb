package mdb

import (
	"io"
	"net"
	"strings"
	"sync/atomic"
	"time"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
)

type Session struct {
	MaxConnectRetries int
	RetryInterval     time.Duration
	originSession     *mgo.Session
	refreshing        int32
}

//Origin returns origin mgo session
func (s *Session) Origin() *mgo.Session {
	return s.originSession
}

func (s *Session) LiveServers() []string {
	return s.originSession.LiveServers()
}

func (s *Session) DB(name string) *Database {
	mgoDB := s.originSession.DB(name)
	return &Database{
		Name:     name,
		originDB: mgoDB,
		Session:  s.with(mgoDB.Session),
	}
}

func (s *Session) Login(credential *mgo.Credential) error {
	lastErr := s.execWithRetry(func() error {
		return s.originSession.Login(credential)
	})

	return lastErr
}

func (s *Session) LogoutAll() {
	s.originSession.LogoutAll()
}

func (s *Session) ResetIndexCache() {
	s.originSession.ResetIndexCache()
}

func (s *Session) New() *Session {
	return s.with(s.originSession.New())
}

func (s *Session) Copy() *Session {
	return s.with(s.originSession.Copy())
}

func (s *Session) Clone() *Session {
	return s.with(s.originSession.Clone())
}

func (s *Session) Close() {
	s.originSession.Close()
}

func (s *Session) Refresh() {
	s.originSession.Refresh()
}

func (s *Session) SetMode(consistency mgo.Mode, refresh bool) {
	s.originSession.SetMode(consistency, refresh)
}

func (s *Session) Mode() mgo.Mode {
	return s.originSession.Mode()
}

func (s *Session) SetSyncTimeout(d time.Duration) {
	s.originSession.SetSyncTimeout(d)
}

func (s *Session) SetSocketTimeout(d time.Duration) {
	s.originSession.SetSocketTimeout(d)
}

func (s *Session) SetCursorTimeout(d time.Duration) {
	s.originSession.SetCursorTimeout(d)
}

func (s *Session) SetPoolLimit(limit int) {
	s.originSession.SetPoolLimit(limit)
}

func (s *Session) SetBypassValidation(bypass bool) {
	s.originSession.SetBypassValidation(bypass)
}

func (s *Session) SetBatch(n int) {
	s.originSession.SetBatch(n)
}

func (s *Session) SetPrefetch(p float64) {
	s.originSession.SetPrefetch(p)
}

func (s *Session) Safe() (safe *mgo.Safe) {
	return s.originSession.Safe()
}

func (s *Session) SetSafe(safe *mgo.Safe) {
	s.originSession.SetSafe(safe)
}

func (s *Session) EnsureSafe(safe *mgo.Safe) {
	s.originSession.EnsureSafe(safe)
}

func (s *Session) Run(cmd interface{}, result interface{}) error {
	lastErr := s.execWithRetry(func() error {
		return s.originSession.Run(cmd, result)
	})

	return lastErr
}

func (s *Session) SelectServers(tags ...bson.D) {
	s.originSession.SelectServers(tags...)
}

//Ping does not retry
func (s *Session) Ping() error {
	return s.originSession.Ping()
}

func (s *Session) Fsync(async bool) error {
	lastErr := s.execWithRetry(func() error {
		return s.originSession.Fsync(async)
	})

	return lastErr
}

func (s *Session) FsyncLock() error {
	lastErr := s.execWithRetry(func() error {
		return s.originSession.FsyncLock()
	})

	return lastErr
}

func (s *Session) FsyncUnlock() error {
	lastErr := s.execWithRetry(func() error {
		return s.originSession.FsyncUnlock()
	})

	return lastErr
}

func (s *Session) FindRef(ref *mgo.DBRef) *Query {
	return &Query{originQuery: s.originSession.FindRef(ref), session: s}
}

func (s *Session) DatabaseNames() ([]string, error) {
	var names []string
	lastErr := s.execWithRetry(func() error {
		var err error
		names, err = s.originSession.DatabaseNames()
		return err
	})

	return names, lastErr
}

func (s *Session) BuildInfo() (mgo.BuildInfo, error) {
	var info mgo.BuildInfo
	lastErr := s.execWithRetry(func() error {
		var err error
		info, err = s.originSession.BuildInfo()
		return err
	})

	return info, lastErr
}

func (s *Session) with(session *mgo.Session) *Session {
	return &Session{
		originSession:     session,
		MaxConnectRetries: s.MaxConnectRetries,
		RetryInterval:     s.RetryInterval,
		refreshing:        0,
	}
}

func (s *Session) execWithRetry(f func() error) error {
	err := f()

	if isNetworkError(err) {
		for i := 0; i < s.MaxConnectRetries; i++ {
			lastErr := f()

			if isNetworkError(lastErr) {
				if ok := s.refresh(); !ok {
					time.Sleep(s.RetryInterval)
				}

				continue
			}

			return lastErr
		}
	}

	return err
}

func (s *Session) refresh() bool {
	if atomic.CompareAndSwapInt32(&s.refreshing, 0, 1) {
		return false
	}

	s.originSession.Refresh()
	if err := s.Ping(); err != nil {
		return false
	}

	atomic.StoreInt32(&s.refreshing, 0)

	return true
}

//TODO: Check if closed by user
//mgo common error is eof Closed explicitly
func isNetworkError(err error) bool {
	if err == nil {
		return false
	}

	if err == mgo.ErrNotFound || err == mgo.ErrCursor || mgo.IsDup(err) {
		return false
	}

	if err == io.EOF {
		return true
	}

	if err, ok := err.(net.Error); ok && err.Timeout() {
		return false
	}

	if _, ok := err.(*net.OpError); ok {
		return true
	}

	e := strings.ToLower(err.Error())
	if strings.HasPrefix(e, "closed") || strings.HasSuffix(e, "closed") {
		return true
	}

	return false
}

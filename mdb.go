// The mdb package provides mongodb driver
// with automatic reconnection and retries when connection is break.
// It is wrapper over the mgo driver and exposes exactly the same API.
//
// mdb performs automatic retries under certain conditions.
// Mainly, if an error is returned by the client (connection errors).
// Otherwise, the error is returned.
package mdb

import (
	"github.com/globalsign/mgo"
	"time"
)

const (
	DefaultMaxRetries    = 2
	DefaultRetryInterval = time.Second * 2
)

type Option func(session *Session)

func Dial(mgoUrl string, opts ...Option) (*Session, error) {
	mgoSession, err := mgo.Dial(mgoUrl)
	if err != nil {
		return nil, err
	}

	sess := Wrap(mgoSession, DefaultMaxRetries, DefaultRetryInterval)
	applyOptions(sess, opts...)

	return sess, nil
}

func DialWithInfo(info *mgo.DialInfo, opts ...Option) (*Session, error) {
	mgoSession, err := mgo.DialWithInfo(info)
	if err != nil {
		return nil, err
	}

	sess := Wrap(mgoSession, DefaultMaxRetries, DefaultRetryInterval)
	applyOptions(sess, opts...)

	return sess, nil
}

func DialWithTimeout(mgoUrl string, timeout time.Duration, opts ...Option) (*Session, error) {
	mgoSession, err := mgo.DialWithTimeout(mgoUrl, timeout)
	if err != nil {
		return nil, err
	}

	sess := Wrap(mgoSession, DefaultMaxRetries, DefaultRetryInterval)
	applyOptions(sess, opts...)

	return sess, nil
}

func Wrap(sess *mgo.Session, maxRetries int, retryInterval time.Duration) *Session {
	return &Session{
		RetryInterval:     retryInterval,
		MaxConnectRetries: maxRetries,
		originSession:     sess,
		refreshing:        0,
	}
}

func MaxRetries(max int) func(session *Session) {
	return func(s *Session) {
		s.MaxConnectRetries = max
	}
}

func RetryInterval(interval time.Duration) func(session *Session) {
	return func(s *Session) {
		s.RetryInterval = interval
	}
}

func applyOptions(s *Session, opts ...Option) {
	for _, o := range opts {
		o(s)
	}
}

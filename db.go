package mdb

import (
	"github.com/globalsign/mgo"
)

type Database struct {
	Name     string
	Session  *Session
	originDB *mgo.Database
}

//Origin returns origin mgo database
func (db *Database) Origin() *mgo.Database {
	return db.originDB
}

func (db *Database) CreateView(view string, source string, pipeline interface{}, collation *mgo.Collation) error {
	lastErr := db.Session.execWithRetry(func() error {
		return db.originDB.CreateView(view, source, pipeline, collation)
	})

	return lastErr
}

func (db *Database) With(s *Session) *Database {
	return &Database{
		Name:     db.Name,
		originDB: db.originDB.With(s.originSession),
		Session:  s,
	}
}

//TODO: Implement auto-retry for GridFS
func (db *Database) GridFS(prefix string) *mgo.GridFS {
	panic("not implemented")
}

func (db *Database) Run(cmd interface{}, result interface{}) error {
	lastErr := db.Session.execWithRetry(func() error {
		return db.originDB.Run(cmd, result)
	})

	return lastErr
}

func (db *Database) Login(user, pass string) error {
	lastErr := db.Session.execWithRetry(func() error {
		return db.originDB.Login(user, pass)
	})

	return lastErr
}

func (db *Database) Logout() {
	db.originDB.Logout()
}

func (db *Database) UpsertUser(user *mgo.User) error {
	lastErr := db.Session.execWithRetry(func() error {
		return db.originDB.UpsertUser(user)
	})

	return lastErr
}

func (db *Database) AddUser(username, password string, readOnly bool) error {
	lastErr := db.Session.execWithRetry(func() error {
		return db.originDB.AddUser(username, password, readOnly)
	})

	return lastErr
}

func (db *Database) RemoveUser(user string) error {
	lastErr := db.Session.execWithRetry(func() error {
		return db.originDB.RemoveUser(user)
	})

	return lastErr
}

func (db *Database) DropDatabase() error {
	lastErr := db.Session.execWithRetry(func() error {
		return db.originDB.DropDatabase()
	})

	return lastErr
}

func (db *Database) FindRef(ref *mgo.DBRef) *Query {
	return &Query{
		session:     db.Session,
		originQuery: db.originDB.FindRef(ref),
	}
}

func (db *Database) CollectionNames() ([]string, error) {
	var names []string
	lastErr := db.Session.execWithRetry(func() error {
		var err error
		names, err = db.originDB.CollectionNames()
		return err
	})

	return names, lastErr
}

func (db *Database) Close() {
	db.Session.Close()
}

func (db *Database) with(mgoDB *mgo.Database) *Database {
	return &Database{
		Name:     db.Name,
		Session:  db.Session.with(mgoDB.Session),
		originDB: mgoDB,
	}
}

func (db *Database) C(name string) *Collection {
	c := &Collection{
		Name:             name,
		session:          db.Session,
		Database:         db,
		originCollection: db.originDB.C(name),
	}

	return c
}

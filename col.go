package mdb

import (
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
)

type Collection struct {
	Name             string
	Database         *Database
	session          *Session
	originCollection *mgo.Collection
}

//Origin returns origin mgo collection
func (c *Collection) Origin() *mgo.Collection {
	return c.originCollection
}

func (c *Collection) With(s *mgo.Session) *Collection {
	mgoCollection := c.originCollection.With(s)
	return &Collection{
		Name:             c.Name,
		Database:         c.Database.with(mgoCollection.Database),
		originCollection: mgoCollection,
		session:          c.session.with(s),
	}
}

func (c *Collection) Repair() *Iter {
	iter := &Iter{session: c.session.with(c.Database.originDB.Session)}
	c.session.execWithRetry(func() error {
		iter.originIter = c.originCollection.Repair()
		return iter.originIter.Err()
	})

	return iter
}

func (c *Collection) Insert(docs ...interface{}) error {
	lastErr := c.session.execWithRetry(func() error {
		return c.originCollection.Insert(docs...)
	})

	return lastErr
}

func (c *Collection) Count() (int, error) {
	var n int
	lastErr := c.session.execWithRetry(func() error {
		var err error
		n, err = c.originCollection.Count()
		return err
	})

	return n, lastErr
}

func (c *Collection) Create(info *mgo.CollectionInfo) error {
	lastErr := c.session.execWithRetry(func() error {
		return c.originCollection.Create(info)
	})

	return lastErr
}

func (c *Collection) DropCollection() error {
	lastErr := c.session.execWithRetry(func() error {
		return c.originCollection.DropCollection()
	})

	return lastErr
}

func (c *Collection) DropIndexName(name string) error {
	lastErr := c.session.execWithRetry(func() error {
		return c.originCollection.DropIndexName(name)
	})

	return lastErr
}

func (c *Collection) DropAllIndexes() error {
	lastErr := c.session.execWithRetry(func() error {
		return c.originCollection.DropAllIndexes()
	})

	return lastErr
}

func (c *Collection) DropIndex(key ...string) error {
	lastErr := c.session.execWithRetry(func() error {
		return c.originCollection.DropIndex(key...)
	})

	return lastErr
}

func (c *Collection) EnsureIndex(index mgo.Index) error {
	lastErr := c.session.execWithRetry(func() error {
		return c.originCollection.EnsureIndex(index)
	})

	return lastErr
}

func (c *Collection) Pipe(pipe interface{}) *Pipe {
	p := c.originCollection.Pipe(pipe)
	return &Pipe{
		session:    c.session.with(c.Database.originDB.Session),
		originPipe: p,
	}
}

func (c *Collection) Remove(selector interface{}) error {
	lastErr := c.session.execWithRetry(func() error {
		return c.originCollection.Remove(selector)
	})

	return lastErr
}

func (c *Collection) RemoveId(id interface{}) error {
	return c.Remove(bson.D{{"_id", id}})
}

func (c *Collection) Indexes() ([]mgo.Index, error) {
	var indexes []mgo.Index
	lastErr := c.session.execWithRetry(func() error {
		var err error
		indexes, err = c.originCollection.Indexes()
		return err
	})

	return indexes, lastErr
}

func (c *Collection) RemoveAll(selector interface{}) (*mgo.ChangeInfo, error) {
	var info *mgo.ChangeInfo
	lastErr := c.session.execWithRetry(func() error {
		var err error
		info, err = c.originCollection.RemoveAll(selector)
		return err
	})

	return info, lastErr
}

func (c *Collection) UpdateId(id interface{}, update interface{}) (err error) {
	return c.Update(bson.M{"_id": id}, update)
}

func (c *Collection) Update(selector interface{}, update interface{}) error {
	lastErr := c.session.execWithRetry(func() error {
		return c.originCollection.Update(selector, update)
	})

	return lastErr
}

func (c *Collection) UpdateAll(selector interface{}, update interface{}) (*mgo.ChangeInfo, error) {
	var info *mgo.ChangeInfo
	lastErr := c.session.execWithRetry(func() error {
		var err error
		info, err = c.originCollection.UpdateAll(selector, update)
		return err
	})

	return info, lastErr
}

func (c *Collection) Upsert(selector interface{}, update interface{}) (*mgo.ChangeInfo, error) {
	var info *mgo.ChangeInfo
	lastErr := c.session.execWithRetry(func() error {
		var err error
		info, err = c.originCollection.Upsert(selector, update)
		return err
	})

	return info, lastErr
}

func (c *Collection) UpsertId(id interface{}, update interface{}) (*mgo.ChangeInfo, error) {
	var info *mgo.ChangeInfo
	lastErr := c.session.execWithRetry(func() error {
		var err error
		info, err = c.originCollection.UpsertId(id, update)
		return err
	})

	return info, lastErr
}

func (c *Collection) EnsureIndexKey(key ...string) (err error) {
	return c.EnsureIndex(mgo.Index{Key: key})
}

func (c *Collection) FindId(id interface{}) *Query {
	return c.Find(bson.D{{"_id", id}})
}

func (c *Collection) Find(query interface{}) *Query {
	return &Query{
		session:     c.session.with(c.Database.originDB.Session),
		originQuery: c.originCollection.Find(query),
	}
}

func (c *Collection) NewIter(session *mgo.Session, firstBatch []bson.Raw, cursorId int64, err error) *Iter {
	return &Iter{
		originIter: c.originCollection.NewIter(session, firstBatch, cursorId, err),
		session:    c.session.with(session),
	}
}

func (c *Collection) Bulk() *mgo.Bulk {
	return c.originCollection.Bulk()
}

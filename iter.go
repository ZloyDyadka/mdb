package mdb

import (
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
)

type Iter struct {
	originIter *mgo.Iter
	session    *Session
}

//Origin returns origin mgo iter
func (i *Iter) Origin() *mgo.Iter {
	return i.originIter
}

func (i *Iter) Err() (err error) {
	return i.originIter.Err()
}

func (i *Iter) Close() error {
	lastErr := i.session.execWithRetry(func() error {
		return i.originIter.Close()
	})

	return lastErr
}

func (i *Iter) State() (int64, []bson.Raw) {
	return i.originIter.State()
}

func (q *Query) Collation(collation *mgo.Collation) *Query {
	return &Query{
		session:     q.session,
		originQuery: q.originQuery.Collation(collation),
	}
}

func (i *Iter) Done() bool {
	return i.originIter.Done()
}

func (i *Iter) Timeout() bool {
	return i.originIter.Timeout()
}

func (i *Iter) Next(result interface{}) bool {
	var next bool
	i.session.execWithRetry(func() error {
		next = i.originIter.Next(result)
		return i.Err()
	})

	return next
}

func (i *Iter) For(result interface{}, f func() error) error {
	lastErr := i.session.execWithRetry(func() error {
		return i.originIter.For(result, f)
	})

	return lastErr
}

func (i *Iter) All(result interface{}) error {
	lastErr := i.session.execWithRetry(func() error {
		return i.originIter.All(result)
	})

	return lastErr
}

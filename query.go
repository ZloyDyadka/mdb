package mdb

import (
	"time"

	"github.com/globalsign/mgo"
)

type Query struct {
	originQuery *mgo.Query
	session     *Session
}

//Origin returns origin mgo query
func (q *Query) Origin() *mgo.Query {
	return q.originQuery
}

func (q *Query) Batch(n int) *Query {
	q.originQuery = q.originQuery.Batch(n)
	return q
}

func (q *Query) Prefetch(p float64) *Query {
	q.originQuery = q.originQuery.Prefetch(p)
	return q
}

func (q *Query) Skip(n int) *Query {
	q.originQuery = q.originQuery.Skip(n)
	return q
}

func (q *Query) Limit(n int) *Query {
	q.originQuery = q.originQuery.Limit(n)
	return q
}

func (q *Query) Select(selector interface{}) *Query {
	q.originQuery = q.originQuery.Select(selector)
	return q
}

func (q *Query) Sort(fields ...string) *Query {
	q.originQuery = q.originQuery.Sort(fields...)
	return q
}

func (q *Query) Explain(result interface{}) error {
	lastErr := q.session.execWithRetry(func() error {
		return q.originQuery.Explain(result)
	})

	return lastErr
}

func (q *Query) Hint(indexKey ...string) *Query {
	q.originQuery = q.originQuery.Hint(indexKey...)
	return q
}

func (q *Query) SetMaxScan(n int) *Query {
	q.originQuery = q.originQuery.SetMaxScan(n)
	return q
}

func (q *Query) SetMaxTime(d time.Duration) *Query {
	q.originQuery = q.originQuery.SetMaxTime(d)
	return q
}

func (q *Query) Snapshot() *Query {
	q.originQuery = q.originQuery.Snapshot()
	return q
}

func (q *Query) Comment(comment string) *Query {
	q.originQuery = q.originQuery.Comment(comment)
	return q
}

func (q *Query) LogReplay() *Query {
	q.originQuery = q.originQuery.LogReplay()
	return q
}

func (q *Query) One(result interface{}) error {
	err := q.session.execWithRetry(func() error {
		return q.originQuery.One(result)
	})

	return err
}

func (q *Query) Count() (int, error) {
	var n int
	lastErr := q.session.execWithRetry(func() error {
		var err error
		n, err = q.originQuery.Count()
		return err
	})

	return n, lastErr
}

func (q *Query) Iter() *Iter {
	i := &Iter{session: q.session}
	q.session.execWithRetry(func() error {
		i.originIter = q.originQuery.Iter()
		return i.originIter.Err()
	})

	return i
}

func (q *Query) Tail(timeout time.Duration) *Iter {
	i := &Iter{session: q.session}
	q.session.execWithRetry(func() error {
		i.originIter = q.originQuery.Tail(timeout)
		return i.originIter.Err()
	})

	return i
}

func (q *Query) Distinct(key string, result interface{}) error {
	lastErr := q.session.execWithRetry(func() error {
		return q.originQuery.Distinct(key, result)
	})

	return lastErr
}

func (q *Query) MapReduce(job *mgo.MapReduce, result interface{}) (*mgo.MapReduceInfo, error) {
	var info *mgo.MapReduceInfo
	lastErr := q.session.execWithRetry(func() error {
		var err error
		info, err = q.originQuery.MapReduce(job, result)
		return err
	})

	return info, lastErr
}

func (q *Query) Apply(change mgo.Change, result interface{}) (*mgo.ChangeInfo, error) {
	var info *mgo.ChangeInfo
	lastErr := q.session.execWithRetry(func() error {
		var err error
		info, err = q.originQuery.Apply(change, result)
		return err
	})

	return info, lastErr
}

func (q *Query) All(result interface{}) error {
	return q.Iter().All(result)
}

func (q *Query) For(result interface{}, f func() error) error {
	return q.Iter().For(result, f)
}

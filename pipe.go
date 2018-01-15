package mdb

import (
	"github.com/globalsign/mgo"
)

type Pipe struct {
	originPipe *mgo.Pipe
	session    *Session
}

//Origin returns origin mgo pipe
func (p *Pipe) Origin() *mgo.Pipe {
	return p.originPipe
}

func (p *Pipe) Iter() *Iter {
	i := &Iter{session: p.session}
	p.session.execWithRetry(func() error {
		i.originIter = p.originPipe.Iter()
		return i.originIter.Err()
	})

	return i
}

func (p *Pipe) All(result interface{}) error {
	return p.Iter().All(result)
}

func (p *Pipe) One(result interface{}) error {
	lastErr := p.session.execWithRetry(func() error {
		return p.originPipe.One(result)
	})

	return lastErr
}

func (p *Pipe) Explain(result interface{}) error {
	lastErr := p.session.execWithRetry(func() error {
		return p.originPipe.Explain(result)
	})

	return lastErr
}

func (p *Pipe) AllowDiskUse() *Pipe {
	return &Pipe{
		originPipe: p.originPipe.AllowDiskUse(),
		session:    p.session,
	}
}

func (p *Pipe) Batch(n int) *Pipe {
	return &Pipe{
		session:    p.session,
		originPipe: p.originPipe.Batch(n),
	}
}

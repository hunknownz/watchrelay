package publisher

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/hunknownz/watchrelay/event"
)

type Publisher struct {
	sync.Map
	sync.Once

	id atomic.Uint64
}

func (p *Publisher) Subscribe(ctx context.Context, establish func() (chan []event.IEvent, error)) (ch <-chan []event.IEvent, err error) {
	p.Do(func() {
		err = p.start(establish)
	})
	if err != nil {
		return nil, err
	}

	sub := make(chan []event.IEvent, 128)
	id := p.id.Add(1)
	p.Store(id, sub)
	go func() {
		<-ctx.Done()
		p.unsubscribe(id)
	}()

	return sub, nil
}

func (p *Publisher) start(establish func() (chan []event.IEvent, error)) error {
	ch, err := establish()
	if err != nil {
		return err
	}

	go p.broadcast(ch)

	return nil
}

func (p *Publisher) broadcast(ch chan []event.IEvent) {
	for events := range ch {
		p.Range(func(key, value interface{}) bool {
			id := key.(uint64)
			sub := value.(chan []event.IEvent)
			select {
			case sub <- events:
			default:
				// drop slow subscriber
				go p.unsubscribe(id)
			}
			return true
		})
	}
}

func (p *Publisher) unsubscribe(id uint64) {
	sub, ok := p.Load(id)
	if !ok {
		return
	}

	close(sub.(chan []event.IEvent))
}

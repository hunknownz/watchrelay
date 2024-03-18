package publisher

import (
	"context"
	"sync"

	"github.com/hunknownz/watchrelay/event"
	"github.com/hunknownz/watchrelay/resource"
)

type Publisher struct {
	sync.Map

	running bool
}

func Subscribe[T resource.IVersionedResource](pub *Publisher, ctx context.Context, establish func() (chan []event.IEvent, error)) (sub <-chan []*event.Event[T], err error) {
	if !p.running {
		err = p.start(establish)
		if err != nil {
			return nil, err
		}
	}

	var v T
	sub = make(chan []*event.Event[T], 128)
	resourceName := resource.GetResourceName(v)
	pub.Store(sub, resourceName)
	go func() {
		<-ctx.Done()
		pub.unsubscribe(sub)
	}()

	return sub, nil
}

func (p *Publisher) start(establish func() (chan []event.IEvent, error)) error {
	ch, err := establish()
	if err != nil {
		return err
	}

	go p.broadcast(ch)
	p.running = true

	return nil
}

func filter(events []event.IEvent, resourceName string) ([]event.IEvent, bool) {
	return events, true
}

func (p *Publisher) broadcast(ch chan []event.IEvent) {
	for events := range ch {
		p.Range(func(key, value interface{}) bool {
			sub := value.(chan []event.IEvent)
			select {
			case sub <- events:
			default:
				// drop slow subscriber
				go unsubscribe(p, sub)
			}
			return true
		})
	}
}

func unsubscribe[T resource.IVersionedResource](pub *Publisher, key chan []*event.Event[T]) {
	sub, ok := pub.Load(key)
	if !ok {
		return
	}

	close(sub.(chan []*event.Event[T]))
	pub.Delete(key)
}

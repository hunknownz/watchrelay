package publisher

import (
	"context"
	"errors"
	"sync"

	"github.com/hunknownz/watchrelay/event"
	"github.com/hunknownz/watchrelay/resource"
)

type Publisher struct {
	sync.Map

	running bool
}

type ISubscriber interface {
	Close()
	Send(pub *Publisher, events []event.IEvent, resourceName string) bool
}

type Subscriber[T resource.IVersionedResource] chan []*event.Event[T]

func (s Subscriber[T]) Close() {
	close(s)
}

func (s Subscriber[T]) Send(pub *Publisher, iEvents []event.IEvent, resourceName string) bool {
	events, ok := filter[T](iEvents, resourceName)
	if !ok {
		return true
	}

	select {
	case s <- events:
	default:
		// drop slow subscriber
		pub.unsubscribe(s)
	}
	return true
}

func Subscribe[T resource.IVersionedResource](pub *Publisher, ctx context.Context, establish func() (chan []event.IEvent, error)) (sub <-chan []*event.Event[T], err error) {
	if pub == nil {
		return nil, errors.New("watchrelay: Publisher is nil")

	}

	if !pub.running {
		err = pub.start(establish)
		if err != nil {
			return nil, err
		}
	}

	var v T
	subscriber := make(Subscriber[T], 128)
	sub = subscriber
	resourceName := resource.GetResourceName(v)
	pub.Store(ISubscriber(subscriber), resourceName)
	go func() {
		<-ctx.Done()
		pub.unsubscribe(subscriber)
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

func filter[T resource.IVersionedResource](events []event.IEvent, resourceName string) ([]*event.Event[T], bool) {
	var filtered []*event.Event[T]
	for _, e := range events {
		if e.GetResourceName() == resourceName {
			filtered = append(filtered, e.(*event.Event[T]))
		}
	}
	return filtered, len(filtered) > 0
}

func (p *Publisher) broadcast(ch chan []event.IEvent) {
	for events := range ch {
		p.Range(func(key, value interface{}) bool {
			sub := key.(ISubscriber)
			return sub.Send(p, events, value.(string))
		})
	}
}

func (p *Publisher) unsubscribe(key ISubscriber) {
	_, ok := p.Load(key)
	if !ok {
		return
	}

	key.Close()
	p.Delete(key)
}

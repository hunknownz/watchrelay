package watchrelay

import "sync"

type Publisher struct {
	sync.Mutex
	running bool
	subs    map[chan []*Event]struct{}
}

func NewEventHub() *Publisher {
	return &Publisher{
		subs: make(map[chan []*Event]struct{}),
	}
}

func (p *Publisher) Subscribe(sub chan []*Event, establish func() (chan []*Event, error)) {
	p.Lock()
	defer p.Unlock()

	if !p.running {

	}
	p.subs[sub] = struct{}{}
}

func (p *Publisher) start(establish func() (chan []*Event, error)) error {
	ch, err := establish()
	if err != nil {
		return err
	}

	go p.broadcast(ch)
	p.running = true
	return nil
}

func (p *Publisher) broadcast(ch chan []*Event) {
	for events := range ch {
		p.Lock()
		for sub := range p.subs {
			select {
			case sub <- events:
			default:
				// drop slow subscriber
				go p.Unsubscribe(sub)
			}
		}
		p.Unlock()
	}
}

func (p *Publisher) Unsubscribe(sub chan []*Event) {
	p.Lock()
	defer p.Unlock()

	if _, ok := p.subs[sub]; !ok {
		return
	}

	close(sub)
	delete(p.subs, sub)
}

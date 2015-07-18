package ncsp

import (
	"errors"
)

type Broadcaster struct {
	in        chan bool
	register  chan chan bool
	listeners []chan bool
}

func NewBroadcaster(cnt int) *Broadcaster {
	b := new(Broadcaster)
	b.in = make(chan bool)
	b.register = make(chan chan bool)
	b.listeners = make([]chan bool, 0)
	cntr := int(cnt)
	go func() {
		for {
			select {
			case v := <-b.in:
				if cntr == 0 {
					break
				}
				for _, c := range b.listeners {
					c <- v
				}
				cntr--
			case r := <-b.register:
				b.listeners = append(b.listeners, r)
			}
		}
	}()
	return b
}

func (b *Broadcaster) Write(v bool, cnt int) error {
	if len(b.listeners) >= cnt {
		b.in <- v
	} else {
		return errors.New("Not ready yet")
	}
	return nil
}

func (b *Broadcaster) Listen() chan bool {
	ch := make(chan bool)
	b.register <- ch
	return ch
}

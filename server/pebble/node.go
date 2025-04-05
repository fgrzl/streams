package pebble

import (
	"context"
	"errors"
	"sync"

	"github.com/fgrzl/streams/broker"
	"github.com/google/uuid"
)

type Node struct {
	id         uuid.UUID
	Bus        broker.Bus
	Service    Service
	Supervisor Supervisor
	Observer   *PebbleObserver
	quorum     Quorum
	disposed   sync.Once
}

func NewNode(bus broker.Bus, path string) (*Node, error) {
	ctx := context.Background()
	id := uuid.New()
	quorum := NewQuorum(id)
	supervisor := NewDefualtSupervisor(bus, quorum)
	service, err := NewService(path, supervisor)
	if err != nil {
		return nil, err
	}

	if err := service.Synchronize(ctx); err != nil {
		if !errors.Is(err, broker.ErrNoResponders) {
			service.Close()
			return nil, err
		}
	}

	observer, err := NewPebbleObserver(bus, service, quorum)
	if err != nil {
		service.Close()
		return nil, err
	}
	return &Node{
		id:         id,
		Bus:        bus,
		Service:    service,
		Supervisor: supervisor,
		Observer:   observer,
		quorum:     quorum,
	}, nil
}

func (n *Node) GetBus() broker.Bus {
	return n.Bus
}

func (n *Node) Close() error {
	var err error
	n.disposed.Do(func() {
		if n.Observer != nil {
			n.Observer.Close()
			n.Observer = nil
		}
		if n.Service != nil {
			n.Service.Close()
			n.Service = nil
		}
		n.Bus = nil
		n.Supervisor = nil
		n.quorum = nil
	})
	return err
}

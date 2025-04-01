package azure

import (
	"sync"

	"github.com/fgrzl/streams/broker"
	"github.com/google/uuid"
)

type Node struct {
	id       uuid.UUID
	Bus      broker.Bus
	Service  Service
	Observer Observer
	disposed sync.Once
}

func NewNode(bus broker.Bus, options *TableProviderOptions) (*Node, error) {

	service, err := NewService(bus, options)
	if err != nil {
		return nil, err
	}

	observer, err := NewDefaultObserver(bus, service)
	if err != nil {
		service.Close()
		return nil, err
	}

	return &Node{
		id:       uuid.New(),
		Bus:      bus,
		Service:  service,
		Observer: observer,
	}, nil
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
	})
	return err
}

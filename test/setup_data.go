package streams_test

import (
	"fmt"
	"log/slog"
	"path/filepath"
	"testing"

	"time"

	"github.com/fgrzl/enumerators"
	"github.com/fgrzl/logging"
	"github.com/fgrzl/streams"
	"github.com/fgrzl/streams/broker"
	"github.com/fgrzl/streams/server/pebble"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func configurations(t *testing.T) map[string]broker.Bus {
	logging.ConfigureLogging()
	return map[string]broker.Bus{
		"single":    singleInstance(t),
		"clustered": clusteredInstance(t),
	}
}

func singleInstance(t *testing.T) broker.Bus {
	bus := NewMockBus()
	newInstance(t, bus)
	return bus
}

func clusteredInstance(t *testing.T) broker.Bus {
	bus := NewMockBus()
	setupCluster(t, bus)
	return bus
}

func setupCluster(t *testing.T, bus broker.Bus) {
	var nodes []*pebble.Node
	for range 5 {
		nodes = append(nodes, newInstance(t, bus))
	}

	service := nodes[0]

	for service.Supervisor.GetActiveNodeCount() < 5 {
		time.Sleep(0)
	}
}

func newInstance(t *testing.T, bus broker.Bus) *pebble.Node {
	instancePath := filepath.Join(t.TempDir(), uuid.NewString())
	node, err := pebble.NewNode(bus, instancePath)
	require.NoError(t, err)
	t.Cleanup(func() {
		slog.Warn("Test Cleanup")
		node.Close()
	})
	return node
}

func setupConsumerData(t *testing.T, client streams.Client) {
	ctx := t.Context()

	for i := range 5 {
		for j := range 5 {
			space, segment, records := fmt.Sprintf("space%d", i), fmt.Sprintf("segment%d", j), generateRange(0, 1_000)
			results := client.Produce(ctx, space, segment, records)
			err := enumerators.Consume(results)
			require.NoError(t, err)
		}
	}
}

func generateRange(seed, count int) enumerators.Enumerator[*streams.Record] {
	return enumerators.Range(seed, count, func(i int) *streams.Record {
		return &streams.Record{
			Sequence: uint64(i + 1),
			Payload:  []byte(fmt.Sprintf("test data %d", i+1)),
		}
	})
}

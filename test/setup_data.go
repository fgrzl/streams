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
	"github.com/fgrzl/streams/server/azure"
	"github.com/fgrzl/streams/server/pebble"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func configurations(t *testing.T) map[string]broker.Bus {
	logging.ConfigureLogging()
	return map[string]broker.Bus{
		"azure":     azureInstance(t),
		"single":    singleInstance(t),
		"clustered": clusteredInstance(t),
	}
}

func azureInstance(t *testing.T) broker.Bus {
	bus := NewMockBus() // Replace with actual Azure bus initialization if needed

	// Default Azurite configuration for local testing
	accountName := "devstoreaccount1"
	accountKey := "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
	endpoint := "http://127.0.0.1:10002/devstoreaccount1"

	credential, err := azure.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		panic(err)
	}

	options := &azure.TableProviderOptions{
		Prefix:              "test",
		Table:               uuid.NewString(),
		Endpoint:            endpoint,
		SharedKeyCredential: credential,
	}

	instance, err := azure.NewNode(bus, options)
	require.NoError(t, err, "failed to create azure service instance")
	require.NotNil(t, instance, "azure service instance should not be nil")
	return bus
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

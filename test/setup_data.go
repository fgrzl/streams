package test

import (
	"fmt"
	"net"
	"os"
	"strconv"

	"github.com/fgrzl/streams/pkg/enumerators"
	"github.com/fgrzl/streams/pkg/models"
	"github.com/fgrzl/streams/pkg/util"
	"github.com/google/uuid"
)

func IsGitHubAction() bool {
	val, _ := strconv.ParseBool(os.Getenv("GITHUB_ACTIONS"))
	return val
}

func GetSampleEntries(seed int, count int) enumerators.Enumerator[*models.Entry] {

	correlationId := uuid.New()
	causationId := uuid.New()
	return enumerators.Range(seed, count, func(i int) *models.Entry {

		entryId := uuid.New()
		return &models.Entry{
			Sequence:      uint64(i + 1),
			Timestamp:     util.GetTimestamp(),
			EntryId:       entryId[:],
			CorrelationId: correlationId[:],
			CausationId:   causationId[:],
			Payload:       []byte(fmt.Sprintf("test entry %d", i)),
		}
	})
}

func GetSampleEntry(space string, partition string, sequence uint64) *models.Entry {
	entryId := uuid.New()
	correlationId := uuid.New()
	causationId := uuid.New()
	return &models.Entry{
		Sequence:      sequence,
		Timestamp:     util.GetTimestamp(),
		EntryId:       entryId[:],
		CorrelationId: correlationId[:],
		CausationId:   causationId[:],
		Payload:       []byte(fmt.Sprintf("test entry %v", entryId)),
	}

}

func GetAvailablePort() string {
	// Create a listener on an ephemeral port
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic("Could not find available port")
	}
	defer listener.Close() // Ensure the listener is closed

	// Retrieve the assigned address and port
	addr := listener.Addr().(*net.TCPAddr)
	return addr.AddrPort().String()
}

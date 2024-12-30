package util

import (
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/fgrzl/streams/pkg/config"
)

var (
	globalClock *clock
	once        sync.Once
)

// get the next monotonic timestamp
func GetTimestamp() int64 {
	return globalClock.getTimestamp()
}

// Clock keeps track of the application startup time.
type clock struct {
	start time.Time
}

// Initializes the global monotonic clock in the `init` function.
func init() {
	once.Do(func() {
		t := getCurrentTime()
		globalClock = &clock{start: t}
	})
}

// Get the monotonic timestamp in Unix milliseconds.
func (mc *clock) getTimestamp() int64 {
	elapsed := time.Since(mc.start)
	return mc.start.UnixMilli() + elapsed.Milliseconds()
}

// NTP constants
const (
	ntpEpochOffset = 2208988800 // Seconds between 1900 and 1970
)

// List of NTP servers to query
var ntpServers = []string{
	"time.google.com:123",
	"time.aws.com:123",
	"time.cloudflare.com:123",
	"time.windows.com:123",
}

func getCurrentTime() time.Time {
	server := config.GetTimeServer()

	if server == "system" {
		// Use the system clock
		return time.Now()
	}

	if server == "default" || server == "" {
		server = "time.google.com:123"
	}

	// Use the specified NTP server
	ntpTime, err := ntpTime(server)
	if err != nil {
		// Fall back to the system clock if the NTP server fails
		log.Printf("Failed to fetch time from NTP server (%s): %v. Falling back to system time.", server, err)
		return time.Now()
	}

	return ntpTime
}

// ntpTime fetches the current time from an NTP server
func ntpTime(server string) (time.Time, error) {
	// Connect to the NTP server
	conn, err := net.Dial("udp", server)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to connect to NTP server (%s): %w", server, err)
	}
	defer conn.Close()

	// Create an NTP request packet
	req := make([]byte, 48)
	req[0] = 0x1B // Set NTP version to 3 and client mode

	// Send the request
	_, err = conn.Write(req)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to send request to %s: %w", server, err)
	}

	// Read the response
	resp := make([]byte, 48)
	_, err = conn.Read(resp)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to read response from %s: %w", server, err)
	}

	// Extract the transmit timestamp (bytes 40-47 in the response)
	seconds := binary.BigEndian.Uint32(resp[40:44])
	fraction := binary.BigEndian.Uint32(resp[44:48])

	// Convert NTP time to Go time
	ntpSeconds := float64(seconds) + float64(fraction)/0x100000000
	unixSeconds := ntpSeconds - ntpEpochOffset
	return time.Unix(int64(unixSeconds), 0), nil
}

package serializers

import (
	"encoding/binary"
	"io"

	"github.com/fgrzl/streams/pkg/enumerators"
	"github.com/fgrzl/streams/pkg/models"

	"google.golang.org/protobuf/proto"
)

// entryIterator is the concrete implementation of EntryIterator.
type pageReader struct {
	stream  io.ReadCloser
	buf     []byte
	current *models.Entry
	err     error
}

// NewEntryIterator creates a new instance of EntryIterator.
func NewPageReader(stream io.ReadSeekCloser, position int64) enumerators.Enumerator[*models.Entry] {

	if position > 0 {
		stream.Seek(position, io.SeekStart)
	}

	return &pageReader{
		stream: stream,
		buf:    make([]byte, 4),
	}
}
func (e *pageReader) MoveNext() bool {

	// Read the length prefix (4 bytes).
	if _, err := io.ReadFull(e.stream, e.buf); err != nil {
		if err == io.EOF {
			return false
		}
		e.err = err
		return false
	}
	length := binary.LittleEndian.Uint32(e.buf)
	data := make([]byte, length)
	if _, err := io.ReadFull(e.stream, data); err != nil {
		e.err = err
		return true
	}

	// Unmarshal the protobuf data.
	entry := &models.Entry{}
	if err := proto.Unmarshal(data, entry); err != nil {
		e.err = err
	}
	e.current = entry

	return true
}
func (e *pageReader) Current() (*models.Entry, error) {
	return e.current, e.err
}

func (e *pageReader) Err() error {
	return e.err
}

func (enumerator *pageReader) Dispose() {
	enumerator.stream.Close()
}

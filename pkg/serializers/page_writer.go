package serializers

import (
	"encoding/binary"
	"io"

	"github.com/fgrzl/enumerators"
	"github.com/fgrzl/streams/pkg/models"

	"google.golang.org/protobuf/proto"
)

const indexInterval = 1024 * 1024 * 4

func Write(enumerator enumerators.Enumerator[*models.Entry], writer io.Writer) (*models.Page, error) {
	defer enumerator.Dispose()
	page := &models.Page{}

	var n int
	var position int
	var currentPosition int
	var lastIndexedPosition int

	lengthBuf := make([]byte, 4)

	for enumerator.MoveNext() {
		// Update the start position after the write
		position = currentPosition

		current, err := enumerator.Current()
		if err != nil {
			return nil, err
		}
		if current == nil {
			continue
		}

		data, err := proto.Marshal(current)
		if err != nil {
			return nil, err
		}

		dataLen := len(data)
		binary.LittleEndian.PutUint32(lengthBuf, uint32(dataLen))

		if n, err = writer.Write(lengthBuf); err != nil {
			return nil, err
		}
		currentPosition += n

		if n, err = writer.Write(data); err != nil {
			return nil, err
		}
		currentPosition += n

		// Update sparse index if the interval is met or exceeded
		if currentPosition-lastIndexedPosition >= indexInterval {
			page.Index = append(page.Index, &models.PageIndexEntry{Sequence: current.Sequence, Position: int64(position)})
			lastIndexedPosition = currentPosition
		}

		// Update page metadata
		page.Size += int64(len(data))
		if page.FirstSequence == 0 {
			page.FirstSequence = current.Sequence
		}
		if page.FirstTimestamp == 0 {
			page.FirstTimestamp = current.Timestamp
		}
		page.LastSequence = current.Sequence
		page.LastTimestamp = current.Timestamp
		page.Count++
	}
	page.Index = append(page.Index, &models.PageIndexEntry{Sequence: page.LastSequence, Position: int64(position)})
	return page, enumerator.Err()
}

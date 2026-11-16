package filequeue

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
)

// Frame layout on disk:
//
//	magic    [4]byte  = "SMQ1"
//	type     byte
//	length   uint32 (big endian) — payload length
//	payload  [length]byte
//	crc      uint32 (big endian) — IEEE CRC over type|length|payload
//
// Every frame is self-delimiting, so a torn tail write is detected by the
// checksum (or an unexpected EOF) and discarded on open.
var frameMagic = [4]byte{'S', 'M', 'Q', '1'}

const (
	headerLen = 4 + 1 + 4
	crcLen    = 4
)

var crcTable = crc32.MakeTable(crc32.IEEE)

// wal is a crash-safe write-ahead log. Every frame is length-prefixed and
// followed by a CRC32 checksum. A torn or corrupt tail frame is ignored on
// open, so recovery always ends at the last complete frame.
type wal struct {
	file  *os.File
	path  string
	fsync bool
}

// openWAL opens (creating if needed) the log file and verifies its tail.
// When a partial/corrupt frame is found, the file is truncated to the end of
// the last complete frame and corruptAt holds the byte offset where recovery
// cut the log (-1 when the log was intact).
func openWAL(path string, fsync bool) (*wal, int64, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, -1, fmt.Errorf("filequeue: open wal %s: %w", path, err)
	}

	w := &wal{
		file:  file,
		path:  path,
		fsync: fsync,
	}

	validEnd, corruptAt, err := w.verifyTail()
	if err != nil {
		_ = file.Close()

		return nil, -1, err
	}

	if corruptAt >= 0 {
		if err := file.Truncate(validEnd); err != nil {
			_ = file.Close()

			return nil, -1, fmt.Errorf("filequeue: truncate corrupt tail: %w", err)
		}
	}

	if _, err := file.Seek(validEnd, io.SeekStart); err != nil {
		_ = file.Close()

		return nil, -1, fmt.Errorf("filequeue: seek wal: %w", err)
	}

	return w, corruptAt, nil
}

// verifyTail scans the whole file frame by frame and returns the offset just
// beyond the last complete frame. corruptAt is >= 0 when something had to be
// discarded.
func (w *wal) verifyTail() (validEnd, corruptAt int64, err error) {
	file := w.file

	if _, errSeek := file.Seek(0, io.SeekStart); errSeek != nil {
		return 0, -1, fmt.Errorf("filequeue: rewind wal: %w", errSeek)
	}

	var offset int64

	for {
		header := make([]byte, headerLen)

		_, readErr := io.ReadFull(file, header)
		if errors.Is(readErr, io.EOF) {
			return offset, -1, nil
		}

		if errors.Is(readErr, io.ErrUnexpectedEOF) {
			// a partial header at the tail: everything before it is intact.
			return offset, offset, nil
		}

		if readErr != nil {
			return 0, -1, fmt.Errorf("filequeue: read frame header: %w", readErr)
		}

		var magic [4]byte

		copy(magic[:], header[:4])

		if magic != frameMagic {
			return offset, offset, nil
		}

		frameType := header[4]
		payloadLen := binary.BigEndian.Uint32(header[5:])

		trailer := make([]byte, payloadLen+crcLen)

		_, readErr = io.ReadFull(file, trailer)

		switch {
		case readErr == nil:
		case errors.Is(readErr, io.ErrUnexpectedEOF), errors.Is(readErr, io.EOF):
			// partial payload/crc: discard the incomplete frame.
			return offset, offset, nil
		default:
			return 0, -1, fmt.Errorf("filequeue: read frame payload: %w", readErr)
		}

		payload := trailer[:payloadLen]
		gotCRC := binary.BigEndian.Uint32(trailer[payloadLen:])

		wantCRC := crc32.Checksum(append(header[4:], payload...), crcTable)
		if gotCRC != wantCRC {
			return offset, offset, nil
		}

		if !validFrameType(frameType) {
			return offset, offset, nil
		}

		offset += int64(headerLen) + int64(len(trailer))
	}
}

func validFrameType(t byte) bool {
	switch frameType(t) {
	case frameEnqueue, frameInflight, frameComplete, frameRequeue,
		frameAbandon, framePace, frameSnapshot, frameDedup:
		return true
	default:
		return false
	}
}

// appendFrame writes one frame durably.
func (w *wal) appendFrame(typ frameType, payload []byte) error {
	var header [headerLen]byte

	copy(header[:4], frameMagic[:])

	header[4] = byte(typ)

	const maxFramePayload = 1<<32 - 1

	if len(payload) > maxFramePayload {
		return fmt.Errorf("filequeue: frame payload too large: %d", len(payload))
	}

	// Bounds were checked against maxFramePayload above, so the conversion is
	// safe by construction.
	binary.BigEndian.PutUint32(header[5:], uint32(len(payload))) //nolint:gosec // length validated against uint32 max

	crc := crc32.New(crcTable)
	if _, err := crc.Write(header[4:]); err != nil {
		return fmt.Errorf("filequeue: crc header: %w", err)
	}

	if _, err := crc.Write(payload); err != nil {
		return fmt.Errorf("filequeue: crc payload: %w", err)
	}

	var crcBytes [crcLen]byte

	binary.BigEndian.PutUint32(crcBytes[:], crc.Sum32())

	buf := make([]byte, 0, headerLen+len(payload)+crcLen)
	buf = append(buf, header[:]...)
	buf = append(buf, payload...)
	buf = append(buf, crcBytes[:]...)

	if _, err := w.file.Write(buf); err != nil {
		return fmt.Errorf("filequeue: write frame: %w", err)
	}

	if w.fsync {
		if err := w.file.Sync(); err != nil {
			return fmt.Errorf("filequeue: fsync frame: %w", err)
		}
	}

	return nil
}

// readFrames replays complete frames from the start of the log.
func (w *wal) readFrames(fn func(typ frameType, payload []byte) error) error {
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("filequeue: rewind wal for replay: %w", err)
	}

	for {
		header := make([]byte, headerLen)

		_, err := io.ReadFull(w.file, header)
		if errors.Is(err, io.EOF) {
			return nil
		}

		if err != nil {
			return fmt.Errorf("filequeue: replay header: %w", err)
		}

		ft := frameType(header[4])
		payloadLen := binary.BigEndian.Uint32(header[5:])

		trailer := make([]byte, payloadLen+crcLen)
		if _, err := io.ReadFull(w.file, trailer); err != nil {
			return fmt.Errorf("filequeue: replay payload: %w", err)
		}

		payload := trailer[:payloadLen]

		if err := fn(ft, payload); err != nil {
			return err
		}
	}
}

// closeWAL flushes and closes the log.
func (w *wal) closeWAL() error {
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("filequeue: sync on close: %w", err)
	}

	return w.file.Close()
}

// rotateWAL writes the snapshot as the first frame of a fresh log and
// atomically replaces the current log file.
func (w *wal) rotateWAL(snapshot []byte) error {
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("filequeue: sync before rotate: %w", err)
	}

	if err := w.file.Close(); err != nil {
		return fmt.Errorf("filequeue: close before rotate: %w", err)
	}

	finalPath := w.path
	tmpPath := finalPath + ".tmp"

	tmp, err := os.OpenFile(tmpPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("filequeue: create rotated wal: %w", err)
	}

	w.file = tmp
	w.path = tmpPath

	if err := w.appendFrame(frameSnapshot, snapshot); err != nil {
		return err
	}

	if err := os.Rename(tmpPath, finalPath); err != nil {
		return fmt.Errorf("filequeue: replace rotated wal: %w", err)
	}

	w.path = finalPath

	return nil
}

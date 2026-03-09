package domain

import (
	"encoding/binary"
	"io"
)

type Entry struct {
	key     []byte
	val     []byte
	deleted bool
}

func NewPutEntry(key, val []byte) *Entry {
	return &Entry{
		key:     key,
		val:     val,
		deleted: false,
	}
}

func NewDelEntry(key []byte) *Entry {
	return &Entry{
		key:     key,
		deleted: true,
	}
}

func (ent *Entry) Encode() []byte {
	data := make([]byte, 4+4+1+len(ent.key)+len(ent.val))
	binary.LittleEndian.PutUint32(data[0:4], uint32(len(ent.key)))
	copy(data[9:], ent.key)
	if ent.deleted {
		data[8] = 1
	} else {
		binary.LittleEndian.PutUint32(data[4:8], uint32(len(ent.val)))
		copy(data[9+len(ent.key):], ent.val)
	}
	return data
}

func (ent *Entry) Decode(r io.Reader) error {
	header := make([]byte, 9)
	if _, err := io.ReadFull(r, header); err != nil {
		return err
	}

	keyLen := binary.LittleEndian.Uint32(header[0:4])
	valLen := binary.LittleEndian.Uint32(header[4:8])
	deleted := header[8]

	data := make([]byte, keyLen+valLen)
	if _, err := io.ReadFull(r, data); err != nil {
		return err
	}
	ent.key = data[:keyLen]
	if deleted != 0 {
		ent.deleted = true
	} else {
		ent.deleted = false
		ent.val = data[keyLen:]
	}
	return nil
}

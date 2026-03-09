package domain

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
)

var (
	ErrBadCheckSum = errors.New("bad checksum")
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

/*
 | crc32  | key size | val size | deleted | key data | val data |
 |--------|----------|----------|---------|----------|----------|
 | 4 bytes| 4 bytes  | 4 bytes  | 1 byte  | ...      | ...      |
*/

func (ent *Entry) Encode() []byte {
	data := make([]byte, 4+4+4+1+len(ent.key)+len(ent.val))
	binary.LittleEndian.PutUint32(data[4:8], uint32(len(ent.key)))
	copy(data[4+4+4+1:], ent.key)
	if ent.deleted {
		data[4+4+4] = 1
	} else {
		binary.LittleEndian.PutUint32(data[8:12], uint32(len(ent.val)))
		copy(data[4+4+4+1+len(ent.key):], ent.val)
	}
	binary.LittleEndian.PutUint32(data[0:4], crc32.ChecksumIEEE(data[4:]))
	return data
}

func (ent *Entry) Decode(r io.Reader) error {
	header := make([]byte, 4+4+4+1)
	if _, err := io.ReadFull(r, header); err != nil {
		return err
	}

	checkSum := binary.LittleEndian.Uint32(header[0:4])
	keyLen := binary.LittleEndian.Uint32(header[4:8])
	valLen := binary.LittleEndian.Uint32(header[8:12])
	deleted := header[4+4+4]

	data := make([]byte, keyLen+valLen)
	if _, err := io.ReadFull(r, data); err != nil {
		return err
	}

	h := crc32.NewIEEE()
	h.Write(header[4:])
	h.Write(data)
	if h.Sum32() != checkSum {
		return ErrBadCheckSum
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

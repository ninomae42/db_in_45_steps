package domain

import (
	"encoding/binary"
	"errors"
	"slices"
)

type CellType uint8

const (
	TypeI64 CellType = 1
	TypeStr CellType = 2
)

var ErrIncompleteData = errors.New("expect more data")

type Cell struct {
	Type CellType
	I64  int64
	Str  []byte
}

/*
 Strのformat
 | Length  | Data |
 |---------|------|
 | 4 bytes | ...  |
*/

func (cell *Cell) Encode(toAppend []byte) []byte {
	switch cell.Type {
	case TypeI64:
		return binary.LittleEndian.AppendUint64(toAppend, uint64(cell.I64))
	case TypeStr:
		toAppend = binary.LittleEndian.AppendUint32(toAppend, uint32(len(cell.Str)))
		return append(toAppend, cell.Str...)
	default:
		panic("unreachable")
	}
}

func (cell *Cell) Decode(data []byte) (rest []byte, err error) {
	switch cell.Type {
	case TypeI64:
		if len(data) < 8 {
			return data, ErrIncompleteData
		}
		cell.I64 = int64(binary.LittleEndian.Uint64(data[0:8]))
		return data[8:], nil
	case TypeStr:
		if len(data) < 4 {
			return data, ErrIncompleteData
		}
		size := binary.LittleEndian.Uint32(data[0:4])
		if uint64(len(data)) < 4+uint64(size) {
			return data, ErrIncompleteData
		}
		cell.Str = slices.Clone(data[4 : 4+size])
		return data[4+size:], nil
	default:
		panic("unreachable")
	}
}

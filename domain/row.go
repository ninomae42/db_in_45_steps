package domain

import (
	"errors"
	"slices"
)

var (
	ErrExtraData     = errors.New("extra data remaining")
	ErrKeyTooShort   = errors.New("key is too short")
	ErrOutOfRange    = errors.New("out of range")
	ErrRowNotFound   = errors.New("row is not found")
	ErrTableMismatch = errors.New("table name mismatch")
)

type Schema struct {
	Table string
	Cols  []Column
	PKey  []int // to specify which columns are primary key
}

type Column struct {
	Name string
	Type CellType
}

type Row []Cell

func (schema *Schema) NewRow() Row {
	return make(Row, len(schema.Cols))
}

func (row Row) EncodeKey(schema *Schema) []byte {
	check(len(row) == len(schema.Cols))
	key := append([]byte(schema.Table), 0x00)
	for _, idx := range schema.PKey {
		cell := row[idx]
		check(cell.Type == schema.Cols[idx].Type)
		key = append(key, byte(cell.Type)) // avoid 0xff
		key = cell.EncodeKey(key)
	}
	key = append(key, 0x00) // -infinity
	return key
}

func EncodeKeyPrefix(schema *Schema, prefix []Cell, positive bool) []byte {
	key := append([]byte(schema.Table), 0x00)
	for i, cell := range prefix {
		check(cell.Type == schema.Cols[schema.PKey[i]].Type)
		key = append(key, byte(cell.Type)) // avoid 0xff
		key = cell.EncodeKey(key)
	}
	if positive {
		key = append(key, 0xff) // +infinity
	} // -infinity
	return key
}

func (row Row) EncodeVal(schema *Schema) (val []byte) {
	check(len(row) == len(schema.Cols))
	for idx, cell := range row {
		if !slices.Contains(schema.PKey, idx) {
			check(cell.Type == schema.Cols[idx].Type)
			val = cell.EncodeVal(val)
		}
	}
	return
}

func (row Row) DecodeKey(schema *Schema, key []byte) (err error) {
	check(len(row) == len(schema.Cols))

	// テーブル名+終端文字(\x00)の長さを検証
	tablePrefixLen := len(schema.Table) + 1
	if len(key) < tablePrefixLen {
		return ErrOutOfRange
	}

	// keyに含まれるテーブル名とschemaのテーブル名が一致するか検証
	if schema.Table+"\x00" != string(key[:tablePrefixLen]) {
		return ErrOutOfRange
	}

	key = key[len(schema.Table)+1:]

	for _, idx := range schema.PKey {
		cell := NewCell(schema.Cols[idx].Type)
		if !(0 < len(key) && key[0] == byte(cell.Type)) {
			return errors.New("bad key")
		}
		key = key[1:]
		if key, err = cell.DecodeKey(key); err != nil {
			return
		}
		row[idx] = cell
	}
	if !(len(key) == 1 && key[0] == 0x00) {
		return errors.New("bad key")
	}

	return nil
}

func (row Row) DecodeVal(schema *Schema, val []byte) (err error) {
	check(len(row) == len(schema.Cols))

	for idx, column := range schema.Cols {
		if slices.Contains(schema.PKey, idx) {
			continue
		}
		cell := NewCell(column.Type)
		if val, err = cell.DecodeVal(val); err != nil {
			return
		}
		row[idx] = cell
	}

	if len(val) != 0 {
		return ErrExtraData
	}

	return nil
}

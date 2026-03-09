package domain

import (
	"errors"
	"slices"
)

var (
	ErrExtraData     = errors.New("extra data remaining")
	ErrKeyTooShort   = errors.New("key is too short")
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

func (row Row) EncodeKey(schema *Schema) (key []byte) {
	key = append(key, []byte(schema.Table)...)
	key = append(key, 0x00)
	check(len(row) == len(schema.Cols))
	for idx, cell := range row {
		if slices.Contains(schema.PKey, idx) {
			check(cell.Type == schema.Cols[idx].Type)
			key = cell.Encode(key)
		}
	}
	return
}
func (row Row) EncodeVal(schema *Schema) (val []byte) {
	check(len(row) == len(schema.Cols))
	for idx, cell := range row {
		if !slices.Contains(schema.PKey, idx) {
			check(cell.Type == schema.Cols[idx].Type)
			val = cell.Encode(val)
		}
	}
	return
}

func (row Row) DecodeKey(schema *Schema, key []byte) (err error) {
	check(len(row) == len(schema.Cols))

	// テーブル名+終端文字(\x00)の長さを検証
	tablePrefixLen := len(schema.Table) + 1
	if len(key) < tablePrefixLen {
		return ErrKeyTooShort
	}

	// keyに含まれるテーブル名とschemaのテーブル名が一致するか検証
	if schema.Table+"\x00" != string(key[tablePrefixLen]) {
		return ErrTableMismatch
	}

	key = key[len(schema.Table)+1:]

	for idx, column := range schema.Cols {
		if !slices.Contains(schema.PKey, idx) {
			continue

		}
		cell := NewCell(column.Type)
		if key, err = cell.Decode(key); err != nil {
			return
		}
		row[idx] = cell
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
		if val, err = cell.Decode(val); err != nil {
			return
		}
		row[idx] = cell
	}

	if len(val) != 0 {
		return ErrExtraData
	}

	return nil
}

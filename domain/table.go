package domain

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
)

type DB struct {
	KV     KV
	tables map[string]Schema
}

func (db *DB) Open() error {
	db.tables = map[string]Schema{}
	return db.KV.Open()
}

func (db *DB) Close() error { return db.KV.Close() }

func (db *DB) GetSchema(table string) (Schema, error) {
	schema, ok := db.tables[table]
	if !ok {
		val, ok, err := db.KV.Get([]byte("@schema_" + table))
		if err == nil && ok {
			err = json.Unmarshal(val, &schema)
		}
		if err != nil {
			return Schema{}, err
		}
		if !ok {
			return Schema{}, errors.New("table is not found")
		}
		db.tables[table] = schema
	}
	return schema, nil
}

func (db *DB) Select(schema *Schema, row Row) (ok bool, err error) {
	key := row.EncodeKey(schema)
	val, ok, err := db.KV.Get(key)
	if err != nil || !ok {
		return ok, err
	}
	if err := row.DecodeVal(schema, val); err != nil {
		return false, err
	}
	return true, nil
}

func (db *DB) Insert(schema *Schema, row Row) (updated bool, err error) {
	key := row.EncodeKey(schema)
	val := row.EncodeVal(schema)
	return db.KV.SetEx(key, val, ModeInsert)
}

func (db *DB) Upsert(schema *Schema, row Row) (updated bool, err error) {
	key := row.EncodeKey(schema)
	val := row.EncodeVal(schema)
	return db.KV.SetEx(key, val, ModeUpsert)
}

func (db *DB) Update(schema *Schema, row Row) (updated bool, err error) {
	key := row.EncodeKey(schema)
	val := row.EncodeVal(schema)
	return db.KV.SetEx(key, val, ModeUpdate)
}

func (db *DB) Delete(schema *Schema, row Row) (deleted bool, err error) {
	key := row.EncodeKey(schema)
	return db.KV.Del(key)
}

type SQLResult struct {
	Updated int
	Header  []string
	Values  []Row
}

func (db *DB) ExecStmt(stmt interface{}) (r SQLResult, err error) {
	switch ptr := stmt.(type) {
	case *StmtCreateTable:
		err = db.execCreateTable(ptr)
	case *StmtSelect:
		r.Header = ptr.cols
		r.Values, err = db.execSelect(ptr)
	case *StmtInsert:
		r.Updated, err = db.execInsert(ptr)
	case *StmtUpdate:
		r.Updated, err = db.execUpdate(ptr)
	case *StmtDelete:
		r.Updated, err = db.execDelete(ptr)
	default:
		panic("unreachable")
	}
	return
}

func (db *DB) execCreateTable(stmt *StmtCreateTable) (err error) {
	// 1. Convert StmtCreatTable to Schema.
	schema := Schema{
		Table: stmt.table,
		Cols:  stmt.cols,
	}
	for _, pkeyCol := range stmt.pkey {
		idx := slices.IndexFunc(stmt.cols, func(col Column) bool {
			if col.Name == pkeyCol {
				return true
			}
			return false
		})
		schema.PKey = append(schema.PKey, idx)
	}
	// 2. Store Schema under the key @schema_ + table name.
	key := "@schema_" + stmt.table
	buf, err := json.Marshal(schema)
	if err != nil {
		return err
	}
	if _, err = db.KV.SetEx([]byte(key), buf, ModeInsert); err != nil {
		return err
	}

	// 3. Add it to the DB.tables map.
	db.tables[stmt.table] = schema

	return nil
}

func (db *DB) execSelect(stmt *StmtSelect) ([]Row, error) {
	// 1. Get schema info based on table name
	schema, err := db.GetSchema(stmt.table)
	if err != nil {
		return nil, err
	}

	// 2. Get selected column indices from schema.Cols based on column name
	indices, err := lookupColumns(schema.Cols, stmt.cols)
	if err != nil {
		return nil, err
	}

	// 3. Check WHERE matches the primary key, and get row with key filled.
	row, err := makePKey(&schema, stmt.keys)
	if err != nil {
		return nil, err
	}
	ok, err := db.Select(&schema, row)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}

	// 4. Get only the columns selected by statement
	row = subsetRow(row, indices)
	return []Row{row}, nil
}

// lookupColumns
// check column names (select a, b), return indices in schema.Cols
func lookupColumns(cols []Column, names []string) ([]int, error) {
	indices := make([]int, 0)
	for _, colName := range names {
		idx := slices.IndexFunc(cols, func(col Column) bool {
			return colName == col.Name
		})
		if idx < 0 {
			return nil, errors.New("column is not found")
		}
		indices = append(indices, idx)
	}
	return indices, nil
}

// makePKey
// check that WHERE matches the primary key, return a Row with the key filled
func makePKey(schema *Schema, keys []NamedCell) (Row, error) {
	if len(schema.PKey) != len(keys) {
		return nil, fmt.Errorf("primary key mismatch: expected %d columns, got %d", len(schema.PKey), len(keys))
	}
	row := schema.NewRow()
	for _, idx1 := range schema.PKey {
		col := schema.Cols[idx1]
		idx2 := slices.IndexFunc(keys, func(expr NamedCell) bool {
			return expr.column == col.Name && expr.value.Type == col.Type
		})
		if idx2 < 0 {
			return nil, fmt.Errorf("primary key mismatch: missing or invalid column %q", col.Name)
		}
		row[idx1] = keys[idx2].value
	}
	return row, nil
}

// setValue
// check that SET matches the column name, return a Row with the value filled
func setValue(schema *Schema, values []NamedCell, row Row) (Row, error) {
	for _, col := range values {
		idx := slices.IndexFunc(schema.Cols, func(expr Column) bool {
			return expr.Name == col.column && expr.Type == col.value.Type
		})
		if idx < 0 {
			return nil, fmt.Errorf("column name mismatch: missing or invalid column %q", col.column)
		}
		row[idx] = col.value
	}
	return row, nil
}

// subsetRow
// return only the columns in select a, b
func subsetRow(row Row, indices []int) Row {
	subRow := make(Row, 0)
	for _, idx := range indices {
		subRow = append(subRow, row[idx])
	}
	return subRow
}

func (db *DB) execInsert(stmt *StmtInsert) (count int, err error) {
	schema, err := db.GetSchema(stmt.table)
	if err != nil {
		return 0, err
	}

	updated, err := db.Insert(&schema, stmt.value)
	if err != nil {
		return 0, err
	}
	if updated {
		count++
	}

	return count, nil
}

func (db *DB) execUpdate(stmt *StmtUpdate) (count int, err error) {
	// 1. Get schema info based on the table name
	schema, err := db.GetSchema(stmt.table)
	if err != nil {
		return 0, err
	}

	// 2. Check WHERE matches the primary key, and get a Row with key filled
	row, err := makePKey(&schema, stmt.keys)
	if err != nil {
		return 0, err
	}

	// 3. Check that SET matches the column name, and get a Row with value filled
	row, err = setValue(&schema, stmt.value, row)
	if err != nil {
		return 0, err
	}
	updated, err := db.Update(&schema, row)
	if err != nil {
		return 0, err
	}
	if updated {
		count++
	}

	return count, nil
}

func (db *DB) execDelete(stmt *StmtDelete) (count int, err error) {
	// 1. Get schema info based on the table name
	schema, err := db.GetSchema(stmt.table)
	if err != nil {
		return 0, err
	}

	// 2. Check WHERE matches the primary key, and get a Row with key filled
	row, err := makePKey(&schema, stmt.keys)
	if err != nil {
		return 0, err
	}
	updated, err := db.Delete(&schema, row)
	if err != nil {
		return 0, err
	}
	if updated {
		count++
	}

	return count, nil
}

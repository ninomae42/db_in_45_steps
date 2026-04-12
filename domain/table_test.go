package domain

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTableByPKey(t *testing.T) {
	db := DB{}
	db.KV.log.FileName = ".test_db"
	defer os.Remove(db.KV.log.FileName)

	os.Remove(db.KV.log.FileName)
	err := db.Open()
	assert.Nil(t, err)
	defer db.Close()

	schema := &Schema{
		Table: "link",
		Cols: []Column{
			{Name: "time", Type: TypeI64},
			{Name: "src", Type: TypeStr},
			{Name: "dst", Type: TypeStr},
		},
		PKey: []int{1, 2}, // (src, dst)
	}

	row := Row{
		Cell{Type: TypeI64, I64: 123},
		Cell{Type: TypeStr, Str: []byte("a")},
		Cell{Type: TypeStr, Str: []byte("b")},
	}
	ok, err := db.Select(schema, row)
	assert.True(t, !ok && err == nil) // 存在しないデータの検索(Read - Not Found)

	updated, err := db.Insert(schema, row)
	assert.True(t, updated && err == nil) // データの新規挿入(Create)

	out := Row{
		Cell{},
		Cell{Type: TypeStr, Str: []byte("a")},
		Cell{Type: TypeStr, Str: []byte("b")},
	}
	ok, err = db.Select(schema, out)
	assert.True(t, ok && err == nil)
	assert.Equal(t, row, out) // 挿入したデータの取得と整合性(Read - Success)

	row[0].I64 = 456
	updated, err = db.Update(schema, row)
	assert.True(t, updated && err == nil) // データの更新(Update)

	ok, err = db.Select(schema, out)
	assert.True(t, ok && err == nil)
	assert.Equal(t, row, out) // 更新内容の反映確認(Read after Update)

	deleted, err := db.Delete(schema, row)
	assert.True(t, deleted && err == nil) // データの削除(Delete)

	ok, err = db.Select(schema, row)
	assert.True(t, !ok && err == nil) // 削除後の状態確認(Read after Delete)
}

func parseStmt(t *testing.T, s string) interface{} {
	p := NewParser(s)
	stmt, err := p.parseStmt()
	require.Nil(t, err)
	return stmt
}

func TestSQLByPKey(t *testing.T) {
	db := DB{}
	db.KV.log.FileName = ".test_db"
	defer os.Remove(db.KV.log.FileName)

	os.Remove(db.KV.log.FileName)
	err := db.Open()
	assert.Nil(t, err)
	defer db.Close()

	s := "create table link (time int64, src string, dst string, primary key (src, dst));"
	_, err = db.ExecStmt(parseStmt(t, s))
	require.Nil(t, err)

	// added myself
	schema, err := db.GetSchema("link")
	require.Nil(t, err)
	assert.Equal(t, "link", schema.Table)
	assert.Equal(t, 3, len(schema.Cols))
	assert.Equal(t, 2, len(schema.PKey))

	s = "insert into link values (123, 'bob', 'alice');"
	r, err := db.ExecStmt(parseStmt(t, s))
	require.Nil(t, err)
	require.Equal(t, 1, r.Updated)

	s = "select time from link where dst = 'alice' and src = 'bob';"
	r, err = db.ExecStmt(parseStmt(t, s))
	require.Nil(t, err)
	require.Equal(t, []Row{{Cell{Type: TypeI64, I64: 123}}}, r.Values)

	s = "update link set time = 456 where dst = 'alice' and src = 'bob';"
	r, err = db.ExecStmt(parseStmt(t, s))
	require.Nil(t, err)
	require.Equal(t, 1, r.Updated)

	s = "select time from link where dst = 'alice' and src = 'bob';"
	r, err = db.ExecStmt(parseStmt(t, s))
	require.Nil(t, err)
	require.Equal(t, []Row{{Cell{Type: TypeI64, I64: 456}}}, r.Values)

	// reopen
	err = db.Close()
	require.Nil(t, err)
	db = DB{}
	db.KV.log.FileName = ".test_db"
	err = db.Open()
	require.Nil(t, err)

	s = "delete from link where src = 'bob' and dst = 'alice';"
	r, err = db.ExecStmt(parseStmt(t, s))
	require.Nil(t, err)
	require.Equal(t, 1, r.Updated)

	s = "select time from link where dst = 'alice' and src = 'bob';"
	r, err = db.ExecStmt(parseStmt(t, s))
	require.Nil(t, err)
	require.Equal(t, 0, len(r.Values))
}

func TestSQLUpdatePreservesUnsetColumns(t *testing.T) {
	db := DB{}
	db.KV.log.FileName = ".test_db_update"
	defer os.Remove(db.KV.log.FileName)

	os.Remove(db.KV.log.FileName)
	err := db.Open()
	require.Nil(t, err)
	defer db.Close()

	// 非PKカラムが name, age の2つあるテーブル
	s := "create table users (id int64, name string, age int64, primary key (id));"
	_, err = db.ExecStmt(parseStmt(t, s))
	require.Nil(t, err)

	s = "insert into users values (1, 'alice', 30);"
	r, err := db.ExecStmt(parseStmt(t, s))
	require.Nil(t, err)
	require.Equal(t, 1, r.Updated)

	// name だけ UPDATE（age は SET しない）
	s = "update users set name = 'bob' where id = 1;"
	r, err = db.ExecStmt(parseStmt(t, s))
	require.Nil(t, err)
	require.Equal(t, 1, r.Updated)

	// name が更新されていること
	s = "select name from users where id = 1;"
	r, err = db.ExecStmt(parseStmt(t, s))
	require.Nil(t, err)
	require.Equal(t, []Row{{Cell{Type: TypeStr, Str: []byte("bob")}}}, r.Values)

	// age が 0 に上書きされず、元の値 30 が保持されていること
	s = "select age from users where id = 1;"
	r, err = db.ExecStmt(parseStmt(t, s))
	require.Nil(t, err)
	require.Equal(t, []Row{{Cell{Type: TypeI64, I64: 30}}}, r.Values)
}

func TestIterByPKey(t *testing.T) {
	db := DB{}
	db.KV.log.FileName = ".test_db"
	defer os.Remove(db.KV.log.FileName)

	os.Remove(db.KV.log.FileName)
	err := db.Open()
	assert.Nil(t, err)
	defer db.Close()

	schema := &Schema{
		Table: "t",
		Cols: []Column{
			{Name: "k", Type: TypeI64},
			{Name: "v", Type: TypeI64},
		},
		PKey: []int{0},
	}

	N := int64(10)
	for i := int64(0); i < N; i += 2 {
		row := Row{
			Cell{Type: TypeI64, I64: i},
			Cell{Type: TypeI64, I64: i},
		}
		updated, err := db.Insert(schema, row)
		require.True(t, updated && err == nil)
	}

	for i := int64(-1); i < N+1; i++ {
		row := Row{
			Cell{Type: TypeI64, I64: i},
			Cell{},
		}

		out := []int64{}
		iter, err := db.Seek(schema, row)
		for ; err == nil && iter.Valid(); err = iter.Next() {
			out = append(out, iter.Row()[1].I64)
		}
		require.Nil(t, err)

		expected := []int64{}
		for j := i; j < N; j++ {
			if j >= 0 && j%2 == 0 {
				expected = append(expected, j)
			}
		}
		assert.Equal(t, expected, out)
	}
}

// QzBQWVJJOUhU https://trialofcode.org/

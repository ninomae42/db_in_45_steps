package domain

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
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

// QzBQWVJJOUhU https://trialofcode.org/

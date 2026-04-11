package domain

import (
	"bytes"
	"slices"
)

type KV struct {
	log  Log
	keys [][]byte
	vals [][]byte
}

func (kv *KV) Open() error {
	if err := kv.log.Open(); err != nil {
		return err
	}
	entries := []Entry{}
	for {
		ent := Entry{}
		eof, err := kv.log.Read(&ent)
		if eof {
			break
		} else if err != nil {
			return err
		}
		entries = append(entries, ent)
	}
	slices.SortStableFunc(entries, func(a, b Entry) int {
		return bytes.Compare(a.key, b.key)
	})
	kv.keys, kv.vals = kv.keys[:0], kv.vals[:0]
	for _, ent := range entries {
		n := len(kv.keys)
		if 0 < n && bytes.Equal(kv.keys[n-1], ent.key) {
			kv.keys, kv.vals = kv.keys[:n-1], kv.vals[:n-1]
		}
		if !ent.deleted {
			kv.keys = append(kv.keys, ent.key)
			kv.vals = append(kv.vals, ent.val)
		}
	}

	return nil
}

func (kv *KV) Close() error {
	return kv.log.Close()
}

func (kv *KV) Get(key []byte) (val []byte, ok bool, err error) {
	if idx, ok := slices.BinarySearchFunc(kv.keys, key, bytes.Compare); ok {
		return kv.vals[idx], true, nil
	}
	return nil, false, nil
}

type UpdateMode int

const (
	ModeUpsert UpdateMode = 0 // insert or update
	ModeInsert UpdateMode = 1 // insert new
	ModeUpdate UpdateMode = 2 // update existing
)

func (kv *KV) SetEx(key []byte, val []byte, mode UpdateMode) (updated bool, err error) {
	idx, exist := slices.BinarySearchFunc(kv.keys, key, bytes.Compare)
	switch mode {
	case ModeUpsert: // same as the old Set(), insert or overwrite
		updated = !exist || !bytes.Equal(kv.vals[idx], val)
	case ModeInsert: // if the key already exists, do not update and return false
		updated = !exist
	case ModeUpdate: // only update existing keys
		updated = exist && !bytes.Equal(kv.vals[idx], val)
	default:
		panic("unreachable")
	}
	if updated {
		if err = kv.log.Write(NewPutEntry(key, val)); err != nil {
			return false, err
		}
		if exist {
			kv.vals[idx] = val
		} else {
			kv.keys = slices.Insert(kv.keys, idx, key)
			kv.vals = slices.Insert(kv.vals, idx, val)
		}
	}
	return
}

func (kv *KV) Set(key []byte, val []byte) (updated bool, err error) {
	return kv.SetEx(key, val, ModeUpsert)
}

func (kv *KV) Del(key []byte) (bool, error) {
	if idx, ok := slices.BinarySearchFunc(kv.keys, key, bytes.Compare); ok {
		if err := kv.log.Write(NewDelEntry(key)); err != nil {
			return false, err
		}
		kv.keys = slices.Delete(kv.keys, idx, idx+1)
		kv.vals = slices.Delete(kv.vals, idx, idx+1)
		return true, nil
	}

	return false, nil
}

type KVIterator struct {
	keys [][]byte
	vals [][]byte
	pos  int // current position
}

func (kv *KV) Seek(key []byte) (*KVIterator, error) {
	pos, _ := slices.BinarySearchFunc(kv.keys, key, bytes.Compare)
	return &KVIterator{keys: kv.keys, vals: kv.vals, pos: pos}, nil
}

// Valid
// is travarsal ended or not
func (iter *KVIterator) Valid() bool {
	return 0 <= iter.pos && iter.pos < len(iter.keys)
}

// Key
// get current element key
func (iter *KVIterator) Key() []byte {
	return iter.keys[iter.pos]
}

// Val
// get current element value
func (iter *KVIterator) Val() []byte {
	return iter.vals[iter.pos]
}

// Next
// move to next element
func (iter *KVIterator) Next() error {
	if iter.pos < len(iter.keys) {
		iter.pos++
	}
	return nil
}

// Prev
// move to previous element
func (iter *KVIterator) Prev() error {
	if 0 <= iter.pos {
		iter.pos--
	}
	return nil
}

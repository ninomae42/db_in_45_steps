package domain

import (
	"bytes"
)

type KV struct {
	log Log
	mem map[string][]byte
}

func (kv *KV) Open() error {
	if err := kv.log.Open(); err != nil {
		return err
	}
	kv.mem = map[string][]byte{}

	for {
		ent := Entry{}
		eof, err := kv.log.Read(&ent)
		if eof {
			break
		} else if err != nil {
			return err
		}
		if ent.deleted {
			delete(kv.mem, string(ent.key))
		} else {
			kv.mem[string(ent.key)] = ent.val
		}
	}

	return nil
}

func (kv *KV) Close() error {
	return kv.log.Close()
}

func (kv *KV) Get(key []byte) (val []byte, ok bool, err error) {
	if val, ok = kv.mem[string(key)]; ok {
		return val, ok, nil
	}
	return nil, false, nil
}
func (kv *KV) Set(key []byte, val []byte) (updated bool, err error) {
	prev, exist := kv.mem[string(key)]
	updated = !exist || !bytes.Equal(prev, val)
	if updated {
		ent := NewPutEntry(key, val)
		if err = kv.log.Write(ent); err != nil {
			return false, err
		}
		kv.mem[string(key)] = val
	}
	return
}

func (kv *KV) Del(key []byte) (deleted bool, err error) {
	_, deleted = kv.mem[string(key)]
	if deleted {
		ent := NewDelEntry(key)
		if err := kv.log.Write(ent); err != nil {
			return false, err
		}
		delete(kv.mem, string(key))
	}
	return
}

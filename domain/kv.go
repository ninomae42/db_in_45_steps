package domain

type KV struct {
	mem map[string][]byte
}

func (kv *KV) Open() error {
	kv.mem = map[string][]byte{}
	return nil
}

func (kv *KV) Close() error {
	return nil
}

func (kv *KV) Get(key []byte) (val []byte, ok bool, err error) {
	if val, ok = kv.mem[string(key)]; ok {
		return val, ok, nil
	}
	return nil, false, nil
}
func (kv *KV) Set(key []byte, val []byte) (updated bool, err error) {
	if _, ok := kv.mem[string(key)]; ok {
		return false, nil
	}
	kv.mem[string(key)] = val
	return true, nil
}
func (kv *KV) Del(key []byte) (deleted bool, err error) {
	if _, ok := kv.mem[string(key)]; !ok {
		return false, nil
	}
	delete(kv.mem, string(key))
	return true, nil
}

package s3cache

import (
	"os"
)

// entry tracks one cached slice file.
type entry struct {
	key  string
	path string
	size int64
}

// get returns an open file for a cached slice (hit path is TOCTOU-safe:
// the fd pins the bytes even if eviction unlinks the path).
func (p *Proxy) get(key string) (*os.File, int64, bool) {
	p.mu.Lock()
	el, ok := p.index[key]
	if !ok {
		p.mu.Unlock()
		return nil, 0, false
	}
	p.lru.MoveToFront(el)
	ent := el.Value.(*entry)
	f, err := os.Open(ent.path)
	p.mu.Unlock()
	if err != nil {
		return nil, 0, false
	}
	return f, ent.size, true
}

// insert admits a slice and evicts LRU tails past MaxBytes. Unlinks
// happen after unlock: see the lock-discipline note on Proxy.mu.
func (p *Proxy) insert(key, path string, size int64) {
	p.mu.Lock()
	if el, ok := p.index[key]; ok {
		p.lru.MoveToFront(el)
		p.mu.Unlock()
		return
	}
	p.index[key] = p.lru.PushFront(&entry{key: key, path: path, size: size})
	p.used += size
	var evicted []string
	for p.used > p.cfg.MaxBytes {
		back := p.lru.Back()
		if back == nil {
			break
		}
		ent := back.Value.(*entry)
		delete(p.index, ent.key)
		p.lru.Remove(back)
		p.used -= ent.size
		p.evictions++
		evicted = append(evicted, ent.path)
	}
	p.mu.Unlock()
	for _, path := range evicted {
		_ = os.Remove(path)
	}
}

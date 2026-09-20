package s3cache

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// rememberTotal caches object length learned from Content-Range. Bounded:
// a full-lake scan through the proxy must not grow it without limit.
// Persisted best-effort so a restarted proxy keeps serving from disk
// without a single origin trip.
func (p *Proxy) rememberTotal(obj string, length int64) {
	p.totalsMu.Lock()
	if p.totals[obj] == length {
		p.totalsMu.Unlock()
		return
	}
	if len(p.totals) >= 1<<16 {
		p.totals = map[string]int64{obj: length}
	} else {
		p.totals[obj] = length
	}
	p.totalsMu.Unlock()
	if f, err := os.OpenFile(p.objectsLogPath(), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644); err == nil {
		_, _ = f.WriteString(obj + " " + strconv.FormatInt(length, 10) + "\n")
		f.Close()
	}
}

func (p *Proxy) objectsLogPath() string { return filepath.Join(p.cfg.CacheDir, "objects.log") }

func (p *Proxy) cachedTotal(obj string) (int64, bool) {
	p.totalsMu.Lock()
	defer p.totalsMu.Unlock()
	n, ok := p.totals[obj]
	return n, ok
}

func isDigits(s string) bool {
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	return len(s) > 0
}

// recover re-admits persisted slices and object lengths after a proxy
// restart. Slice files are content-keyed but the key is an opaque hash,
// so object lengths ride in objects.log; without them serve() cannot
// validate ranges without an origin HEAD/GET.
func (p *Proxy) recover() {
	_ = filepath.Walk(p.cfg.CacheDir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		// Layout: CacheDir/<key[:2]>/<key[2:]>/<index>.slice — slice keys
		// contain a slash (obj/index), which filepath.Join nests.
		idx, ok := strings.CutSuffix(info.Name(), ".slice")
		if !ok || idx == "" {
			return nil // tmp files, logs
		}
		parent := filepath.Base(filepath.Dir(path))
		grand := filepath.Base(filepath.Dir(filepath.Dir(path)))
		if len(grand) != 2 || !isDigits(idx) {
			return nil
		}
		p.insert(grand+parent+"/"+idx, path, info.Size())
		return nil
	})
	data, err := os.ReadFile(p.objectsLogPath())
	if err != nil {
		return
	}
	lines := strings.Split(strings.TrimRight(string(data), "\n"), "\n")
	if len(lines) > 1<<16 { // keep the most recent entries only
		lines = lines[len(lines)-1<<16:]
	}
	for _, line := range lines {
		fields := strings.Fields(line)
		if len(fields) != 2 {
			continue
		}
		n, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil || n < 0 {
			continue
		}
		p.totals[fields[0]] = n
	}
}

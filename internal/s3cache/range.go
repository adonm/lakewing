package s3cache

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// Control-flow errors. Callers branch with errors.Is; never on text.
var (
	// errMultipartRange makes multipart Range requests fall back to a
	// full-body 200 (legal per RFC 9110: servers may ignore Range).
	errMultipartRange = errors.New("multipart ranges unsupported")
	// errUnsatisfiable maps to 416 with a Content-Range */length header.
	errUnsatisfiable = errors.New("range not satisfiable")
)

// parseRange handles bytes=start-end, bytes=start- and bytes=-suffix
// against a known object length.
func parseRange(header string, length int64) (int64, int64, error) {
	if header == "" {
		if length < 0 {
			return 0, -1, fmt.Errorf("unknown length")
		}
		return 0, length - 1, nil
	}
	rest, ok := strings.CutPrefix(header, "bytes=")
	if !ok {
		return 0, 0, fmt.Errorf("unsupported range unit")
	}
	if strings.Contains(rest, ",") {
		return 0, 0, errMultipartRange
	}
	lo, hi, _ := strings.Cut(rest, "-")
	if lo == "" {
		suf, err := strconv.ParseInt(hi, 10, 64)
		if err != nil || suf <= 0 || length < 0 {
			return 0, 0, fmt.Errorf("bad suffix range")
		}
		if suf > length {
			suf = length
		}
		return length - suf, length - 1, nil
	}
	start, err := strconv.ParseInt(lo, 10, 64)
	if err != nil || start < 0 {
		return 0, 0, fmt.Errorf("bad range start")
	}
	if hi == "" {
		if length < 0 {
			return start, -1, nil
		}
		if start >= length {
			return 0, 0, errUnsatisfiable
		}
		return start, length - 1, nil
	}
	end, err := strconv.ParseInt(hi, 10, 64)
	if err != nil || end < start {
		return 0, 0, fmt.Errorf("bad range end")
	}
	if length >= 0 {
		if start >= length {
			return 0, 0, errUnsatisfiable
		}
		if end >= length {
			end = length - 1
		}
	}
	return start, end, nil
}

// splitRange structurally parses a Range header without validating
// against a length: returns lo, hi (hi=-1 when open-ended/absent),
// multipart flag, and syntax errors.
func splitRange(header string) (int64, int64, bool, error) {
	if header == "" {
		return 0, -1, false, nil
	}
	rest, ok := strings.CutPrefix(header, "bytes=")
	if !ok {
		return 0, 0, false, fmt.Errorf("unsupported range unit")
	}
	if strings.Contains(rest, ",") {
		return 0, 0, true, nil
	}
	lo, hi, _ := strings.Cut(rest, "-")
	if lo == "" {
		return 0, -1, false, nil // suffix; needs length
	}
	start, err := strconv.ParseInt(lo, 10, 64)
	if err != nil || start < 0 {
		return 0, 0, false, fmt.Errorf("bad range start")
	}
	if hi == "" {
		return start, -1, false, nil
	}
	end, err := strconv.ParseInt(hi, 10, 64)
	if err != nil || end < start {
		return 0, 0, false, fmt.Errorf("bad range end")
	}
	return start, end, false, nil
}

// parseContentRange parses "bytes lo-hi/total" (total may be "*").
func parseContentRange(cr string) (int64, int64, int64, error) {
	rest, ok := strings.CutPrefix(cr, "bytes ")
	if !ok {
		return 0, 0, -1, fmt.Errorf("bad content-range")
	}
	bounds, total, _ := strings.Cut(rest, "/")
	lo, hi, _ := strings.Cut(bounds, "-")
	start, err1 := strconv.ParseInt(lo, 10, 64)
	end, err2 := strconv.ParseInt(hi, 10, 64)
	if err1 != nil || err2 != nil {
		return 0, 0, -1, fmt.Errorf("bad content-range bounds")
	}
	if total == "*" {
		return start, end, -1, nil
	}
	n, err := strconv.ParseInt(total, 10, 64)
	if err != nil {
		return 0, 0, -1, fmt.Errorf("bad content-range total")
	}
	return start, end, n, nil
}

func singleJoin(base, p string) string {
	return strings.TrimSuffix(base, "/") + "/" + strings.TrimPrefix(p, "/")
}

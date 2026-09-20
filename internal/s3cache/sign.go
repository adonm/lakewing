package s3cache

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"
)

// emptyPayloadHash is the SigV4 hash of an empty body: all proxy upstream
// requests are GET/HEAD with no payload.
const emptyPayloadHash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

// signer produces SigV4 headers for upstream requests. The proxy is the
// only signer: client credentials never cross it, so nothing upstream
// depends on how workers authenticate to the proxy — and workers do zero
// HMAC work per request.
type signer struct {
	keyID  string
	secret string
	region string

	mu      sync.Mutex
	date    string
	dateKey []byte // HMAC("AWS4"+secret, date); region/service/request keys derived per use
}

func newSigner(keyID, secret, region string) *signer {
	if region == "" {
		region = "us-east-1"
	}
	return &signer{keyID: keyID, secret: secret, region: region}
}

func hmacSHA256(key, data []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(data)
	return h.Sum(nil)
}

func sha256Hex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

// key derives the SigV4 signing key, caching the date-stage per UTC day.
func (s *signer) key(date string) []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.date != date {
		s.dateKey = hmacSHA256([]byte("AWS4"+s.secret), []byte(date))
		s.date = date
	}
	k := hmacSHA256(s.dateKey, []byte(s.region))
	k = hmacSHA256(k, []byte("s3"))
	return hmacSHA256(k, []byte("aws4_request"))
}

// awsURIEncode encodes per SigV4: unreserved characters kept, everything
// else percent-encoded uppercase hex.
func awsURIEncode(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') ||
			c == '-' || c == '_' || c == '.' || c == '~' {
			b.WriteByte(c)
		} else {
			fmt.Fprintf(&b, "%%%02X", c)
		}
	}
	return b.String()
}

// canonicalURI encodes each path segment, preserving slashes.
func canonicalURI(path string) string {
	segs := strings.Split(path, "/")
	for i, s := range segs {
		segs[i] = awsURIEncode(s)
	}
	return strings.Join(segs, "/")
}

// canonicalQuery sorts and re-encodes query parameters; validators
// canonicalize the received query the same way, so signing the canonical
// form is always consistent with what is sent.
func canonicalQuery(raw string) string {
	if raw == "" {
		return ""
	}
	q, err := url.ParseQuery(raw)
	if err != nil {
		return ""
	}
	keys := make([]string, 0, len(q))
	for k := range q {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		vals := append([]string(nil), q[k]...)
		sort.Strings(vals)
		for _, v := range vals {
			if b.Len() > 0 {
				b.WriteByte('&')
			}
			b.WriteString(awsURIEncode(k))
			b.WriteByte('=')
			b.WriteString(awsURIEncode(v))
		}
	}
	return b.String()
}

// sign adds X-Amz-Date, X-Amz-Content-Sha256 and Authorization to req.
// Only host and the two x-amz headers are signed: minimal, valid SigV4.
func (s *signer) sign(req *http.Request) {
	if s.keyID == "" || s.secret == "" {
		return
	}
	amzDate := time.Now().UTC().Format("20060102T150405Z")
	date := amzDate[:8]
	req.Header.Set("X-Amz-Date", amzDate)
	req.Header.Set("X-Amz-Content-Sha256", emptyPayloadHash)
	host := req.Host
	if host == "" {
		host = req.URL.Host
	}
	canonicalRequest := strings.Join([]string{
		req.Method,
		canonicalURI(req.URL.Path),
		canonicalQuery(req.URL.RawQuery),
		"host:" + host + "\n" + "x-amz-content-sha256:" + emptyPayloadHash + "\n" + "x-amz-date:" + amzDate + "\n",
		"host;x-amz-content-sha256;x-amz-date",
		emptyPayloadHash,
	}, "\n")
	scope := date + "/" + s.region + "/s3/aws4_request"
	stringToSign := strings.Join([]string{"AWS4-HMAC-SHA256", amzDate, scope, sha256Hex(canonicalRequest)}, "\n")
	signature := hex.EncodeToString(hmacSHA256(s.key(date), []byte(stringToSign)))
	req.Header.Set("Authorization", fmt.Sprintf(
		"AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=%s",
		s.keyID, scope, signature))
}

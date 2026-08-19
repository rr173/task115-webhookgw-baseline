package delivery

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"strings"
)

// Sign returns the HMAC-SHA256 signature of body under secret, prefixed for the
// X-Webhook-Signature header.
func Sign(secret string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

// httpStatusFromReason extracts an HTTP status code from a failure reason
// produced by deliver, returning false if the reason is not an HTTP status.
func httpStatusFromReason(reason string) (int, bool) {
	const prefix = "endpoint returned HTTP "
	if !strings.HasPrefix(reason, prefix) {
		return 0, false
	}
	code, err := strconv.Atoi(strings.TrimSpace(reason[len(prefix):]))
	if err != nil {
		return 0, false
	}
	return code, true
}

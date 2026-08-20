package model

import "strings"

// CanonicalEventType applies the gateway's single, case-insensitive rule for
// event types: trim surrounding whitespace and fold to lower case. Partners
// sometimes send the same event with different casing (for example
// "Order.Created" and "ORDER.CREATED"); without a canonical form, a subscription
// stored for one casing would not reliably match an event arriving in another.
//
// The same rule is applied when subscription events are stored, when ingested
// events are persisted and when an event type is matched against a
// subscription, so saving and matching always agree.
func CanonicalEventType(t string) string {
	return strings.ToLower(strings.TrimSpace(t))
}

package clock

import (
	"testing"
	"time"
)

func TestFakeClockAdvance(t *testing.T) {
	c := NewFakeClock(time.Unix(100, 0))
	if !c.Now().Equal(time.Unix(100, 0)) {
		t.Fatalf("initial now wrong: %v", c.Now())
	}
	c.Advance(time.Second)
	if !c.Now().Equal(time.Unix(101, 0)) {
		t.Fatalf("after advance: %v", c.Now())
	}
	c.Set(time.Unix(200, 0))
	if !c.Now().Equal(time.Unix(200, 0)) {
		t.Fatalf("after set: %v", c.Now())
	}
}

func TestRealClock(t *testing.T) {
	var c Clock = RealClock{}
	if c.Now().IsZero() {
		t.Fatal("real clock returned zero time")
	}
}

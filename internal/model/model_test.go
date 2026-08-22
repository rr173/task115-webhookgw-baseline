package model

import "testing"

func TestPageBoundsDefaults(t *testing.T) {
	var f AttemptFilter
	off, lim := f.PageBounds()
	if off != 0 || lim != 20 {
		t.Fatalf("defaults: offset=%d limit=%d", off, lim)
	}
}

func TestPageBoundsPaged(t *testing.T) {
	f := AttemptFilter{Page: 3, PageSize: 10}
	off, lim := f.PageBounds()
	if off != 20 || lim != 10 {
		t.Fatalf("paged: offset=%d limit=%d", off, lim)
	}
}

func TestPageBoundsInvalidClamped(t *testing.T) {
	f := AttemptFilter{Page: 0, PageSize: -5}
	off, lim := f.PageBounds()
	if off != 0 || lim != 20 {
		t.Fatalf("clamped: offset=%d limit=%d", off, lim)
	}
}

// TestPageBoundsFirstPageStartsAtZero guards the regression where page 1
// computed a non-zero offset and the first page began mid-result-set.
func TestPageBoundsFirstPageStartsAtZero(t *testing.T) {
	cases := []AttemptFilter{
		{Page: 1, PageSize: 10},
		{Page: 1, PageSize: 50},
		{Page: 1, PageSize: 1},
	}
	for _, f := range cases {
		off, lim := f.PageBounds()
		if off != 0 || lim != f.PageSize {
			t.Fatalf("page1: offset=%d limit=%d (want 0,%d)", off, lim, f.PageSize)
		}
	}
	// Successive pages must be contiguous (no gap, no overlap).
	for page := 1; page <= 4; page++ {
		f := AttemptFilter{Page: page, PageSize: 10}
		off, _ := f.PageBounds()
		if off != (page-1)*10 {
			t.Fatalf("page %d: offset=%d want %d", page, off, (page-1)*10)
		}
	}
}

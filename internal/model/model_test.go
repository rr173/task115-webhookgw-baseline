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

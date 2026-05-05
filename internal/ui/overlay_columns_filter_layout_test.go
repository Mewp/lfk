package ui

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// The column toggle overlay's filter bar must follow the namespace
// overlay's layout: anchored right under the title (not at the
// bottom), and always present (placeholder when inactive). Without
// this, the filter row appears/disappears between renders ("disappears
// randomly") and adds an unaccounted row that overflows the overlay
// height ("resizes the window").

// firstLineContaining returns the index of the first line that contains
// substr, or -1 if none. Strips ANSI before matching.
func firstLineContaining(out, substr string) int {
	lines := strings.Split(stripANSI(out), "\n")
	for i, line := range lines {
		if strings.Contains(line, substr) {
			return i
		}
	}
	return -1
}

func TestColumnToggleOverlay_FilterBarAnchoredBelowTitle(t *testing.T) {
	entries := []ColumnToggleEntry{
		{Key: "Namespace", Visible: true},
		{Key: "Ready", Visible: true},
		{Key: "Status", Visible: true},
	}
	out := RenderColumnToggleOverlay(entries, 0, "", false, 50, 20)

	filterLine := firstLineContaining(out, "filter")
	itemLine := firstLineContaining(out, "Namespace")
	if !assert.GreaterOrEqual(t, filterLine, 0, "filter bar must render somewhere") {
		return
	}
	if !assert.GreaterOrEqual(t, itemLine, 0, "items must render somewhere") {
		return
	}
	// Filter bar must come BEFORE the items (anchored under title), not
	// after them ("appears at the bottom" was the bug).
	assert.Less(t, filterLine, itemLine,
		"filter bar must precede the items list, not sit at the bottom")
}

func TestColumnToggleOverlay_FilterBarAlwaysPresent(t *testing.T) {
	entries := []ColumnToggleEntry{{Key: "IP", Visible: true}}

	// No filter, not active — placeholder should still render so the
	// row count is stable across renders.
	out := RenderColumnToggleOverlay(entries, 0, "", false, 50, 20)
	assert.GreaterOrEqual(t, firstLineContaining(out, "filter"), 0,
		"filter row must show a placeholder when inactive so it never disappears")
}

func TestColumnToggleOverlay_FilterActiveShowsCursor(t *testing.T) {
	entries := []ColumnToggleEntry{{Key: "IP", Visible: true}}
	out := RenderColumnToggleOverlay(entries, 0, "ip", true, 50, 20)
	plain := stripANSI(out)
	assert.Contains(t, plain, "ip",
		"active filter must render the typed text")
}

// As the user types and filter narrows, the renderer must NOT emit
// more lines than lipgloss can absorb inside the declared box. Bug
// repro: with 30 entries and height=20, the renderer used to emit 20
// content lines, but lipgloss `OverlayStyle.Height(20)` only fits 18
// rows of content (the remaining 2 are vertical padding from
// Padding(1,2)). Lipgloss grew the box on overflow; as filter narrowed,
// the content fit again and the box shrank back to its nominal size.
//
// The renderer must keep its content within the lipgloss-padded budget
// (height - 2 for vertical padding), so the final rendered box has the
// same row count for every filter state.
func TestColumnToggleOverlay_LineCountFitsPaddedBudget(t *testing.T) {
	manyEntries := make([]ColumnToggleEntry, 30)
	for i := range manyEntries {
		manyEntries[i] = ColumnToggleEntry{Key: "col" + string(rune('A'+i%26))}
	}
	const height = 20
	out := RenderColumnToggleOverlay(manyEntries, 0, "", false, 50, height)
	lines := strings.Count(out, "\n") + 1
	assert.LessOrEqual(t, lines, height-2,
		"content must fit the lipgloss-padded inner area (height - 2 for OverlayStyle.Padding(1,2)); otherwise the box grows on overflow and visibly shrinks back when the filter narrows the list")
}

// Pin the user-visible symptom: the OUTER box (after FillLinesBg +
// OverlayStyle.Height) must have the same row count across every
// filter state, so the box doesn't visibly shrink as the user types.
func TestColumnToggleOverlay_FullBoxHeightStableAcrossFilterStates(t *testing.T) {
	entries := make([]ColumnToggleEntry, 30)
	for i := range entries {
		entries[i] = ColumnToggleEntry{Key: "col" + string(rune('A'+i%26))}
	}
	const height = 20
	const width = 50

	render := func(slice []ColumnToggleEntry, filter string, fActive bool) int {
		content := RenderColumnToggleOverlay(slice, 0, filter, fActive, width, height)
		filled := FillLinesBg(content, width-4, SurfaceBg)
		full := OverlayStyle.Width(width).Height(height).Render(filled)
		return strings.Count(full, "\n") + 1
	}

	baseline := render(entries, "", false)
	for _, n := range []int{30, 15, 14, 13, 12, 11, 5, 1} {
		got := render(entries[:n], "col", true)
		assert.Equal(t, baseline, got,
			"full overlay box must be the same height for entries=%d (baseline=%d, got=%d)",
			n, baseline, got)
	}
}

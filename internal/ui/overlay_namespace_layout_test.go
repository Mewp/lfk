package ui

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/janosmiko/lfk/internal/model"
)

// As the user types and the filter narrows the namespace list, the
// renderer must NOT emit more lines than lipgloss can pad inside the
// declared box. The user-visible bug pattern: with 30 namespaces and a
// height=20 box the renderer used to emit 20–21 content lines, which
// is more than lipgloss can absorb in `OverlayStyle.Height(20)` (the
// content area inside Padding(1,2) is only 18 rows). Lipgloss grew the
// box on overflow; as the filter narrowed the list, the content fit
// inside 18 again and the box "shrank" back — the user noticed this
// transition right as the "↓ N below" indicator turned into its
// placeholder row.
//
// The renderer must keep its content within the lipgloss-padded budget
// (height - 2 for vertical padding), so the final rendered box has the
// same row count for every filter state.
func TestRenderNamespaceOverlay_LineCountFitsPaddedBudget(t *testing.T) {
	manyItems := make([]model.Item, 30)
	for i := range manyItems {
		manyItems[i] = model.Item{Name: fmt.Sprintf("ns-%d", i)}
	}
	const height = 20
	out := RenderNamespaceOverlay(manyItems, "", 0, "default", false, nil, false, height)
	lines := strings.Count(out, "\n") + 1
	assert.LessOrEqual(t, lines, height-2,
		"content must fit the lipgloss-padded inner area (height - 2 for OverlayStyle.Padding(1,2)); otherwise the box grows on overflow and visibly shrinks back when the filter narrows the list")
}

// The visible symptom the user reports is the OUTER box changing size
// across filter states. This pins it: render the full overlay (the
// same path renderOverlay walks — FillLinesBg + OverlayStyle.Height) at
// every interesting list size and verify the row count is identical.
// Without this, the renderer can pass `lines <= height` while still
// allowing lipgloss to expand the box on a near-overflow input.
func TestRenderNamespaceOverlay_FullBoxHeightStableAcrossFilterStates(t *testing.T) {
	items := make([]model.Item, 30)
	for i := range items {
		items[i] = model.Item{Name: fmt.Sprintf("ns-%d", i)}
	}
	const height = 20
	const width = 60

	render := func(slice []model.Item, filter string, fmode bool) int {
		content := RenderNamespaceOverlay(slice, filter, 0, "default", false, nil, fmode, height)
		filled := FillLinesBg(content, width-4, SurfaceBg)
		full := OverlayStyle.Width(width).Height(height).Render(filled)
		return strings.Count(full, "\n") + 1
	}

	baseline := render(items, "", false)
	// Walk the filter through the boundaries that historically moved
	// the box: full list, just over the visible cap, exactly the cap,
	// just under the cap (where "↓ N below" turns into placeholder),
	// and well below.
	cases := []struct {
		label string
		slice []model.Item
	}{
		{"30", items},
		{"15", items[:15]},
		{"14", items[:14]},
		{"13", items[:13]},
		{"12", items[:12]},
		{"5", items[:5]},
		{"3", items[:3]},
		{"1", items[:1]},
	}
	for _, c := range cases {
		got := render(c.slice, "ns", true)
		assert.Equal(t, baseline, got,
			"full overlay box height must be identical for items=%s (baseline=%d, got=%d) — otherwise the user sees the box shrink as they type",
			c.label, baseline, got)
	}
}

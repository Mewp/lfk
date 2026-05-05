# CrashLoopBackOff Investigator — Design

- Date: 2026-05-04
- Branch: `feat/crashloop-investigator`
- Source: CLAUDE-TODO.md L931 — *"Crashloopbackoff investigator -- one panel combining events + restart history + last logs + describe for the failing container"*

## Goal

A per-Pod diagnostic overlay that combines four pieces of information needed during a CrashLoopBackOff investigation in one keystroke from the Pod action menu:

1. Restart history (per-container, including init containers).
2. Events scoped to the pod.
3. Last logs (`kubectl logs --previous`) of the failing container, with a toggle to current logs.
4. Describe output, trimmed to the active container.

The investigator is also useful for healthy or `ImagePullBackOff` pods — the action is always available, the panel reports what it finds.

## Non-goals

- Live watch / auto-refresh streams (manual refresh only — see *Refresh* below).
- Cross-pod aggregation. The investigator is per-pod; for fleet-wide views the existing dashboard alerts already handle that surface.
- Auto-suggesting fixes. The investigator surfaces facts; remediation is the operator's job.

## User-visible behavior

### Trigger

- New entry in the Pod action menu: `Crash Investigator` with key `I`. Always available on Pods, including healthy pods (the panel still answers "no recent crashes" usefully).
- No top-level hotkey at this time. Action menu only.

### Overlay layout — tabbed

```text
┌────────────────────────────────────────────────────────────────┐
│ Crash Investigator — namespace/pod-xyz · container: app · #7   │
├────────────────────────────────────────────────────────────────┤
│ [Summary] [Events] [Logs] [Describe]                           │
├────────────────────────────────────────────────────────────────┤
│                                                                │
│   …active tab body…                                            │
│                                                                │
├────────────────────────────────────────────────────────────────┤
│  Tab/Shift+Tab switch · 1-4 jump · c container · p prev/curr  │
│  Shift+R refresh · Esc close                                   │
└────────────────────────────────────────────────────────────────┘
```

- `Tab` / `Shift+Tab`: cycle tabs forward / backward.
- `1` / `2` / `3` / `4`: jump directly to Summary / Events / Logs / Describe.
- `c`: cycle active container (init containers included). Updates the header marker and re-renders Logs / Describe / Events for the new container; Summary stays aggregated but highlights the active row.
- `p` (Logs tab only): toggle between previous logs (`kubectl logs --previous`) and current logs. Default is previous. Header line reflects the active mode.
- `j` / `k`, `g` / `G`: scroll within the active tab body. Per-tab scroll offset is preserved when switching tabs.
- `/`: search within the active tab body (Logs / Describe), reusing the existing log/yaml search primitives.
- `Shift+R`: refresh — re-fetch all four sections, preserving active container, active tab, logs-previous toggle, and scroll offsets.
- `Esc` / `q`: close overlay; clear `crashInvestigation` state.

### Tab contents

**Summary** (aggregated, always-on landing tab):
- Pod-level header: `Phase`, `Pod IP`, `Node`, `QoS class`, `Age`.
- Container table — one row per container with columns:
  - `CONTAINER | STATE | RESTARTS | LAST EXIT | LAST REASON | LAST FINISHED`
- Init containers rendered in a separate sub-table above app containers, same column layout.
- The row matching the currently-active container is visually highlighted (theme bg + bold).
- "Last terminated" details for the active container expanded under the table:
  - signal, exit code, started-at, finished-at, message field if non-empty.

**Events**:
- List of events with `involvedObject.name=<pod>,involvedObject.kind=Pod` plus events on the Pod's owner ReplicaSet/Job that fall within the pod's lifetime window.
- Reuses the existing `event timeline` row format (Type / Reason / Age / Source / Message). Warning events colored, normal events dim.
- `j/k` scrolls; `/` searches.

**Logs**:
- Tail of the active container's logs, default `--previous` (last terminated instance's stdout/stderr).
- `p` toggles between previous and current logs; the header line shows `LOGS · previous · 200 lines · container=app` or `LOGS · current · 200 lines · container=app`.
- 200-line tail (matches existing log-preview pattern). Scrolling and search match the log viewer.
- Empty-state when previous logs are unavailable: `no previous container output — this container has not been terminated yet. press p to view current logs.`

**Describe**:
- The Pod's `kubectl describe` output trimmed to the active container's section, with the Pod-level header (Name/Namespace/Node/Status/Age) preserved at the top.
- If trimming fails (regex doesn't match the expected describe layout), fall back to the full pod describe with a note at the top of the panel.

### Multi-container handling

- On open, active container defaults to the first container (init or app, init takes precedence) with `RestartCount > 0` or in `Waiting:CrashLoopBackOff`.
- If no container is unhealthy, falls back to the first app container (then first init container if there are no app containers, which is rare).
- `c` cycles through all containers (init + app) in declaration order. Active container is shown in the overlay header.

## Architecture

### Data layer — `internal/k8s/crash_investigator.go`

```go
type CrashInvestigation struct {
    Pod            PodSummary       // name, ns, phase, IP, node, qos, age, owner kind/name
    InitContainers []ContainerCrash // declaration order
    AppContainers  []ContainerCrash // declaration order
    Events         []corev1.Event   // already filtered + sorted desc by lastSeen
    Describe       string           // full describe text, trimmed via DescribeContainerSection
    DescribeError  string           // populated if describe fetch failed
}

type PodSummary struct {
    Name      string
    Namespace string
    Phase     string
    PodIP     string
    Node      string
    QoSClass  string
    Age       time.Duration
    OwnerKind string
    OwnerName string
}

type ContainerCrash struct {
    Name             string
    IsInit           bool
    Image            string
    State            string  // "Running", "Waiting", "Terminated"
    StateReason      string  // for Waiting/Terminated, e.g. "CrashLoopBackOff"
    Ready            bool
    RestartCount     int32
    Started          *time.Time

    LastTermination  *ContainerTermination // nil if never terminated

    PreviousLog string
    CurrentLog  string
    LogError    string // populated per-stream on partial fetch failure
}

type ContainerTermination struct {
    Reason     string
    ExitCode   int32
    Signal     int32
    StartedAt  time.Time
    FinishedAt time.Time
    Message    string
}

func (c *Client) GetCrashInvestigation(ctx context.Context, contextName, namespace, podName string) (*CrashInvestigation, error)
```

`GetCrashInvestigation` flow:
1. `clientset.CoreV1().Pods(ns).Get(ctx, name, …)` — single Get for spec + status.
2. `clientset.CoreV1().Events(ns).List(ctx, …)` with `FieldSelector: involvedObject.name=<pod>,involvedObject.kind=Pod`. Owner-RS/Job events fetched only if Pod's owner is RS/Job (separate List).
3. For each container (init + app), in parallel via `errgroup`:
   - `Logs(ctx, &corev1.PodLogOptions{Container: name, Previous: true, TailLines: 200}).DoRaw(ctx)` → `PreviousLog` or `LogError`.
   - `Logs(..., Previous: false, TailLines: 200).DoRaw(ctx)` → `CurrentLog` or `LogError` (errors are joined).
4. Describe via existing `kubectl describe pod` helper; trim to container section.
5. Assemble and return; per-container log errors do *not* fail the whole call. Pod Get failure does fail the whole call.

Limits / safety:
- Hard cap `TailLines: 200` per stream.
- `ctx` flowed through every API call; any caller-side cancellation aborts pending fetches.
- Errgroup with bounded goroutines: container streams run concurrently but bounded by `2 * len(containers)` (previous + current per container). For typical pods this is ≤8 goroutines.

### App layer

- Action menu entry — `internal/model/actions.go`, `actionsForCoreKind` case `"Pod"` gains `{Label: "Crash Investigator", Description: "Investigate crash loop / failing pod", Key: "I"}`.
- Action dispatch — `internal/app/update_actions.go` adds `case "Crash Investigator":` calling `executeActionCrashInvestigator`.
- Action executor — new `internal/app/update_actions_crash_investigator.go` with:
  ```go
  func (m Model) executeActionCrashInvestigator() (tea.Model, tea.Cmd) {
      m.loading = true
      m.setStatusMessage("Investigating crashes…", false)
      return m, m.loadCrashInvestigation()
  }
  ```
- Load command — `internal/app/commands_load_preview.go` adds `loadCrashInvestigation()` mirroring `loadPodStartup` (bg-task wrapped).
- Message — `internal/app/messages.go`:
  ```go
  type crashInvestigationMsg struct {
      info *k8s.CrashInvestigation
      err  error
  }
  ```
- Update handler — new `internal/app/update_crash_investigator.go`:
  ```go
  func (m Model) updateCrashInvestigation(msg crashInvestigationMsg) (tea.Model, tea.Cmd)
  ```
  responsibilities: clear loading, set state, open overlay, default tab + active container, default `logsShowPrevious=true`. On error, status message and no overlay.
- Overlay key handlers — `internal/app/update_overlays_crash_investigator.go` handles `Tab/Shift+Tab/1-4/c/p/Shift+R/Esc/q/j/k/g/G//`. Hint bar configured in `overlay_hintbar.go`.
- Renderer — `internal/app/view_overlays.go` adds `case overlayCrashInvestigator:` calling `renderOverlayCrashInvestigator()`.
- Pure-UI renderer — new `internal/ui/overlay_crash_investigator.go` exposing `RenderCrashInvestigatorOverlay(entry CrashInvestigatorEntry) string`. Takes a presentation-only struct so the UI package never imports `k8s`.

### State on `Model`

To stay under app.go's 800-line cap, all CrashInvestigator fields live in a single sub-struct (mirroring `whoCanState` from the recent RBAC PR):

```go
type crashInvState struct {
    data            *k8s.CrashInvestigation
    activeContainer string
    activeTab       crashInvTab
    showPreviousLogs bool
    scroll          map[crashInvScrollKey]int // key = (tab, container)
    searchActive    bool
    searchInput     TextInput
}

type crashInvTab int
const (
    crashInvTabSummary crashInvTab = iota
    crashInvTabEvents
    crashInvTabLogs
    crashInvTabDescribe
)

type crashInvScrollKey struct {
    tab       crashInvTab
    container string
}
```

`Model.crashInv crashInvState` (single field). All handlers read/write `m.crashInv.*`.

### Refresh semantics (`Shift+R`)

1. Snapshot current `activeContainer`, `activeTab`, `showPreviousLogs`, `scroll` map.
2. Re-dispatch `loadCrashInvestigation()`.
3. On `crashInvestigationMsg` success — overwrite `data`, but reapply the snapshot. If `activeContainer` no longer exists in the new data, fall back to first failing or first container (same logic as initial open).
4. On error — overlay stays open with old data, status message shows the error.

This avoids the "scroll position resets on refresh" anti-pattern.

## Error handling & edge cases

- **Pod has no previous instance (`LastTerminationState.Terminated == nil` for the active container)**: Logs tab shows `no previous container output — this container has not been terminated yet. press p to view current logs.`. `p` still works.
- **Pod is healthy** (no restarts, action triggered anyway): all tabs render. Summary shows current state and zeros for restart counts; Logs is mostly "no previous logs" until something terminates.
- **Per-container fetch fails**: per-container `LogError` is set; that container's Logs panel renders `failed to load logs: <err>`. Other containers and other tabs unaffected.
- **Pod deleted while overlay open**: `Shift+R` returns 404; close overlay, status `Pod no longer exists`.
- **Active container removed after refresh** (rare; spec mutated): clamp to first container in new list.
- **Init container in CLB**: shown in InitContainers sub-table; `c` cycles through all containers (init + app); Logs tab fetches with the init container name like any other container (the K8s API supports this for init containers in `Waiting`/`Terminated`).
- **Describe fails entirely**: Describe tab shows the error; other tabs still render. The whole investigation does *not* fail if only describe fails.
- **No events for the pod**: Events tab shows `No events for this pod.`.
- **Previous logs API returns "previous terminated container ... not found"**: not an error — `PreviousLog` empty, no `LogError`. Render the empty-state message.

## Testing strategy

### Data layer — `internal/k8s/crash_investigator_test.go` (table-driven, fakeclient-based)

1. Single-container CLB pod: 1 container with `RestartCount > 0`, `LastTermination` populated, `PreviousLog` populated.
2. Multi-container pod, only one in CLB: all containers returned; only the failing one has `LastTermination`.
3. Init container in CLB: separated into `InitContainers` slice; `AppContainers` still populated.
4. Healthy pod (no restarts): empty `LastTermination`, `PreviousLog` empty, no `LogError`.
5. Pod with no previous logs (fakeclient `Logs` call for `--previous` returns the standard "previous terminated container ... not found" sentinel): `LogError` empty, `PreviousLog` empty (treated as expected emptiness, not an error).
6. Events filter: returns only events with `involvedObject.name=<pod>`; events for other pods in the same namespace are ignored.
7. Pod 404: returns wrapped error.
8. Describe fetch fails: `DescribeError` populated, other fields populated.

### App layer — `internal/app/update_crash_investigator_test.go`

1. Action dispatch: selecting "Crash Investigator" on a Pod sets loading and dispatches `loadCrashInvestigation`.
2. Message handler success: sets `crashInv.data`, opens overlay, picks first failing container as active, defaults to `crashInvTabSummary`, `showPreviousLogs=true`.
3. Message handler error: shows status, no overlay open.
4. `Tab`/`Shift+Tab` cycle through tabs in order (forward + reverse with wraparound).
5. `1/2/3/4` jump to Summary/Events/Logs/Describe respectively.
6. `c` cycles active container (init included); preserves tab and `showPreviousLogs`.
7. `p` toggles `showPreviousLogs` only on Logs tab (no-op on other tabs).
8. `Shift+R` re-fetches: on success, preserves active container, tab, `showPreviousLogs`, scroll offsets.
9. `Esc`/`q` closes overlay and clears `crashInv.data`.
10. Active container falls back to first container when previously-active is gone after refresh.
11. Action is read-only (no mutating call); covered by adding "Crash Investigator" to the read-only allowlist test (`internal/app/readonly_test.go`).

### Renderer — `internal/ui/overlay_crash_investigator_test.go`

1. Tab bar renders 4 tabs with active tab highlighted.
2. Summary tab renders aggregated container table; init containers in a separate sub-table.
3. Events tab renders `No events for this pod.` when `len(Events) == 0`.
4. Logs tab shows `no previous container output…` empty-state when `PreviousLog == ""` and `showPreviousLogs == true`.
5. Logs tab header reflects `previous`/`current` state and active container name.
6. Multi-container Summary visually marks the active container row.
7. Theme-bg regression: forces `termenv.TrueColor`, applies `DefaultTheme`, asserts ≥4 bg-setting SGRs (`48;5;` / `48;2;`) — same shape as `TestRenderSecretEditorOverlay_InnerPanelMatchesOuterBg`.

### Manual verification — append to `TESTS.md`

```text
1. kind create cluster
2. kubectl apply -f - <<EOF
   apiVersion: v1
   kind: Pod
   metadata: { name: crashy }
   spec:
     containers:
     - name: app
       image: busybox
       command: ["sh", "-c", "exit 1"]
   EOF
3. Wait until pod is in CrashLoopBackOff
4. lfk → navigate to the pod → x → I
5. Verify Summary shows RestartCount > 0 and LastReason "Error" (or "CrashLoopBackOff" if Waiting)
6. Tab to Logs — should show stdout/stderr of the failed `exit 1`
7. p — toggles to current logs (probably empty)
8. Shift+R — re-fetches, scroll position and active tab preserved
9. c — cycles container (single-container pod: no-op; verify with multi-container pod)
10. Esc — overlay closes
```

## Out of scope (deferred)

- Auto-refresh / live tick. Manual `Shift+R` only.
- Cross-namespace investigation (always scoped to one pod).
- Diff between consecutive previous logs (only the most recent previous instance is fetched).
- Aggregating events from multiple owners (only owner RS/Job, not transitively).

## Documentation updates

- `docs/keybindings.md`: add `Shift+R refresh`, `c container`, `p prev/curr`, `Tab/Shift+Tab`, `1-4 jump` rows under a new "Crash Investigator overlay" subsection.
- `docs/views-and-overlays.md`: new row under Overlays / Diagnostics: `overlayCrashInvestigator | Pod action menu → Crash Investigator (I) | One-panel CrashLoopBackOff investigation: events, restart history, last logs, describe.`
- `internal/ui/help.go`: add the overlay's keybindings to the help screen so `?` discoverability stays in sync.
- `README.md`: one-line bullet under the existing feature list.
- `CLAUDE-TODO.md` line 931: mark `[>]` with PR number once implemented.

## Implementation order (rough)

1. `internal/k8s/crash_investigator.go` + tests (red → green).
2. `internal/ui/overlay_crash_investigator.go` + tests (renderer takes a presentation struct, can be tested without app).
3. `internal/app/` glue (state, action, command, message, update handler, overlay key handler, view dispatch) + tests.
4. Action menu entry, hint bar, read-only allowlist update.
5. Documentation updates.
6. Manual verification per `TESTS.md` entry.

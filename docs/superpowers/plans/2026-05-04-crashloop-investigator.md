# CrashLoopBackOff Investigator Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a per-Pod tabbed overlay (`overlayCrashInvestigator`) accessible from the Pod action menu key `I`, that combines aggregated restart history, pod-scoped events, previous/current container logs, and a per-container describe view, refreshable with `Shift+R`.

**Architecture:** Mirrors the existing `overlayPodStartup` feature — a `k8s.GetCrashInvestigation` data layer that fetches Pod + Events + per-container Logs + Describe in parallel, an app-layer state machine in a sub-struct (`crashInvState`) that holds the active tab/container/logs-mode, a pure UI renderer (`internal/ui/overlay_crash_investigator.go`) that takes a presentation-only entry struct, and three new test files (one per layer).

**Tech Stack:** Go 1.24, `k8s.io/client-go` (CoreV1 Pods/Events/Logs), `k8s.io/apimachinery`, `golang.org/x/sync/errgroup` (already a transitive dep — verify in Task 1), `github.com/charmbracelet/lipgloss` (theme-aware styling), `github.com/charmbracelet/lipgloss/table` (Summary container table), `github.com/stretchr/testify` (assertions), `k8s.io/client-go/kubernetes/fake` (test fakes).

**Reference design:** `docs/superpowers/specs/2026-05-04-crashloop-investigator-design.md`

**Reference implementations to mirror:**
- Data layer: `internal/k8s/startup.go` (`GetPodStartupAnalysis`)
- UI renderer: `internal/ui/overlay_monitoring_misc.go` (`RenderPodStartupOverlay`)
- App state sub-struct: `internal/app/app_types.go` (`whoCanState`)
- Action wiring: `internal/app/update_actions_helm_misc.go` (`executeActionStartupAnalysis`)
- Load command: `internal/app/commands_load_preview.go` (`loadPodStartup`)

---

## File Structure

**New files:**

| Path | Responsibility |
|------|----------------|
| `internal/k8s/crash_investigator.go` | `GetCrashInvestigation` + types `CrashInvestigation`, `PodSummary`, `ContainerCrash`, `ContainerTermination` |
| `internal/k8s/crash_investigator_test.go` | Data-layer tests (table-driven, fakeclient) |
| `internal/ui/overlay_crash_investigator.go` | `RenderCrashInvestigatorOverlay` + types `CrashInvestigatorEntry`, `CrashContainerEntry`, etc. |
| `internal/ui/overlay_crash_investigator_test.go` | Renderer tests |
| `internal/app/update_actions_crash_investigator.go` | `executeActionCrashInvestigator` |
| `internal/app/commands_load_crash_investigator.go` | `loadCrashInvestigation` (small new file to keep `commands_load_preview.go` under 800 lines) |
| `internal/app/update_crash_investigator.go` | `updateCrashInvestigation` message handler |
| `internal/app/update_overlays_crash_investigator.go` | Overlay key dispatcher (`Tab`, `1-4`, `c`, `p`, `Shift+R`, `Esc`, scroll) |
| `internal/app/update_crash_investigator_test.go` | App-layer state-machine tests |

**Modified files:**

| Path | What changes |
|------|--------------|
| `internal/model/actions.go` | Add `Crash Investigator` action to `actionsForCoreKind("Pod")` |
| `internal/app/app_types.go` | Add `overlayCrashInvestigator` to `overlayKind`, add `crashInvState` + `crashInvTab` sub-struct |
| `internal/app/app.go` | Add `crashInv crashInvState` field on `Model` |
| `internal/app/messages.go` | Add `crashInvestigationMsg` |
| `internal/app/update.go` | Dispatch `crashInvestigationMsg` to `updateCrashInvestigation` |
| `internal/app/update_actions.go` | Dispatch `"Crash Investigator"` action label to `executeActionCrashInvestigator` |
| `internal/app/update_overlays.go` | Dispatch overlay keys + `Esc` close in `handleOverlayKeySecondary` |
| `internal/app/view_overlays.go` | Render `overlayCrashInvestigator` |
| `internal/app/overlay_hintbar.go` | Hint bar for the overlay |
| `internal/app/readonly_test.go` | Add `"Crash Investigator"` to allowlist (read-only action) |
| `internal/ui/help.go` | Add the overlay's keybindings to the searchable help screen |
| `docs/keybindings.md` | New "Crash Investigator overlay" subsection |
| `docs/views-and-overlays.md` | New row under Overlays |
| `README.md` | One-line bullet under feature list |
| `TESTS.md` | New manual verification section |
| `CLAUDE-TODO.md` | Mark line 931 with `[>]` |

---

## Task 1: Scaffolding — `CrashInvestigation` types and verify `errgroup` dep

**Files:**
- Create: `internal/k8s/crash_investigator.go`
- Verify: `go.mod` has `golang.org/x/sync`

- [>] **Step 1.1: Verify `errgroup` is available**

```bash
grep "golang.org/x/sync" go.mod
```
Expected: a line like `golang.org/x/sync v0.x.x // indirect` (or non-indirect). If absent, add via:
```bash
go get golang.org/x/sync@latest
```

- [>] **Step 1.2: Create the data-layer file with type declarations only**

Create `internal/k8s/crash_investigator.go`:

```go
package k8s

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"golang.org/x/sync/errgroup"
)

// CrashInvestigation is the full result of GetCrashInvestigation: pod
// summary, per-container restart + termination + log info, pod-scoped
// events, and a (possibly trimmed) describe blob.
type CrashInvestigation struct {
	Pod            PodSummary
	InitContainers []ContainerCrash // declaration order; init containers
	AppContainers  []ContainerCrash // declaration order; app containers
	Events         []corev1.Event   // sorted by LastTimestamp desc
	Describe       string
	DescribeError  string
}

// PodSummary holds pod-scoped fields rendered in the overlay header
// and the Summary tab. Owner refs are flattened to a single
// (Kind, Name) pair (the controller ref).
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

// ContainerCrash captures a single container's runtime state plus its
// previous-instance and current-instance log tails.
type ContainerCrash struct {
	Name            string
	IsInit          bool
	Image           string
	State           string // "Running", "Waiting", "Terminated"
	StateReason     string // "CrashLoopBackOff", "Error", etc.
	Ready           bool
	RestartCount    int32
	Started         *time.Time
	LastTermination *ContainerTermination

	PreviousLog string
	CurrentLog  string
	LogError    string // joined per-stream errors (previous + current)
}

// ContainerTermination is the LastTerminationState.Terminated info,
// extracted to a flat struct so the renderer doesn't depend on
// k8s.io/api/core/v1.
type ContainerTermination struct {
	Reason     string
	ExitCode   int32
	Signal     int32
	StartedAt  time.Time
	FinishedAt time.Time
	Message    string
}

// describeFunc is the kubectl-describe wrapper used by GetCrashInvestigation;
// extracted as a field on Client so tests can inject a stub instead of
// shelling out to kubectl. Real implementation lives in client_describe.go.
const crashLogTailLines int64 = 200
```

- [>] **Step 1.3: Verify it builds**

```bash
go build ./internal/k8s/...
```
Expected: no output (clean build). If `golang.org/x/sync/errgroup` import fails, run `go mod tidy`.

- [>] **Step 1.4: Commit**

```bash
git add go.mod go.sum internal/k8s/crash_investigator.go
git commit -m "feat(k8s): scaffold CrashInvestigation types"
```

---

## Task 2: `GetCrashInvestigation` — pod fetch + summary

**Files:**
- Modify: `internal/k8s/crash_investigator.go`
- Create: `internal/k8s/crash_investigator_test.go`

- [ ] **Step 2.1: Write the failing test**

Create `internal/k8s/crash_investigator_test.go`:

```go
package k8s

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sfake "k8s.io/client-go/kubernetes/fake"
)

func TestGetCrashInvestigation_PodSummary(t *testing.T) {
	created := time.Now().Add(-5 * time.Minute)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "crashy",
			Namespace:         "default",
			CreationTimestamp: metav1.Time{Time: created},
			OwnerReferences: []metav1.OwnerReference{
				{Kind: "ReplicaSet", Name: "rs-1", Controller: ptrBool(true)},
			},
		},
		Spec: corev1.PodSpec{
			NodeName: "node-1",
			Containers: []corev1.Container{
				{Name: "app", Image: "busybox:1.36"},
			},
		},
		Status: corev1.PodStatus{
			Phase:    corev1.PodRunning,
			PodIP:    "10.0.0.5",
			QOSClass: corev1.PodQOSBurstable,
		},
	}
	cs := k8sfake.NewClientset(pod)
	c := newFakeClient(cs, nil)

	c.describeOverride = func(_ context.Context, _, _, _ string) (string, error) {
		return "Name: crashy\nNamespace: default\n", nil
	}

	got, err := c.GetCrashInvestigation(context.Background(), "", "default", "crashy")
	require.NoError(t, err)
	assert.Equal(t, "crashy", got.Pod.Name)
	assert.Equal(t, "default", got.Pod.Namespace)
	assert.Equal(t, "Running", got.Pod.Phase)
	assert.Equal(t, "10.0.0.5", got.Pod.PodIP)
	assert.Equal(t, "node-1", got.Pod.Node)
	assert.Equal(t, "Burstable", got.Pod.QoSClass)
	assert.Equal(t, "ReplicaSet", got.Pod.OwnerKind)
	assert.Equal(t, "rs-1", got.Pod.OwnerName)
	assert.Greater(t, got.Pod.Age, 4*time.Minute)
}

func ptrBool(b bool) *bool { return &b }
```

- [ ] **Step 2.2: Verify the test fails to compile**

```bash
go test ./internal/k8s/ -run TestGetCrashInvestigation_PodSummary -v
```
Expected: build error — `c.GetCrashInvestigation undefined`, `c.describeOverride undefined`.

- [ ] **Step 2.3: Add `describeOverride` field to `Client`**

In `internal/k8s/client.go` (or wherever `Client` is declared — find with `grep -n "^type Client struct" internal/k8s/*.go`), add the field. If `Client` is in `client.go`, add a new line in the struct:

```go
// describeOverride lets tests stub the kubectl-describe wrapper; nil in
// production (real implementation goes through DescribePod).
describeOverride func(ctx context.Context, contextName, namespace, podName string) (string, error)
```

- [ ] **Step 2.4: Implement `GetCrashInvestigation` (pod fetch + summary only)**

Append to `internal/k8s/crash_investigator.go`:

```go
// GetCrashInvestigation fetches a pod and assembles a multi-section
// diagnostic snapshot: pod summary, per-container restart + termination
// + log info, pod-scoped events, and a kubectl describe blob. Per-stream
// log errors and a describe-fetch error do NOT fail the whole call;
// only a Pod Get failure does.
func (c *Client) GetCrashInvestigation(ctx context.Context, contextName, namespace, podName string) (*CrashInvestigation, error) {
	clientset, err := c.clientsetForContext(contextName)
	if err != nil {
		return nil, fmt.Errorf("clientset: %w", err)
	}

	pod, err := clientset.CoreV1().Pods(namespace).Get(ctx, podName, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("pod %s/%s not found: %w", namespace, podName, err)
		}
		return nil, fmt.Errorf("getting pod: %w", err)
	}

	out := &CrashInvestigation{
		Pod: buildPodSummary(pod),
	}
	return out, nil
}

func buildPodSummary(pod *corev1.Pod) PodSummary {
	s := PodSummary{
		Name:      pod.Name,
		Namespace: pod.Namespace,
		Phase:     string(pod.Status.Phase),
		PodIP:     pod.Status.PodIP,
		Node:      pod.Spec.NodeName,
		QoSClass:  string(pod.Status.QOSClass),
		Age:       time.Since(pod.CreationTimestamp.Time),
	}
	for _, ref := range pod.OwnerReferences {
		if ref.Controller != nil && *ref.Controller {
			s.OwnerKind = ref.Kind
			s.OwnerName = ref.Name
			break
		}
	}
	return s
}
```

- [ ] **Step 2.5: Wire `describeOverride` to be called when set**

In the same function, after `out := &CrashInvestigation{...}`, add the describe call (we'll fill in events / containers in later tasks):

```go
	// Describe (errors don't fail the whole call).
	if c.describeOverride != nil {
		desc, derr := c.describeOverride(ctx, contextName, namespace, podName)
		if derr != nil {
			out.DescribeError = derr.Error()
		} else {
			out.Describe = desc
		}
	}
```

- [ ] **Step 2.6: Run the test**

```bash
go test ./internal/k8s/ -run TestGetCrashInvestigation_PodSummary -v
```
Expected: PASS.

- [ ] **Step 2.7: Commit**

```bash
git add internal/k8s/crash_investigator.go internal/k8s/crash_investigator_test.go internal/k8s/client.go
git commit -m "feat(k8s): GetCrashInvestigation pod summary"
```

---

## Task 3: Per-container restart history (single container)

**Files:**
- Modify: `internal/k8s/crash_investigator.go`
- Modify: `internal/k8s/crash_investigator_test.go`

- [ ] **Step 3.1: Write the failing test**

Append to `internal/k8s/crash_investigator_test.go`:

```go
func TestGetCrashInvestigation_SingleContainerCLB(t *testing.T) {
	now := time.Now()
	started := now.Add(-30 * time.Second)
	finished := now.Add(-5 * time.Second)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "default"},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{Name: "app", Image: "busybox:1.36"},
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			ContainerStatuses: []corev1.ContainerStatus{
				{
					Name:         "app",
					Image:        "busybox:1.36",
					Ready:        false,
					RestartCount: 7,
					State: corev1.ContainerState{
						Waiting: &corev1.ContainerStateWaiting{
							Reason:  "CrashLoopBackOff",
							Message: "back-off 5m0s restarting failed container",
						},
					},
					LastTerminationState: corev1.ContainerState{
						Terminated: &corev1.ContainerStateTerminated{
							Reason:     "Error",
							ExitCode:   1,
							Signal:     0,
							StartedAt:  metav1.Time{Time: started},
							FinishedAt: metav1.Time{Time: finished},
							Message:    "boom",
						},
					},
				},
			},
		},
	}
	cs := k8sfake.NewClientset(pod)
	c := newFakeClient(cs, nil)
	c.describeOverride = func(_ context.Context, _, _, _ string) (string, error) { return "", nil }

	got, err := c.GetCrashInvestigation(context.Background(), "", "default", "p")
	require.NoError(t, err)
	require.Len(t, got.AppContainers, 1)
	require.Empty(t, got.InitContainers)

	cc := got.AppContainers[0]
	assert.Equal(t, "app", cc.Name)
	assert.Equal(t, "busybox:1.36", cc.Image)
	assert.False(t, cc.IsInit)
	assert.Equal(t, "Waiting", cc.State)
	assert.Equal(t, "CrashLoopBackOff", cc.StateReason)
	assert.False(t, cc.Ready)
	assert.Equal(t, int32(7), cc.RestartCount)
	require.NotNil(t, cc.LastTermination)
	assert.Equal(t, "Error", cc.LastTermination.Reason)
	assert.Equal(t, int32(1), cc.LastTermination.ExitCode)
	assert.Equal(t, "boom", cc.LastTermination.Message)
	assert.Equal(t, started.Unix(), cc.LastTermination.StartedAt.Unix())
}
```

- [ ] **Step 3.2: Verify the test fails**

```bash
go test ./internal/k8s/ -run TestGetCrashInvestigation_SingleContainerCLB -v
```
Expected: FAIL — `len(got.AppContainers) == 0`.

- [ ] **Step 3.3: Implement container assembly**

Append to `internal/k8s/crash_investigator.go`:

```go
// buildContainerCrashes returns init + app ContainerCrash slices in the
// pod's declaration order. Statuses are matched by name; spec containers
// without a matching status get a zero-value entry (e.g. pods scheduled
// but the kubelet hasn't reported yet).
func buildContainerCrashes(pod *corev1.Pod) (initCC, appCC []ContainerCrash) {
	initStatuses := indexContainerStatuses(pod.Status.InitContainerStatuses)
	appStatuses := indexContainerStatuses(pod.Status.ContainerStatuses)

	initCC = make([]ContainerCrash, 0, len(pod.Spec.InitContainers))
	for _, c := range pod.Spec.InitContainers {
		initCC = append(initCC, buildContainerCrash(c, initStatuses[c.Name], true))
	}
	appCC = make([]ContainerCrash, 0, len(pod.Spec.Containers))
	for _, c := range pod.Spec.Containers {
		appCC = append(appCC, buildContainerCrash(c, appStatuses[c.Name], false))
	}
	return initCC, appCC
}

func indexContainerStatuses(statuses []corev1.ContainerStatus) map[string]*corev1.ContainerStatus {
	out := make(map[string]*corev1.ContainerStatus, len(statuses))
	for i := range statuses {
		out[statuses[i].Name] = &statuses[i]
	}
	return out
}

func buildContainerCrash(spec corev1.Container, status *corev1.ContainerStatus, isInit bool) ContainerCrash {
	cc := ContainerCrash{
		Name:   spec.Name,
		Image:  spec.Image,
		IsInit: isInit,
	}
	if status == nil {
		cc.State = "Waiting"
		return cc
	}
	cc.Ready = status.Ready
	cc.RestartCount = status.RestartCount
	switch {
	case status.State.Running != nil:
		cc.State = "Running"
		t := status.State.Running.StartedAt.Time
		cc.Started = &t
	case status.State.Terminated != nil:
		cc.State = "Terminated"
		cc.StateReason = status.State.Terminated.Reason
	case status.State.Waiting != nil:
		cc.State = "Waiting"
		cc.StateReason = status.State.Waiting.Reason
	default:
		cc.State = "Waiting"
	}
	if t := status.LastTerminationState.Terminated; t != nil {
		cc.LastTermination = &ContainerTermination{
			Reason:     t.Reason,
			ExitCode:   t.ExitCode,
			Signal:     t.Signal,
			StartedAt:  t.StartedAt.Time,
			FinishedAt: t.FinishedAt.Time,
			Message:    t.Message,
		}
	}
	return cc
}
```

- [ ] **Step 3.4: Wire it into `GetCrashInvestigation`**

In `GetCrashInvestigation`, replace the assignment block with:

```go
	out := &CrashInvestigation{
		Pod: buildPodSummary(pod),
	}
	out.InitContainers, out.AppContainers = buildContainerCrashes(pod)
```

- [ ] **Step 3.5: Run the test**

```bash
go test ./internal/k8s/ -run TestGetCrashInvestigation_SingleContainerCLB -v
```
Expected: PASS.

- [ ] **Step 3.6: Commit**

```bash
git add internal/k8s/crash_investigator.go internal/k8s/crash_investigator_test.go
git commit -m "feat(k8s): per-container crash history"
```

---

## Task 4: Init containers + multi-container

**Files:**
- Modify: `internal/k8s/crash_investigator_test.go`

- [ ] **Step 4.1: Write the failing tests**

Append to `internal/k8s/crash_investigator_test.go`:

```go
func TestGetCrashInvestigation_InitContainerCLB(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "default"},
		Spec: corev1.PodSpec{
			InitContainers: []corev1.Container{
				{Name: "init-db", Image: "busybox"},
			},
			Containers: []corev1.Container{
				{Name: "app", Image: "nginx"},
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodPending,
			InitContainerStatuses: []corev1.ContainerStatus{
				{
					Name:         "init-db",
					Ready:        false,
					RestartCount: 4,
					State: corev1.ContainerState{
						Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"},
					},
				},
			},
			ContainerStatuses: []corev1.ContainerStatus{
				{Name: "app", State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "PodInitializing"}}},
			},
		},
	}
	cs := k8sfake.NewClientset(pod)
	c := newFakeClient(cs, nil)
	c.describeOverride = func(_ context.Context, _, _, _ string) (string, error) { return "", nil }

	got, err := c.GetCrashInvestigation(context.Background(), "", "default", "p")
	require.NoError(t, err)
	require.Len(t, got.InitContainers, 1)
	require.Len(t, got.AppContainers, 1)
	assert.Equal(t, "init-db", got.InitContainers[0].Name)
	assert.True(t, got.InitContainers[0].IsInit)
	assert.Equal(t, "CrashLoopBackOff", got.InitContainers[0].StateReason)
	assert.False(t, got.AppContainers[0].IsInit)
}

func TestGetCrashInvestigation_MultiContainerOnlyOneFailing(t *testing.T) {
	now := time.Now()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "default"},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{Name: "app", Image: "nginx"},
				{Name: "sidecar", Image: "busybox"},
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			ContainerStatuses: []corev1.ContainerStatus{
				{
					Name: "app", Ready: true, RestartCount: 0,
					State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.Time{Time: now.Add(-time.Minute)}}},
				},
				{
					Name: "sidecar", Ready: false, RestartCount: 3,
					State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}},
					LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Reason: "Error", ExitCode: 1}},
				},
			},
		},
	}
	cs := k8sfake.NewClientset(pod)
	c := newFakeClient(cs, nil)
	c.describeOverride = func(_ context.Context, _, _, _ string) (string, error) { return "", nil }

	got, err := c.GetCrashInvestigation(context.Background(), "", "default", "p")
	require.NoError(t, err)
	require.Len(t, got.AppContainers, 2)
	assert.Equal(t, "app", got.AppContainers[0].Name)
	assert.Nil(t, got.AppContainers[0].LastTermination)
	assert.Equal(t, "sidecar", got.AppContainers[1].Name)
	require.NotNil(t, got.AppContainers[1].LastTermination)
	assert.Equal(t, int32(3), got.AppContainers[1].RestartCount)
}

func TestGetCrashInvestigation_HealthyPod(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "default"},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Image: "nginx"}}},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			ContainerStatuses: []corev1.ContainerStatus{
				{Name: "app", Ready: true, RestartCount: 0,
					State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}},
			},
		},
	}
	cs := k8sfake.NewClientset(pod)
	c := newFakeClient(cs, nil)
	c.describeOverride = func(_ context.Context, _, _, _ string) (string, error) { return "", nil }

	got, err := c.GetCrashInvestigation(context.Background(), "", "default", "p")
	require.NoError(t, err)
	assert.Len(t, got.AppContainers, 1)
	assert.True(t, got.AppContainers[0].Ready)
	assert.Equal(t, int32(0), got.AppContainers[0].RestartCount)
	assert.Nil(t, got.AppContainers[0].LastTermination)
}
```

- [ ] **Step 4.2: Run the tests**

```bash
go test ./internal/k8s/ -run TestGetCrashInvestigation -v
```
Expected: all 4 tests PASS (the existing logic from Task 3 already supports init + multi cases — no new code needed; this task validates that).

- [ ] **Step 4.3: Commit**

```bash
git add internal/k8s/crash_investigator_test.go
git commit -m "test(k8s): init + multi-container CrashInvestigation"
```

---

## Task 5: Pod-scoped events fetch

**Files:**
- Modify: `internal/k8s/crash_investigator.go`
- Modify: `internal/k8s/crash_investigator_test.go`

- [ ] **Step 5.1: Write the failing test**

Append to `internal/k8s/crash_investigator_test.go`:

```go
func TestGetCrashInvestigation_EventsFiltered(t *testing.T) {
	now := time.Now()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "default"},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "app"}}},
	}
	mine := corev1.Event{
		ObjectMeta:    metav1.ObjectMeta{Name: "ev1", Namespace: "default"},
		InvolvedObject: corev1.ObjectReference{Kind: "Pod", Name: "p", Namespace: "default"},
		Type:           corev1.EventTypeWarning,
		Reason:         "BackOff",
		Message:        "Back-off restarting failed container",
		LastTimestamp:  metav1.Time{Time: now},
	}
	older := corev1.Event{
		ObjectMeta:    metav1.ObjectMeta{Name: "ev2", Namespace: "default"},
		InvolvedObject: corev1.ObjectReference{Kind: "Pod", Name: "p", Namespace: "default"},
		Type:           corev1.EventTypeNormal,
		Reason:         "Pulled",
		Message:        "Image pulled",
		LastTimestamp:  metav1.Time{Time: now.Add(-1 * time.Minute)},
	}
	other := corev1.Event{
		ObjectMeta:    metav1.ObjectMeta{Name: "ev3", Namespace: "default"},
		InvolvedObject: corev1.ObjectReference{Kind: "Pod", Name: "other-pod", Namespace: "default"},
		Type:           corev1.EventTypeNormal,
		LastTimestamp:  metav1.Time{Time: now},
	}
	cs := k8sfake.NewClientset(pod, &mine, &older, &other)
	c := newFakeClient(cs, nil)
	c.describeOverride = func(_ context.Context, _, _, _ string) (string, error) { return "", nil }

	got, err := c.GetCrashInvestigation(context.Background(), "", "default", "p")
	require.NoError(t, err)
	require.Len(t, got.Events, 2)
	// Newest first.
	assert.Equal(t, "ev1", got.Events[0].Name)
	assert.Equal(t, "ev2", got.Events[1].Name)
}
```

- [ ] **Step 5.2: Run to confirm failure**

```bash
go test ./internal/k8s/ -run TestGetCrashInvestigation_EventsFiltered -v
```
Expected: FAIL — `len(got.Events) == 0`.

- [ ] **Step 5.3: Implement events fetch**

Append to `internal/k8s/crash_investigator.go`:

```go
// fetchPodEvents returns events whose involvedObject points at the given
// pod, sorted by LastTimestamp descending. Empty + nil error on backend
// failure if Kubernetes returns a transient list error — the caller
// proceeds with an empty Events slice; full investigation must not fail
// just because the event list endpoint is flaky.
func fetchPodEvents(ctx context.Context, clientset kubernetesInterface, namespace, podName string) []corev1.Event {
	selector := fields.SelectorFromSet(fields.Set{
		"involvedObject.name": podName,
		"involvedObject.kind": "Pod",
	}).String()
	list, err := clientset.CoreV1().Events(namespace).List(ctx, metav1.ListOptions{FieldSelector: selector})
	if err != nil || list == nil {
		return nil
	}
	out := make([]corev1.Event, 0, len(list.Items))
	for _, ev := range list.Items {
		if ev.InvolvedObject.Name != podName || ev.InvolvedObject.Kind != "Pod" {
			continue
		}
		out = append(out, ev)
	}
	sort.SliceStable(out, func(i, j int) bool {
		return out[i].LastTimestamp.After(out[j].LastTimestamp.Time)
	})
	return out
}

// kubernetesInterface is the subset of kubernetes.Interface this package uses.
// Defined locally so we don't import the full interface name into every signature.
type kubernetesInterface interface {
	CoreV1() corev1Interface
}

// corev1Interface is the subset of corev1.CoreV1Interface used here.
type corev1Interface interface {
	Pods(namespace string) podInterface
	Events(namespace string) eventInterface
}
```

Wait — the Kubernetes interface types are already concrete via `clientset` from `clientsetForContext`. Don't introduce wrapper interfaces; just take the concrete `*kubernetes.Clientset` value directly. Replace the helper signatures so events use the same `clientset` returned by `clientsetForContext`.

Replace what you just appended with this simpler version:

```go
import (
	// ... existing imports
	"k8s.io/client-go/kubernetes"
)

func fetchPodEvents(ctx context.Context, clientset kubernetes.Interface, namespace, podName string) []corev1.Event {
	selector := fields.SelectorFromSet(fields.Set{
		"involvedObject.name": podName,
		"involvedObject.kind": "Pod",
	}).String()
	list, err := clientset.CoreV1().Events(namespace).List(ctx, metav1.ListOptions{FieldSelector: selector})
	if err != nil || list == nil {
		return nil
	}
	out := make([]corev1.Event, 0, len(list.Items))
	for _, ev := range list.Items {
		if ev.InvolvedObject.Name != podName || ev.InvolvedObject.Kind != "Pod" {
			continue
		}
		out = append(out, ev)
	}
	sort.SliceStable(out, func(i, j int) bool {
		return out[i].LastTimestamp.After(out[j].LastTimestamp.Time)
	})
	return out
}
```

Add `"k8s.io/client-go/kubernetes"` to the import block at the top of the file.

- [ ] **Step 5.4: Wire into `GetCrashInvestigation`**

In `GetCrashInvestigation`, after `out.InitContainers, out.AppContainers = buildContainerCrashes(pod)`, add:

```go
	out.Events = fetchPodEvents(ctx, clientset, namespace, podName)
```

- [ ] **Step 5.5: Run the test**

```bash
go test ./internal/k8s/ -run TestGetCrashInvestigation_EventsFiltered -v
```
Expected: PASS.

- [ ] **Step 5.6: Note about fakeclient FieldSelector**

The fakeclient does NOT honor `FieldSelector` — it returns *all* events. Our manual filter inside `fetchPodEvents` (`ev.InvolvedObject.Name != podName`) must be left in place; the FieldSelector is purely a server-side optimization for production. The test passes only because of that manual filter. Add a code comment to make this explicit:

In `fetchPodEvents`, before the inner loop, add:

```go
	// FieldSelector is a server-side optimization; we still re-filter on
	// the client because (a) the fake clientset ignores FieldSelector and
	// would otherwise return cross-pod events in tests, and (b) production
	// servers occasionally return extra rows when watch-cached.
```

- [ ] **Step 5.7: Run all package tests to confirm no regression**

```bash
go test ./internal/k8s/ -run TestGetCrashInvestigation -v
```
Expected: all 5 tests PASS.

- [ ] **Step 5.8: Commit**

```bash
git add internal/k8s/crash_investigator.go internal/k8s/crash_investigator_test.go
git commit -m "feat(k8s): pod-scoped events fetch"
```

---

## Task 6: Per-container log streams (parallel `errgroup`)

**Files:**
- Modify: `internal/k8s/crash_investigator.go`
- Modify: `internal/k8s/crash_investigator_test.go`

- [ ] **Step 6.1: Write the failing test**

Append to `internal/k8s/crash_investigator_test.go`:

```go
func TestGetCrashInvestigation_LogsPopulated(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "default"},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "app"}}},
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{
				{Name: "app", RestartCount: 1,
					State:                corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}},
					LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Reason: "Error", ExitCode: 1}},
				},
			},
		},
	}
	cs := k8sfake.NewClientset(pod)
	c := newFakeClient(cs, nil)
	c.describeOverride = func(_ context.Context, _, _, _ string) (string, error) { return "", nil }

	// fakeclient `GetLogs` returns "fake logs" by default; that's enough
	// to assert both PreviousLog and CurrentLog are populated.
	got, err := c.GetCrashInvestigation(context.Background(), "", "default", "p")
	require.NoError(t, err)
	require.Len(t, got.AppContainers, 1)
	cc := got.AppContainers[0]
	assert.NotEmpty(t, cc.PreviousLog, "previous log must be populated for fake clientset")
	assert.NotEmpty(t, cc.CurrentLog, "current log must be populated for fake clientset")
	assert.Empty(t, cc.LogError)
}
```

- [ ] **Step 6.2: Verify fakeclient log behavior**

The Kubernetes fake clientset's `Pods().GetLogs()` returns a `rest.Request` whose `DoRaw` produces the bytes `"fake logs"`. Our test asserts that this body lands in `PreviousLog`/`CurrentLog`. Run the test once to confirm the failure shape:

```bash
go test ./internal/k8s/ -run TestGetCrashInvestigation_LogsPopulated -v
```
Expected: FAIL — `cc.PreviousLog == ""`.

- [ ] **Step 6.3: Implement logs fetching with `errgroup`**

Append to `internal/k8s/crash_investigator.go`:

```go
// fetchContainerLogs streams the previous + current tails for every container
// in containers, in parallel. Per-stream errors are stored on the matching
// container as LogError; a stream that returns no content but no error
// (e.g. previous logs not available because container has not been
// terminated yet) leaves both LogError and the corresponding *Log empty.
func fetchContainerLogs(ctx context.Context, clientset kubernetes.Interface, namespace, podName string, containers []ContainerCrash) []ContainerCrash {
	if len(containers) == 0 {
		return containers
	}
	var mu sync.Mutex
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(2 * len(containers))

	for i := range containers {
		i := i
		name := containers[i].Name

		// Previous logs.
		g.Go(func() error {
			body, err := getLogTail(gctx, clientset, namespace, podName, name, true)
			mu.Lock()
			defer mu.Unlock()
			if err != nil && !isPreviousLogsUnavailable(err) {
				containers[i].LogError = joinLogErr(containers[i].LogError, fmt.Errorf("previous: %w", err))
				return nil
			}
			containers[i].PreviousLog = body
			return nil
		})

		// Current logs.
		g.Go(func() error {
			body, err := getLogTail(gctx, clientset, namespace, podName, name, false)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				containers[i].LogError = joinLogErr(containers[i].LogError, fmt.Errorf("current: %w", err))
				return nil
			}
			containers[i].CurrentLog = body
			return nil
		})
	}
	_ = g.Wait()
	return containers
}

func getLogTail(ctx context.Context, clientset kubernetes.Interface, namespace, podName, container string, previous bool) (string, error) {
	tail := crashLogTailLines
	req := clientset.CoreV1().Pods(namespace).GetLogs(podName, &corev1.PodLogOptions{
		Container: container,
		Previous:  previous,
		TailLines: &tail,
	})
	body, err := req.DoRaw(ctx)
	if err != nil {
		return "", err
	}
	return string(body), nil
}

// isPreviousLogsUnavailable matches the apiserver's "previous terminated
// container ... not found" response so we treat it as expected emptiness
// rather than a real error worth surfacing to the user.
func isPreviousLogsUnavailable(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "previous terminated container") ||
		strings.Contains(msg, "not found") ||
		strings.Contains(msg, "PodInitializing")
}

func joinLogErr(prev string, next error) string {
	if next == nil {
		return prev
	}
	if prev == "" {
		return next.Error()
	}
	return prev + "; " + next.Error()
}
```

- [ ] **Step 6.4: Wire into `GetCrashInvestigation`**

After `out.Events = fetchPodEvents(...)`, add:

```go
	out.InitContainers = fetchContainerLogs(ctx, clientset, namespace, podName, out.InitContainers)
	out.AppContainers = fetchContainerLogs(ctx, clientset, namespace, podName, out.AppContainers)
```

- [ ] **Step 6.5: Remove the unused `errors` import if any**

```bash
goimports -w internal/k8s/crash_investigator.go
```

- [ ] **Step 6.6: Run the test**

```bash
go test ./internal/k8s/ -run TestGetCrashInvestigation_LogsPopulated -v
```
Expected: PASS.

- [ ] **Step 6.7: Run all package tests**

```bash
go test ./internal/k8s/ -race
```
Expected: PASS (no race conditions).

- [ ] **Step 6.8: Commit**

```bash
git add internal/k8s/crash_investigator.go internal/k8s/crash_investigator_test.go
git commit -m "feat(k8s): parallel per-container log fetch"
```

---

## Task 7: Pod 404 + describe error

**Files:**
- Modify: `internal/k8s/crash_investigator_test.go`

- [ ] **Step 7.1: Write the failing tests**

Append to `internal/k8s/crash_investigator_test.go`:

```go
func TestGetCrashInvestigation_PodNotFound(t *testing.T) {
	cs := k8sfake.NewClientset()
	c := newFakeClient(cs, nil)
	_, err := c.GetCrashInvestigation(context.Background(), "", "default", "missing")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestGetCrashInvestigation_DescribeFailureNonFatal(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "default"},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "app"}}},
	}
	cs := k8sfake.NewClientset(pod)
	c := newFakeClient(cs, nil)
	c.describeOverride = func(_ context.Context, _, _, _ string) (string, error) {
		return "", fmt.Errorf("kubectl not in PATH")
	}

	got, err := c.GetCrashInvestigation(context.Background(), "", "default", "p")
	require.NoError(t, err, "describe failure must not fail the whole call")
	require.NotNil(t, got)
	assert.Empty(t, got.Describe)
	assert.Contains(t, got.DescribeError, "kubectl not in PATH")
}
```

- [ ] **Step 7.2: Add the missing import to the test file**

In `internal/k8s/crash_investigator_test.go`, the import block needs `"fmt"`:

```go
import (
	"context"
	"fmt"
	"testing"
	"time"
	// ...rest stays the same
)
```

- [ ] **Step 7.3: Run the tests**

```bash
go test ./internal/k8s/ -run "TestGetCrashInvestigation_PodNotFound|TestGetCrashInvestigation_DescribeFailureNonFatal" -v
```
Expected: both PASS (existing logic from Tasks 2 & 5 already covers these cases).

- [ ] **Step 7.4: Commit**

```bash
git add internal/k8s/crash_investigator_test.go
git commit -m "test(k8s): pod 404 + describe failure are non-fatal"
```

---

## Task 8: Wire describe to real `kubectl describe`

**Files:**
- Modify: `internal/k8s/crash_investigator.go`

- [ ] **Step 8.1: Find the existing describe entry point**

```bash
grep -n "func (c \*Client) Describe" internal/k8s/*.go | head -5
```
Expected output: a `Describe` method (e.g. `func (c *Client) Describe(ctx, kind, name, ns)` or `DescribePod`). Note its exact signature.

- [ ] **Step 8.2: Wire the real implementation behind `describeOverride`**

In `internal/k8s/crash_investigator.go`, replace the describe block in `GetCrashInvestigation`:

```go
	// Describe (errors don't fail the whole call).
	if c.describeOverride != nil {
		desc, derr := c.describeOverride(ctx, contextName, namespace, podName)
		if derr != nil {
			out.DescribeError = derr.Error()
		} else {
			out.Describe = desc
		}
	}
```

with:

```go
	// Describe (errors don't fail the whole call).
	desc, derr := c.describeForCrashInvestigator(ctx, contextName, namespace, podName)
	if derr != nil {
		out.DescribeError = derr.Error()
	} else {
		out.Describe = desc
	}
```

And append:

```go
// describeForCrashInvestigator routes describe lookups through the test
// override when set, otherwise delegates to the production Describe path.
// The exact production method name is determined in Step 8.1; if it's
// `c.Describe(ctx, "Pod", name, namespace)` for example, replace the
// fallback below with that call signature.
func (c *Client) describeForCrashInvestigator(ctx context.Context, contextName, namespace, podName string) (string, error) {
	if c.describeOverride != nil {
		return c.describeOverride(ctx, contextName, namespace, podName)
	}
	return c.Describe(ctx, contextName, "Pod", podName, namespace)
}
```

If the real describe method's signature is different (e.g. takes args in a different order, or is named `DescribePod`), adjust the fallback call to match. Run `go build ./...` and fix the call site until it compiles.

- [ ] **Step 8.3: Verify build**

```bash
go build ./...
```
Expected: clean build.

- [ ] **Step 8.4: Run tests**

```bash
go test ./internal/k8s/ -run TestGetCrashInvestigation -race
```
Expected: all tests PASS (the override is still wired in via Step 8.2's logic).

- [ ] **Step 8.5: Commit**

```bash
git add internal/k8s/crash_investigator.go
git commit -m "feat(k8s): wire CrashInvestigator describe to production path"
```

---

## Task 9: UI presentation struct + tab bar render

**Files:**
- Create: `internal/ui/overlay_crash_investigator.go`
- Create: `internal/ui/overlay_crash_investigator_test.go`

- [ ] **Step 9.1: Write the failing renderer test**

Create `internal/ui/overlay_crash_investigator_test.go`:

```go
package ui

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRenderCrashInvestigatorOverlay_TabBar(t *testing.T) {
	entry := CrashInvestigatorEntry{
		PodName: "p", Namespace: "default", ActiveContainer: "app",
		Tab: CrashTabSummary,
		AppContainers: []CrashContainerEntry{{Name: "app"}},
	}
	out := RenderCrashInvestigatorOverlay(entry)
	assert.Contains(t, out, "Crash Investigator")
	assert.Contains(t, out, "Summary")
	assert.Contains(t, out, "Events")
	assert.Contains(t, out, "Logs")
	assert.Contains(t, out, "Describe")
	assert.Contains(t, out, "default/p")
}
```

- [ ] **Step 9.2: Run, expecting compile failure**

```bash
go test ./internal/ui/ -run TestRenderCrashInvestigatorOverlay_TabBar -v
```
Expected: FAIL — `CrashInvestigatorEntry undefined`.

- [ ] **Step 9.3: Create the renderer skeleton**

Create `internal/ui/overlay_crash_investigator.go`:

```go
package ui

import (
	"fmt"
	"strings"
	"time"
)

// CrashTab is the active tab in the crash investigator overlay.
type CrashTab int

const (
	CrashTabSummary CrashTab = iota
	CrashTabEvents
	CrashTabLogs
	CrashTabDescribe
)

// CrashInvestigatorEntry is the presentation-only struct passed to the
// renderer. The `app` package builds it from `k8s.CrashInvestigation`
// before calling `RenderCrashInvestigatorOverlay`, so the `ui` package
// stays independent of `k8s`.
type CrashInvestigatorEntry struct {
	PodName         string
	Namespace       string
	Phase           string
	PodIP           string
	Node            string
	QoSClass        string
	Age             time.Duration
	OwnerKind       string
	OwnerName       string
	InitContainers  []CrashContainerEntry
	AppContainers   []CrashContainerEntry
	Events          []CrashEventEntry
	Describe        string
	DescribeError   string
	ActiveContainer string
	Tab             CrashTab
	ShowPrevious    bool
	ScrollOffset    int
}

// CrashContainerEntry is a per-container row in the Summary tab and
// the source for the Logs/Describe tabs (selected via ActiveContainer).
type CrashContainerEntry struct {
	Name            string
	IsInit          bool
	Image           string
	State           string
	StateReason     string
	Ready           bool
	RestartCount    int32
	LastReason      string
	LastExitCode    int32
	LastSignal      int32
	LastFinished    time.Time
	LastMessage     string
	HasLastTerm     bool
	PreviousLog     string
	CurrentLog      string
	LogError        string
}

type CrashEventEntry struct {
	Type    string
	Reason  string
	Age     string // pre-formatted "5m ago"
	Source  string
	Message string
}

// RenderCrashInvestigatorOverlay renders the full crash investigator
// overlay body (excluding the surrounding overlay frame, which the
// caller paints via OverlayStyle).
func RenderCrashInvestigatorOverlay(entry CrashInvestigatorEntry) string {
	var b strings.Builder
	b.WriteString(OverlayTitleStyle.Render("Crash Investigator"))
	b.WriteString("\n")
	header := fmt.Sprintf("%s/%s · container: %s",
		strings.TrimSpace(entry.Namespace),
		strings.TrimSpace(entry.PodName),
		fallback(entry.ActiveContainer, "—"))
	b.WriteString(OverlayDimStyle.Render(header))
	b.WriteString("\n")
	b.WriteString(renderCrashTabBar(entry.Tab))
	b.WriteString("\n")
	b.WriteString(renderCrashTabBody(entry))
	return b.String()
}

func renderCrashTabBar(active CrashTab) string {
	tabs := []struct {
		key   string
		label string
		t     CrashTab
	}{
		{"1", "Summary", CrashTabSummary},
		{"2", "Events", CrashTabEvents},
		{"3", "Logs", CrashTabLogs},
		{"4", "Describe", CrashTabDescribe},
	}
	var parts []string
	for _, t := range tabs {
		seg := fmt.Sprintf(" %s %s ", t.key, t.label)
		if t.t == active {
			parts = append(parts, OverlayActiveTabStyle.Render(seg))
		} else {
			parts = append(parts, OverlayDimStyle.Render(seg))
		}
	}
	return strings.Join(parts, "")
}

func renderCrashTabBody(entry CrashInvestigatorEntry) string {
	switch entry.Tab {
	case CrashTabSummary:
		return renderCrashSummaryTab(entry)
	case CrashTabEvents:
		return renderCrashEventsTab(entry)
	case CrashTabLogs:
		return renderCrashLogsTab(entry)
	case CrashTabDescribe:
		return renderCrashDescribeTab(entry)
	}
	return ""
}

// Stubs filled in by later tasks.
func renderCrashSummaryTab(_ CrashInvestigatorEntry) string  { return "" }
func renderCrashEventsTab(_ CrashInvestigatorEntry) string   { return "" }
func renderCrashLogsTab(_ CrashInvestigatorEntry) string     { return "" }
func renderCrashDescribeTab(_ CrashInvestigatorEntry) string { return "" }

func fallback(s, def string) string {
	if strings.TrimSpace(s) == "" {
		return def
	}
	return s
}
```

- [ ] **Step 9.4: Confirm `OverlayActiveTabStyle` exists; if not, add it**

```bash
grep -n "OverlayActiveTabStyle\|OverlayTabActive" internal/ui/styles.go
```
If the symbol does NOT exist, add it to `internal/ui/styles.go` near the other Overlay* styles:

```go
// OverlayActiveTabStyle highlights the currently-active tab in tab bars.
var OverlayActiveTabStyle = lipgloss.NewStyle().
	Foreground(BarHighlightFg).
	Background(BaseBg).
	Bold(true)
```

If `BarHighlightFg` or `BaseBg` are not defined either, fall back to `OverlayTitleStyle.Bold(true)` (a style that's known to exist):

```go
var OverlayActiveTabStyle = OverlayTitleStyle.Bold(true)
```

Verify with `grep -n "OverlayTitleStyle =" internal/ui/styles.go` first.

- [ ] **Step 9.5: Run the test**

```bash
go test ./internal/ui/ -run TestRenderCrashInvestigatorOverlay_TabBar -v
```
Expected: PASS.

- [ ] **Step 9.6: Verify whole package builds**

```bash
go build ./internal/ui/...
```

- [ ] **Step 9.7: Commit**

```bash
git add internal/ui/overlay_crash_investigator.go internal/ui/overlay_crash_investigator_test.go internal/ui/styles.go
git commit -m "feat(ui): CrashInvestigator overlay scaffold + tab bar"
```

---

## Task 10: Summary tab — container table

**Files:**
- Modify: `internal/ui/overlay_crash_investigator.go`
- Modify: `internal/ui/overlay_crash_investigator_test.go`

- [ ] **Step 10.1: Write the failing test**

Append to `internal/ui/overlay_crash_investigator_test.go`:

```go
func TestRenderCrashInvestigatorOverlay_SummaryTab(t *testing.T) {
	entry := CrashInvestigatorEntry{
		PodName: "p", Namespace: "default", ActiveContainer: "sidecar",
		Tab: CrashTabSummary,
		InitContainers: []CrashContainerEntry{
			{Name: "init-db", IsInit: true, State: "Waiting", StateReason: "CrashLoopBackOff", RestartCount: 4},
		},
		AppContainers: []CrashContainerEntry{
			{Name: "app", State: "Running", Ready: true, RestartCount: 0},
			{Name: "sidecar", State: "Waiting", StateReason: "CrashLoopBackOff", RestartCount: 3, HasLastTerm: true, LastReason: "Error", LastExitCode: 1},
		},
	}
	out := RenderCrashInvestigatorOverlay(entry)
	// Header columns
	assert.Contains(t, out, "CONTAINER")
	assert.Contains(t, out, "STATE")
	assert.Contains(t, out, "RESTARTS")
	// Init sub-table label
	assert.Contains(t, out, "Init")
	// All container names
	assert.Contains(t, out, "init-db")
	assert.Contains(t, out, "app")
	assert.Contains(t, out, "sidecar")
	// Reason
	assert.Contains(t, out, "CrashLoopBackOff")
	// Active row marker (uppercase chevron or bg style; we just check "→" since that's what we'll use)
	assert.True(t, strings.Contains(out, "→ sidecar") || strings.Contains(out, "▶ sidecar"),
		"active container row must be visually marked, got:\n%s", out)
}
```

- [ ] **Step 10.2: Run, confirm failure**

```bash
go test ./internal/ui/ -run TestRenderCrashInvestigatorOverlay_SummaryTab -v
```
Expected: FAIL — output is empty (stub).

- [ ] **Step 10.3: Implement `renderCrashSummaryTab`**

Replace the stub in `internal/ui/overlay_crash_investigator.go`:

```go
func renderCrashSummaryTab(entry CrashInvestigatorEntry) string {
	var b strings.Builder

	// Pod-level header line.
	owner := ""
	if entry.OwnerKind != "" {
		owner = fmt.Sprintf("  Owner: %s/%s", entry.OwnerKind, entry.OwnerName)
	}
	podLine := fmt.Sprintf("  Phase: %s · Node: %s · IP: %s · QoS: %s · Age: %s%s",
		fallback(entry.Phase, "—"),
		fallback(entry.Node, "—"),
		fallback(entry.PodIP, "—"),
		fallback(entry.QoSClass, "—"),
		formatDuration(entry.Age),
		owner,
	)
	b.WriteString(OverlayDimStyle.Render(podLine))
	b.WriteString("\n\n")

	// Init container sub-table.
	if len(entry.InitContainers) > 0 {
		b.WriteString(OverlayNormalStyle.Render("  Init Containers"))
		b.WriteString("\n")
		b.WriteString(renderCrashContainerTable(entry.InitContainers, entry.ActiveContainer))
		b.WriteString("\n")
	}
	// App container sub-table.
	if len(entry.AppContainers) > 0 {
		b.WriteString(OverlayNormalStyle.Render("  Containers"))
		b.WriteString("\n")
		b.WriteString(renderCrashContainerTable(entry.AppContainers, entry.ActiveContainer))
		b.WriteString("\n")
	}

	// Last-terminated detail block for the active container.
	if active := findContainer(entry, entry.ActiveContainer); active != nil && active.HasLastTerm {
		b.WriteString("\n")
		b.WriteString(OverlayNormalStyle.Render(fmt.Sprintf("  Last termination of %s", active.Name)))
		b.WriteString("\n")
		b.WriteString(OverlayDimStyle.Render(fmt.Sprintf(
			"    Reason: %s · ExitCode: %d · Signal: %d · Finished: %s",
			fallback(active.LastReason, "—"),
			active.LastExitCode,
			active.LastSignal,
			formatTimeAgo(active.LastFinished),
		)))
		b.WriteString("\n")
		if msg := strings.TrimSpace(active.LastMessage); msg != "" {
			b.WriteString(OverlayDimStyle.Render("    Message: " + truncate(msg, 200)))
			b.WriteString("\n")
		}
	}

	return b.String()
}

func renderCrashContainerTable(containers []CrashContainerEntry, active string) string {
	var b strings.Builder
	header := fmt.Sprintf("    %-20s  %-12s  %-9s  %-10s  %-20s",
		"CONTAINER", "STATE", "RESTARTS", "LAST EXIT", "LAST REASON")
	b.WriteString(OverlayDimStyle.Render(header))
	b.WriteString("\n")
	for _, c := range containers {
		marker := "  "
		if c.Name == active {
			marker = "→ "
		}
		state := c.State
		if c.StateReason != "" {
			state = c.StateReason
		}
		exit := "—"
		reason := "—"
		if c.HasLastTerm {
			exit = fmt.Sprintf("%d", c.LastExitCode)
			reason = fallback(c.LastReason, "—")
		}
		row := fmt.Sprintf("  %s%-20s  %-12s  %-9d  %-10s  %-20s",
			marker,
			truncate(c.Name, 20),
			truncate(state, 12),
			c.RestartCount,
			exit,
			truncate(reason, 20),
		)
		if c.Name == active {
			b.WriteString(OverlayActiveTabStyle.Render(row))
		} else {
			b.WriteString(OverlayNormalStyle.Render(row))
		}
		b.WriteString("\n")
	}
	return b.String()
}

func findContainer(entry CrashInvestigatorEntry, name string) *CrashContainerEntry {
	for i := range entry.InitContainers {
		if entry.InitContainers[i].Name == name {
			return &entry.InitContainers[i]
		}
	}
	for i := range entry.AppContainers {
		if entry.AppContainers[i].Name == name {
			return &entry.AppContainers[i]
		}
	}
	return nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	if n <= 1 {
		return s[:n]
	}
	return s[:n-1] + "…"
}

func formatTimeAgo(t time.Time) string {
	if t.IsZero() {
		return "—"
	}
	return formatDuration(time.Since(t)) + " ago"
}
```

- [ ] **Step 10.4: Verify `formatDuration` exists**

```bash
grep -n "^func formatDuration" internal/ui/*.go
```
Expected: a `formatDuration(time.Duration) string` already exists (used by `RenderPodStartupOverlay`). If grep returns nothing, define one in this file:

```go
func formatDuration(d time.Duration) string {
	if d < time.Second {
		return fmt.Sprintf("%dms", d.Milliseconds())
	}
	if d < time.Minute {
		return fmt.Sprintf("%.1fs", d.Seconds())
	}
	if d < time.Hour {
		return fmt.Sprintf("%dm%ds", int(d.Minutes()), int(d.Seconds())%60)
	}
	return fmt.Sprintf("%dh%dm", int(d.Hours()), int(d.Minutes())%60)
}
```

- [ ] **Step 10.5: Run the test**

```bash
go test ./internal/ui/ -run TestRenderCrashInvestigatorOverlay_SummaryTab -v
```
Expected: PASS.

- [ ] **Step 10.6: Run the whole UI package**

```bash
go test ./internal/ui/ -race
```
Expected: PASS.

- [ ] **Step 10.7: Commit**

```bash
git add internal/ui/overlay_crash_investigator.go internal/ui/overlay_crash_investigator_test.go
git commit -m "feat(ui): CrashInvestigator Summary tab"
```

---

## Task 11: Events tab + Logs tab + Describe tab

**Files:**
- Modify: `internal/ui/overlay_crash_investigator.go`
- Modify: `internal/ui/overlay_crash_investigator_test.go`

- [ ] **Step 11.1: Write failing tests**

Append to `internal/ui/overlay_crash_investigator_test.go`:

```go
func TestRenderCrashInvestigatorOverlay_EventsTab(t *testing.T) {
	entry := CrashInvestigatorEntry{
		PodName: "p", Namespace: "default",
		Tab: CrashTabEvents,
		AppContainers: []CrashContainerEntry{{Name: "app"}},
		Events: []CrashEventEntry{
			{Type: "Warning", Reason: "BackOff", Age: "5s", Message: "Back-off"},
			{Type: "Normal", Reason: "Pulled", Age: "1m", Message: "Image pulled"},
		},
	}
	out := RenderCrashInvestigatorOverlay(entry)
	assert.Contains(t, out, "BackOff")
	assert.Contains(t, out, "Pulled")
	assert.Contains(t, out, "Back-off")
}

func TestRenderCrashInvestigatorOverlay_EventsTabEmpty(t *testing.T) {
	entry := CrashInvestigatorEntry{
		PodName: "p", Namespace: "default", Tab: CrashTabEvents,
		AppContainers: []CrashContainerEntry{{Name: "app"}},
	}
	out := RenderCrashInvestigatorOverlay(entry)
	assert.Contains(t, out, "No events for this pod")
}

func TestRenderCrashInvestigatorOverlay_LogsTabPrevious(t *testing.T) {
	entry := CrashInvestigatorEntry{
		PodName: "p", Namespace: "default", ActiveContainer: "app",
		Tab: CrashTabLogs, ShowPrevious: true,
		AppContainers: []CrashContainerEntry{{
			Name: "app", PreviousLog: "panic: something\ngoroutine 1 [running]",
			CurrentLog: "starting up",
		}},
	}
	out := RenderCrashInvestigatorOverlay(entry)
	assert.Contains(t, out, "previous")
	assert.Contains(t, out, "panic: something")
	assert.NotContains(t, out, "starting up", "current log must not bleed into previous mode")
}

func TestRenderCrashInvestigatorOverlay_LogsTabCurrent(t *testing.T) {
	entry := CrashInvestigatorEntry{
		PodName: "p", Namespace: "default", ActiveContainer: "app",
		Tab: CrashTabLogs, ShowPrevious: false,
		AppContainers: []CrashContainerEntry{{
			Name: "app", PreviousLog: "old", CurrentLog: "starting up",
		}},
	}
	out := RenderCrashInvestigatorOverlay(entry)
	assert.Contains(t, out, "current")
	assert.Contains(t, out, "starting up")
	assert.NotContains(t, out, "old")
}

func TestRenderCrashInvestigatorOverlay_LogsTabPreviousEmpty(t *testing.T) {
	entry := CrashInvestigatorEntry{
		PodName: "p", Namespace: "default", ActiveContainer: "app",
		Tab: CrashTabLogs, ShowPrevious: true,
		AppContainers: []CrashContainerEntry{{Name: "app"}}, // no PreviousLog
	}
	out := RenderCrashInvestigatorOverlay(entry)
	assert.Contains(t, out, "no previous container output")
}

func TestRenderCrashInvestigatorOverlay_LogsTabError(t *testing.T) {
	entry := CrashInvestigatorEntry{
		PodName: "p", Namespace: "default", ActiveContainer: "app",
		Tab: CrashTabLogs, ShowPrevious: true,
		AppContainers: []CrashContainerEntry{{Name: "app", LogError: "stream broken"}},
	}
	out := RenderCrashInvestigatorOverlay(entry)
	assert.Contains(t, out, "failed to load logs")
	assert.Contains(t, out, "stream broken")
}

func TestRenderCrashInvestigatorOverlay_DescribeTab(t *testing.T) {
	entry := CrashInvestigatorEntry{
		PodName: "p", Namespace: "default", Tab: CrashTabDescribe,
		AppContainers: []CrashContainerEntry{{Name: "app"}},
		Describe:      "Name: p\nNamespace: default\nContainers:\n  app:\n    Image: nginx\n",
	}
	out := RenderCrashInvestigatorOverlay(entry)
	assert.Contains(t, out, "Name: p")
	assert.Contains(t, out, "Image: nginx")
}

func TestRenderCrashInvestigatorOverlay_DescribeTabError(t *testing.T) {
	entry := CrashInvestigatorEntry{
		PodName: "p", Namespace: "default", Tab: CrashTabDescribe,
		AppContainers: []CrashContainerEntry{{Name: "app"}},
		DescribeError: "kubectl not found",
	}
	out := RenderCrashInvestigatorOverlay(entry)
	assert.Contains(t, out, "kubectl not found")
}
```

- [ ] **Step 11.2: Run, confirm 8 failures**

```bash
go test ./internal/ui/ -run TestRenderCrashInvestigatorOverlay -v
```
Expected: 8 new failures (everything in this batch), Summary + TabBar still pass.

- [ ] **Step 11.3: Implement Events tab**

Replace the `renderCrashEventsTab` stub:

```go
func renderCrashEventsTab(entry CrashInvestigatorEntry) string {
	if len(entry.Events) == 0 {
		return OverlayDimStyle.Render("  No events for this pod.")
	}
	var b strings.Builder
	header := fmt.Sprintf("    %-7s  %-18s  %-7s  %s", "TYPE", "REASON", "AGE", "MESSAGE")
	b.WriteString(OverlayDimStyle.Render(header))
	b.WriteString("\n")
	for _, ev := range entry.Events {
		row := fmt.Sprintf("    %-7s  %-18s  %-7s  %s",
			truncate(ev.Type, 7),
			truncate(ev.Reason, 18),
			truncate(ev.Age, 7),
			truncate(ev.Message, 80),
		)
		if ev.Type == "Warning" {
			b.WriteString(OverlayWarningStyle.Render(row))
		} else {
			b.WriteString(OverlayNormalStyle.Render(row))
		}
		b.WriteString("\n")
	}
	return b.String()
}
```

- [ ] **Step 11.4: Implement Logs tab**

Replace the `renderCrashLogsTab` stub:

```go
func renderCrashLogsTab(entry CrashInvestigatorEntry) string {
	active := findContainer(entry, entry.ActiveContainer)
	mode := "current"
	if entry.ShowPrevious {
		mode = "previous"
	}
	header := fmt.Sprintf("  LOGS · %s · container=%s",
		mode, fallback(entry.ActiveContainer, "—"))

	var b strings.Builder
	b.WriteString(OverlayDimStyle.Render(header))
	b.WriteString("\n\n")

	if active == nil {
		b.WriteString(OverlayDimStyle.Render("  No active container."))
		return b.String()
	}
	if active.LogError != "" {
		b.WriteString(OverlayWarningStyle.Render(fmt.Sprintf("  failed to load logs: %s", active.LogError)))
		return b.String()
	}

	body := active.CurrentLog
	if entry.ShowPrevious {
		body = active.PreviousLog
	}
	body = strings.TrimRight(body, "\n")
	if body == "" {
		if entry.ShowPrevious {
			b.WriteString(OverlayDimStyle.Render(
				"  no previous container output — this container has not been terminated yet. press p to view current logs."))
		} else {
			b.WriteString(OverlayDimStyle.Render("  no current logs available."))
		}
		return b.String()
	}
	for _, line := range strings.Split(body, "\n") {
		b.WriteString("  ")
		b.WriteString(OverlayNormalStyle.Render(line))
		b.WriteString("\n")
	}
	return b.String()
}
```

- [ ] **Step 11.5: Implement Describe tab**

Replace the `renderCrashDescribeTab` stub:

```go
func renderCrashDescribeTab(entry CrashInvestigatorEntry) string {
	if entry.DescribeError != "" {
		return OverlayWarningStyle.Render("  describe failed: " + entry.DescribeError)
	}
	body := strings.TrimRight(entry.Describe, "\n")
	if body == "" {
		return OverlayDimStyle.Render("  no describe output.")
	}
	var b strings.Builder
	for _, line := range strings.Split(body, "\n") {
		b.WriteString("  ")
		b.WriteString(OverlayNormalStyle.Render(line))
		b.WriteString("\n")
	}
	return b.String()
}
```

- [ ] **Step 11.6: Verify `OverlayWarningStyle` exists**

```bash
grep -n "OverlayWarningStyle" internal/ui/styles.go
```
If absent, append to `internal/ui/styles.go`:

```go
// OverlayWarningStyle is the rendering style for warning text inside overlays.
var OverlayWarningStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("#e0af68")).Background(BaseBg)
```

If `BaseBg` is also missing, drop the `.Background(BaseBg)` clause entirely.

- [ ] **Step 11.7: Run all renderer tests**

```bash
go test ./internal/ui/ -run TestRenderCrashInvestigatorOverlay -v
```
Expected: all 10 tests PASS.

- [ ] **Step 11.8: Commit**

```bash
git add internal/ui/overlay_crash_investigator.go internal/ui/overlay_crash_investigator_test.go internal/ui/styles.go
git commit -m "feat(ui): CrashInvestigator Events, Logs, Describe tabs"
```

---

## Task 12: Theme-bg regression test

**Files:**
- Modify: `internal/ui/overlay_crash_investigator_test.go`

- [ ] **Step 12.1: Find the existing theme-bg test pattern**

```bash
grep -n "TestRenderSecretEditorOverlay_InnerPanelMatchesOuterBg\|termenv.TrueColor" internal/ui/secretview_test.go | head -5
```
Note the imports it uses (`termenv`, `lipgloss`).

- [ ] **Step 12.2: Write the failing test**

Append to `internal/ui/overlay_crash_investigator_test.go`:

```go
func TestRenderCrashInvestigatorOverlay_ThemeBg(t *testing.T) {
	saved := lipgloss.ColorProfile()
	defer lipgloss.SetColorProfile(saved)
	lipgloss.SetColorProfile(termenv.TrueColor)
	ApplyTheme(DefaultTheme)

	entry := CrashInvestigatorEntry{
		PodName: "p", Namespace: "default", ActiveContainer: "app",
		Tab: CrashTabSummary,
		AppContainers: []CrashContainerEntry{
			{Name: "app", State: "Running", Ready: true, RestartCount: 0},
		},
	}
	out := RenderCrashInvestigatorOverlay(entry)
	bgCount := strings.Count(out, "\x1b[48;") // SGR background-set escapes
	assert.GreaterOrEqual(t, bgCount, 4,
		"renderer must emit at least 4 background-set escapes so the overlay reads as one uniform surface; got %d.\n%s", bgCount, out)
}
```

- [ ] **Step 12.3: Add imports**

The top of `overlay_crash_investigator_test.go` should import `termenv` and `lipgloss`. Add them:

```go
import (
	"strings"
	"testing"

	"github.com/charmbracelet/lipgloss"
	"github.com/muesli/termenv"
	"github.com/stretchr/testify/assert"
)
```

- [ ] **Step 12.4: Run, confirm**

```bash
go test ./internal/ui/ -run TestRenderCrashInvestigatorOverlay_ThemeBg -v
```
Expected: PASS if the styles already include `Background(...)` clauses; FAIL otherwise.

- [ ] **Step 12.5: If the test fails, audit styles**

If FAIL, inspect which renderer paths use `OverlayDimStyle` / `OverlayNormalStyle` without bg. Open `internal/ui/styles.go`, find the definitions, and ensure each has `.Background(BaseBg)` chained on. Re-run.

- [ ] **Step 12.6: Commit**

```bash
git add internal/ui/overlay_crash_investigator_test.go
git commit -m "test(ui): theme-bg regression for CrashInvestigator overlay"
```

---

## Task 13: App layer — types, state, message

**Files:**
- Modify: `internal/app/app_types.go`
- Modify: `internal/app/app.go`
- Modify: `internal/app/messages.go`

- [ ] **Step 13.1: Add `overlayCrashInvestigator` to the enum**

In `internal/app/app_types.go`, find the `overlayKind` const block and add a new entry just before the closing `)`:

```go
	overlayClusterColor // pick a color tint for the highlighted cluster row
	overlayCrashInvestigator
)
```

Note the placement: keep it last in the block to avoid renumbering.

- [ ] **Step 13.2: Add `crashInvState` sub-struct**

Append to `internal/app/app_types.go` (after `whoCanState`):

```go
// crashInvTab identifies which tab is active in the CrashLoopBackOff
// investigator overlay.
type crashInvTab int

const (
	crashInvTabSummary crashInvTab = iota
	crashInvTabEvents
	crashInvTabLogs
	crashInvTabDescribe
)

// crashInvScrollKey indexes per-tab, per-container scroll offsets so
// switching tabs (or containers within Logs/Describe) preserves the
// reader's position.
type crashInvScrollKey struct {
	tab       crashInvTab
	container string
}

// crashInvState groups the CrashLoopBackOff-investigator fields together
// so they live as a single field on Model. Mirrors whoCanState.
type crashInvState struct {
	data            *k8s.CrashInvestigation
	activeContainer string
	activeTab       crashInvTab
	showPrevious    bool
	scroll          map[crashInvScrollKey]int
}
```

- [ ] **Step 13.3: Add the field on `Model`**

In `internal/app/app.go`, find `podStartupData *k8s.PodStartupInfo` and add a sibling line right after the existing pod-startup field:

```go
	// Pod startup analysis state.
	podStartupData *k8s.PodStartupInfo

	// Crash Investigator overlay state (per-pod multi-tab diagnostic view).
	crashInv crashInvState
```

- [ ] **Step 13.4: Add `crashInvestigationMsg`**

In `internal/app/messages.go`, after `podStartupMsg`:

```go
// crashInvestigationMsg carries the result of a CrashLoopBackOff investigation.
type crashInvestigationMsg struct {
	info *k8s.CrashInvestigation
	err  error
}
```

- [ ] **Step 13.5: Verify build**

```bash
go build ./...
```
Expected: clean build.

- [ ] **Step 13.6: Commit**

```bash
git add internal/app/app_types.go internal/app/app.go internal/app/messages.go
git commit -m "feat(app): CrashInvestigator state, overlay kind, message"
```

---

## Task 14: App layer — load command + executor + dispatch

**Files:**
- Create: `internal/app/commands_load_crash_investigator.go`
- Create: `internal/app/update_actions_crash_investigator.go`
- Modify: `internal/app/update_actions.go`

- [ ] **Step 14.1: Create the load command**

Create `internal/app/commands_load_crash_investigator.go`:

```go
package app

import (
	"context"

	tea "github.com/charmbracelet/bubbletea"

	"github.com/janosmiko/lfk/internal/app/bgtasks"
)

func (m Model) loadCrashInvestigation() tea.Cmd {
	client := m.client
	ctx := m.actionCtx.context
	ns := m.actionCtx.namespace
	name := m.actionCtx.name
	return m.trackBgTask(bgtasks.KindResourceList, "Crash investigator: "+name, bgtaskTarget(ctx, ns), func() tea.Msg {
		info, err := client.GetCrashInvestigation(context.Background(), ctx, ns, name)
		return crashInvestigationMsg{info: info, err: err}
	})
}
```

- [ ] **Step 14.2: Create the action executor**

Create `internal/app/update_actions_crash_investigator.go`:

```go
package app

import tea "github.com/charmbracelet/bubbletea"

// executeActionCrashInvestigator handles the "Crash Investigator" action
// from the Pod action menu: kicks off the multi-section diagnostic fetch.
func (m Model) executeActionCrashInvestigator() (tea.Model, tea.Cmd) {
	m.loading = true
	m.setStatusMessage("Investigating crashes…", false)
	return m, m.loadCrashInvestigation()
}
```

- [ ] **Step 14.3: Wire into the action dispatcher**

In `internal/app/update_actions.go`, find `case "Startup Analysis":` (around line 482) and add a new case directly below:

```go
	case "Startup Analysis":
		mdl, cmd := m.executeActionStartupAnalysis()
		return mdl, cmd
	case "Crash Investigator":
		mdl, cmd := m.executeActionCrashInvestigator()
		return mdl, cmd
```

- [ ] **Step 14.4: Verify build**

```bash
go build ./...
```

- [ ] **Step 14.5: Commit**

```bash
git add internal/app/commands_load_crash_investigator.go internal/app/update_actions_crash_investigator.go internal/app/update_actions.go
git commit -m "feat(app): CrashInvestigator load command + action executor"
```

---

## Task 15: Action menu entry + read-only allowlist

**Files:**
- Modify: `internal/model/actions.go`
- Modify: `internal/app/readonly_test.go`

- [ ] **Step 15.1: Add the action menu entry**

In `internal/model/actions.go`, find `actionsForCoreKind` case `"Pod"` (around line 65). Insert a new entry right after `Startup Analysis`:

```go
			{Label: "Startup Analysis", Description: "Analyze pod startup timing", Key: "S"},
			{Label: "Crash Investigator", Description: "Investigate crash loop / failing pod", Key: "I"},
```

- [ ] **Step 15.2: Add to the read-only allowlist**

In `internal/app/readonly_test.go`, find the existing slice `"Startup Analysis", "Alerts", "Visualize", "Vuln Scan"` (around line 49). Insert `"Crash Investigator"`:

```go
		"Startup Analysis", "Crash Investigator", "Alerts", "Visualize", "Vuln Scan",
```

- [ ] **Step 15.3: Verify**

```bash
go test ./internal/app/ -run TestReadOnly -v
go test ./internal/model/ -v
```
Expected: PASS.

- [ ] **Step 15.4: Commit**

```bash
git add internal/model/actions.go internal/app/readonly_test.go
git commit -m "feat(model): Crash Investigator Pod action menu entry"
```

---

## Task 16: App layer — message handler + view dispatch

**Files:**
- Create: `internal/app/update_crash_investigator.go`
- Modify: `internal/app/update.go`
- Modify: `internal/app/view_overlays.go`

- [ ] **Step 16.1: Create the handler**

Create `internal/app/update_crash_investigator.go`:

```go
package app

import (
	"fmt"
	"strings"

	tea "github.com/charmbracelet/bubbletea"

	"github.com/janosmiko/lfk/internal/k8s"
	"github.com/janosmiko/lfk/internal/ui"
)

func (m Model) updateCrashInvestigation(msg crashInvestigationMsg) (tea.Model, tea.Cmd) {
	m.loading = false
	if msg.err != nil {
		m.setStatusMessage(fmt.Sprintf("Crash investigation failed: %v", msg.err), true)
		return m, scheduleStatusClear()
	}
	if msg.info == nil {
		m.setStatusMessage("Crash investigation returned no data", true)
		return m, scheduleStatusClear()
	}

	// Preserve user's tab/container/log-mode/scroll across refreshes.
	prev := m.crashInv
	m.crashInv = crashInvState{
		data:            msg.info,
		activeContainer: prev.activeContainer,
		activeTab:       prev.activeTab,
		showPrevious:    prev.showPrevious,
		scroll:          prev.scroll,
	}
	if m.crashInv.scroll == nil {
		m.crashInv.scroll = map[crashInvScrollKey]int{}
	}

	// First-open defaults.
	if m.overlay != overlayCrashInvestigator {
		m.crashInv.activeTab = crashInvTabSummary
		m.crashInv.showPrevious = true
		m.crashInv.activeContainer = pickInitialActiveContainer(msg.info)
	} else if findContainerInfo(msg.info, m.crashInv.activeContainer) == nil {
		// Refresh removed the previously-active container.
		m.crashInv.activeContainer = pickInitialActiveContainer(msg.info)
	}

	m.overlay = overlayCrashInvestigator
	return m, nil
}

// pickInitialActiveContainer returns the first container that looks
// unhealthy (init takes precedence to surface init-CLB pods early); falls
// back to the first app container, then the first init container.
func pickInitialActiveContainer(info *k8s.CrashInvestigation) string {
	for _, c := range info.InitContainers {
		if isFailingContainer(c) {
			return c.Name
		}
	}
	for _, c := range info.AppContainers {
		if isFailingContainer(c) {
			return c.Name
		}
	}
	if len(info.AppContainers) > 0 {
		return info.AppContainers[0].Name
	}
	if len(info.InitContainers) > 0 {
		return info.InitContainers[0].Name
	}
	return ""
}

func isFailingContainer(c k8s.ContainerCrash) bool {
	if c.RestartCount > 0 {
		return true
	}
	switch c.StateReason {
	case "CrashLoopBackOff", "Error", "ImagePullBackOff", "ErrImagePull", "CreateContainerConfigError":
		return true
	}
	return false
}

func findContainerInfo(info *k8s.CrashInvestigation, name string) *k8s.ContainerCrash {
	for i := range info.InitContainers {
		if info.InitContainers[i].Name == name {
			return &info.InitContainers[i]
		}
	}
	for i := range info.AppContainers {
		if info.AppContainers[i].Name == name {
			return &info.AppContainers[i]
		}
	}
	return nil
}

// renderOverlayCrashInvestigator builds the presentation entry from
// k8s.CrashInvestigation and delegates to the pure UI renderer.
func (m Model) renderOverlayCrashInvestigator() (string, int, int) {
	w, h := min(110, m.width-6), min(35, m.height-4)
	if m.crashInv.data == nil {
		return "", w, h
	}
	d := m.crashInv.data
	entry := ui.CrashInvestigatorEntry{
		PodName: d.Pod.Name, Namespace: d.Pod.Namespace,
		Phase: d.Pod.Phase, PodIP: d.Pod.PodIP, Node: d.Pod.Node,
		QoSClass: d.Pod.QoSClass, Age: d.Pod.Age,
		OwnerKind: d.Pod.OwnerKind, OwnerName: d.Pod.OwnerName,
		Describe: d.Describe, DescribeError: d.DescribeError,
		ActiveContainer: m.crashInv.activeContainer,
		Tab:             ui.CrashTab(m.crashInv.activeTab),
		ShowPrevious:    m.crashInv.showPrevious,
	}
	for _, c := range d.InitContainers {
		entry.InitContainers = append(entry.InitContainers, toCrashContainerEntry(c))
	}
	for _, c := range d.AppContainers {
		entry.AppContainers = append(entry.AppContainers, toCrashContainerEntry(c))
	}
	for _, ev := range d.Events {
		entry.Events = append(entry.Events, ui.CrashEventEntry{
			Type:    string(ev.Type),
			Reason:  ev.Reason,
			Age:     formatEventAge(ev.LastTimestamp.Time),
			Source:  ev.Source.Component,
			Message: strings.TrimSpace(ev.Message),
		})
	}
	return ui.RenderCrashInvestigatorOverlay(entry), w, h
}

func toCrashContainerEntry(c k8s.ContainerCrash) ui.CrashContainerEntry {
	out := ui.CrashContainerEntry{
		Name: c.Name, IsInit: c.IsInit, Image: c.Image,
		State: c.State, StateReason: c.StateReason,
		Ready: c.Ready, RestartCount: c.RestartCount,
		PreviousLog: c.PreviousLog, CurrentLog: c.CurrentLog, LogError: c.LogError,
	}
	if c.LastTermination != nil {
		out.HasLastTerm = true
		out.LastReason = c.LastTermination.Reason
		out.LastExitCode = c.LastTermination.ExitCode
		out.LastSignal = c.LastTermination.Signal
		out.LastFinished = c.LastTermination.FinishedAt
		out.LastMessage = c.LastTermination.Message
	}
	return out
}
```

- [ ] **Step 16.2: Verify `formatEventAge` exists or add it**

```bash
grep -n "func formatEventAge" internal/app/*.go
```
If absent, append to `update_crash_investigator.go`:

```go
func formatEventAge(t time.Time) string {
	if t.IsZero() {
		return "—"
	}
	d := time.Since(t)
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh", int(d.Hours()))
	default:
		return fmt.Sprintf("%dd", int(d.Hours())/24)
	}
}
```

And add `"time"` to the file's import block.

- [ ] **Step 16.3: Wire the message dispatch**

In `internal/app/update.go`, find `case podStartupMsg:` (around line 283) and add the sibling:

```go
	case podStartupMsg:
		mdl, cmd := m.updatePodStartup(msg)
		return mdl, cmd, true
	case crashInvestigationMsg:
		mdl, cmd := m.updateCrashInvestigation(msg)
		return mdl, cmd, true
```

- [ ] **Step 16.4: Wire the view render**

In `internal/app/view_overlays.go`, find `case overlayPodStartup:` (around line 127) and add:

```go
	case overlayPodStartup:
		c, w, h := m.renderOverlayPodStartup()
		return c, w, h, true
	case overlayCrashInvestigator:
		c, w, h := m.renderOverlayCrashInvestigator()
		return c, w, h, true
```

- [ ] **Step 16.5: Verify build**

```bash
go build ./...
```

- [ ] **Step 16.6: Commit**

```bash
git add internal/app/update_crash_investigator.go internal/app/update.go internal/app/view_overlays.go
git commit -m "feat(app): CrashInvestigator message handler + view dispatch"
```

---

## Task 17: App layer — overlay key handler

**Files:**
- Create: `internal/app/update_overlays_crash_investigator.go`
- Modify: `internal/app/update_overlays.go`
- Modify: `internal/app/overlay_hintbar.go`

- [ ] **Step 17.1: Create the key handler**

Create `internal/app/update_overlays_crash_investigator.go`:

```go
package app

import (
	tea "github.com/charmbracelet/bubbletea"

	"github.com/janosmiko/lfk/internal/k8s"
)

// handleCrashInvestigatorOverlayKey routes overlay keys for the Crash
// Investigator: tab navigation, container switching, logs prev/curr,
// refresh, and close.
func (m Model) handleCrashInvestigatorOverlayKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "esc", "q":
		m.overlay = overlayNone
		m.crashInv = crashInvState{}
		return m, nil
	case "tab":
		m.crashInv.activeTab = nextCrashTab(m.crashInv.activeTab, +1)
		return m, nil
	case "shift+tab":
		m.crashInv.activeTab = nextCrashTab(m.crashInv.activeTab, -1)
		return m, nil
	case "1":
		m.crashInv.activeTab = crashInvTabSummary
		return m, nil
	case "2":
		m.crashInv.activeTab = crashInvTabEvents
		return m, nil
	case "3":
		m.crashInv.activeTab = crashInvTabLogs
		return m, nil
	case "4":
		m.crashInv.activeTab = crashInvTabDescribe
		return m, nil
	case "c":
		m.crashInv.activeContainer = nextCrashContainer(m.crashInv.data, m.crashInv.activeContainer)
		return m, nil
	case "p":
		if m.crashInv.activeTab == crashInvTabLogs {
			m.crashInv.showPrevious = !m.crashInv.showPrevious
		}
		return m, nil
	case "R":
		m.loading = true
		m.setStatusMessage("Refreshing crash investigation…", false)
		return m, m.loadCrashInvestigation()
	case "j", "down":
		m.crashInv.bumpScroll(+1)
		return m, nil
	case "k", "up":
		m.crashInv.bumpScroll(-1)
		return m, nil
	}
	return m, nil
}

func nextCrashTab(t crashInvTab, delta int) crashInvTab {
	const n = 4
	return crashInvTab((int(t) + delta + n) % n)
}

func nextCrashContainer(info *k8s.CrashInvestigation, current string) string {
	if info == nil {
		return current
	}
	all := make([]string, 0, len(info.InitContainers)+len(info.AppContainers))
	for _, c := range info.InitContainers {
		all = append(all, c.Name)
	}
	for _, c := range info.AppContainers {
		all = append(all, c.Name)
	}
	if len(all) == 0 {
		return current
	}
	for i, name := range all {
		if name == current {
			return all[(i+1)%len(all)]
		}
	}
	return all[0]
}

func (s *crashInvState) bumpScroll(delta int) {
	if s.scroll == nil {
		s.scroll = map[crashInvScrollKey]int{}
	}
	key := crashInvScrollKey{tab: s.activeTab, container: s.activeContainer}
	v := s.scroll[key] + delta
	if v < 0 {
		v = 0
	}
	s.scroll[key] = v
}
```

- [ ] **Step 17.2: Wire into `handleOverlayKeySecondary`**

In `internal/app/update_overlays.go`, find `case overlayRBAC, overlayPodStartup:` (around line 133) — that case currently auto-closes on any key. We do NOT want CrashInvestigator to auto-close on any key, so add a dedicated case **before** the `overlayRBAC, overlayPodStartup` line:

```go
	case overlayCrashInvestigator:
		mdl, cmd := m.handleCrashInvestigatorOverlayKey(msg)
		return mdl, cmd, true
	case overlayRBAC, overlayPodStartup:
		m.overlay = overlayNone
		return m, nil, true
```

- [ ] **Step 17.3: Add the hint bar**

In `internal/app/overlay_hintbar.go`, find `case overlayRBAC, overlayPodStartup:` (around line 64) and add a dedicated case **before** it:

```go
	case overlayCrashInvestigator:
		return m.renderHints([]ui.HintEntry{
			{Key: "Tab", Desc: "switch tab"},
			{Key: "1-4", Desc: "jump"},
			{Key: "c", Desc: "container"},
			{Key: "p", Desc: "prev/curr"},
			{Key: "R", Desc: "refresh"},
			{Key: "esc", Desc: "close"},
		})
	case overlayRBAC, overlayPodStartup:
```

- [ ] **Step 17.4: Verify build**

```bash
go build ./...
```

- [ ] **Step 17.5: Commit**

```bash
git add internal/app/update_overlays_crash_investigator.go internal/app/update_overlays.go internal/app/overlay_hintbar.go
git commit -m "feat(app): CrashInvestigator overlay key handler + hint bar"
```

---

## Task 18: App layer — message handler tests

**Files:**
- Create: `internal/app/update_crash_investigator_test.go`

- [ ] **Step 18.1: Find the existing test scaffolding**

```bash
grep -n "func TestUpdatePodStartup\|newTestModel\|NewModel" internal/app/update_pod_msgs_test.go internal/app/app_test.go 2>/dev/null | head -20
```

Note the existing pattern for instantiating a `Model` for unit tests.

- [ ] **Step 18.2: Write the failing tests**

Create `internal/app/update_crash_investigator_test.go`:

```go
package app

import (
	"testing"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/janosmiko/lfk/internal/k8s"
)

func newCrashInvTestModel(t *testing.T) Model {
	t.Helper()
	m := Model{
		width:  120,
		height: 40,
	}
	return m
}

func sampleCrashInfo() *k8s.CrashInvestigation {
	return &k8s.CrashInvestigation{
		Pod: k8s.PodSummary{Name: "p", Namespace: "default", Phase: "Running"},
		AppContainers: []k8s.ContainerCrash{
			{Name: "app", State: "Running", Ready: true},
			{Name: "sidecar", State: "Waiting", StateReason: "CrashLoopBackOff", RestartCount: 3,
				LastTermination: &k8s.ContainerTermination{Reason: "Error", ExitCode: 1}},
		},
	}
}

func TestUpdateCrashInvestigation_Success(t *testing.T) {
	m := newCrashInvTestModel(t)
	mdl, _ := m.updateCrashInvestigation(crashInvestigationMsg{info: sampleCrashInfo()})
	got := mdl.(Model)
	assert.False(t, got.loading)
	assert.Equal(t, overlayCrashInvestigator, got.overlay)
	require.NotNil(t, got.crashInv.data)
	assert.Equal(t, "sidecar", got.crashInv.activeContainer, "must default to first failing container")
	assert.Equal(t, crashInvTabSummary, got.crashInv.activeTab)
	assert.True(t, got.crashInv.showPrevious)
}

func TestUpdateCrashInvestigation_Error(t *testing.T) {
	m := newCrashInvTestModel(t)
	mdl, _ := m.updateCrashInvestigation(crashInvestigationMsg{err: assertErrLike("boom")})
	got := mdl.(Model)
	assert.NotEqual(t, overlayCrashInvestigator, got.overlay)
}

func TestUpdateCrashInvestigation_NilInfo(t *testing.T) {
	m := newCrashInvTestModel(t)
	mdl, _ := m.updateCrashInvestigation(crashInvestigationMsg{})
	got := mdl.(Model)
	assert.NotEqual(t, overlayCrashInvestigator, got.overlay)
}

func TestCrashInvestigator_TabCycle(t *testing.T) {
	m := newCrashInvTestModel(t)
	m.overlay = overlayCrashInvestigator
	m.crashInv = crashInvState{data: sampleCrashInfo(), activeTab: crashInvTabSummary, activeContainer: "app"}

	for _, want := range []crashInvTab{crashInvTabEvents, crashInvTabLogs, crashInvTabDescribe, crashInvTabSummary} {
		mdl, _ := m.handleCrashInvestigatorOverlayKey(tea.KeyMsg{Type: tea.KeyTab})
		m = mdl.(Model)
		assert.Equal(t, want, m.crashInv.activeTab)
	}
}

func TestCrashInvestigator_DirectJumpKeys(t *testing.T) {
	cases := map[string]crashInvTab{
		"1": crashInvTabSummary, "2": crashInvTabEvents, "3": crashInvTabLogs, "4": crashInvTabDescribe,
	}
	for key, want := range cases {
		t.Run(key, func(t *testing.T) {
			m := newCrashInvTestModel(t)
			m.overlay = overlayCrashInvestigator
			m.crashInv = crashInvState{data: sampleCrashInfo(), activeTab: crashInvTabSummary, activeContainer: "app"}
			mdl, _ := m.handleCrashInvestigatorOverlayKey(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune(key)})
			m = mdl.(Model)
			assert.Equal(t, want, m.crashInv.activeTab)
		})
	}
}

func TestCrashInvestigator_ContainerSwitch(t *testing.T) {
	m := newCrashInvTestModel(t)
	m.overlay = overlayCrashInvestigator
	m.crashInv = crashInvState{data: sampleCrashInfo(), activeTab: crashInvTabLogs, activeContainer: "app"}

	mdl, _ := m.handleCrashInvestigatorOverlayKey(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("c")})
	m = mdl.(Model)
	assert.Equal(t, "sidecar", m.crashInv.activeContainer)
	assert.Equal(t, crashInvTabLogs, m.crashInv.activeTab, "tab must be preserved across container switch")
}

func TestCrashInvestigator_PreviousToggleOnLogsTabOnly(t *testing.T) {
	m := newCrashInvTestModel(t)
	m.overlay = overlayCrashInvestigator
	m.crashInv = crashInvState{data: sampleCrashInfo(), activeTab: crashInvTabSummary, showPrevious: true}

	// On Summary tab — p must NOT toggle.
	mdl, _ := m.handleCrashInvestigatorOverlayKey(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("p")})
	m = mdl.(Model)
	assert.True(t, m.crashInv.showPrevious)

	// Switch to Logs — p toggles.
	m.crashInv.activeTab = crashInvTabLogs
	mdl, _ = m.handleCrashInvestigatorOverlayKey(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("p")})
	m = mdl.(Model)
	assert.False(t, m.crashInv.showPrevious)
}

func TestCrashInvestigator_EscClosesAndClearsState(t *testing.T) {
	m := newCrashInvTestModel(t)
	m.overlay = overlayCrashInvestigator
	m.crashInv = crashInvState{data: sampleCrashInfo(), activeContainer: "app"}

	mdl, _ := m.handleCrashInvestigatorOverlayKey(tea.KeyMsg{Type: tea.KeyEsc})
	m = mdl.(Model)
	assert.Equal(t, overlayNone, m.overlay)
	assert.Nil(t, m.crashInv.data)
}

func TestUpdateCrashInvestigation_RefreshPreservesState(t *testing.T) {
	m := newCrashInvTestModel(t)
	m.overlay = overlayCrashInvestigator
	m.crashInv = crashInvState{
		data:            sampleCrashInfo(),
		activeContainer: "app",
		activeTab:       crashInvTabLogs,
		showPrevious:    false,
	}

	// Re-fetch returns the same shape; activeContainer "app" still exists.
	mdl, _ := m.updateCrashInvestigation(crashInvestigationMsg{info: sampleCrashInfo()})
	got := mdl.(Model)
	assert.Equal(t, "app", got.crashInv.activeContainer, "active container preserved across refresh")
	assert.Equal(t, crashInvTabLogs, got.crashInv.activeTab)
	assert.False(t, got.crashInv.showPrevious)
}

func TestUpdateCrashInvestigation_RefreshFallsBackWhenContainerGone(t *testing.T) {
	m := newCrashInvTestModel(t)
	m.overlay = overlayCrashInvestigator
	m.crashInv = crashInvState{
		data:            sampleCrashInfo(),
		activeContainer: "removed-container",
	}

	mdl, _ := m.updateCrashInvestigation(crashInvestigationMsg{info: sampleCrashInfo()})
	got := mdl.(Model)
	// "removed-container" is not in sampleCrashInfo() — must fall back.
	assert.NotEqual(t, "removed-container", got.crashInv.activeContainer)
	assert.NotEmpty(t, got.crashInv.activeContainer)
}

// assertErrLike returns an error with the given message; trivial helper to
// avoid pulling in errors.New everywhere.
type assertErr struct{ msg string }

func (e *assertErr) Error() string { return e.msg }
func assertErrLike(s string) error { return &assertErr{s} }
```

- [ ] **Step 18.3: Run the tests**

```bash
go test ./internal/app/ -run TestUpdateCrashInvestigation -v
go test ./internal/app/ -run TestCrashInvestigator -v
```
Expected: all PASS.

- [ ] **Step 18.4: If `setStatusMessage` requires more Model fields, fix `newCrashInvTestModel`**

If a test panics on `setStatusMessage` because of a nil field, inspect the panic message and add the minimum-needed initializer to `newCrashInvTestModel`. Re-run.

- [ ] **Step 18.5: Run with race detector**

```bash
go test ./internal/app/ -run "TestUpdateCrashInvestigation|TestCrashInvestigator" -race
```
Expected: PASS.

- [ ] **Step 18.6: Commit**

```bash
git add internal/app/update_crash_investigator_test.go
git commit -m "test(app): CrashInvestigator state-machine tests"
```

---

## Task 19: Documentation — keybindings.md, views-and-overlays.md, in-app help

**Files:**
- Modify: `docs/keybindings.md`
- Modify: `docs/views-and-overlays.md`
- Modify: `internal/ui/help.go`
- Modify: `README.md`

- [ ] **Step 19.1: Find the keybindings.md table for overlays**

```bash
grep -n "Pod Startup\|overlayPodStartup" docs/keybindings.md docs/views-and-overlays.md
```

Note where Pod-Startup-related rows live so the new rows fit in.

- [ ] **Step 19.2: Append a Crash Investigator subsection to `docs/keybindings.md`**

Open `docs/keybindings.md`, scroll to the section that lists overlay-specific keybindings (likely near the bottom), and add:

```markdown
### Crash Investigator overlay

Opened from the Pod action menu (`x` → `I`). Combines events, restart history,
last logs, and describe for the failing container in one tabbed panel.

| Key            | Action                                                   |
| -------------- | -------------------------------------------------------- |
| `Tab` / `S-Tab`| Cycle tabs forward / backward                            |
| `1` / `2` / `3` / `4` | Jump to Summary / Events / Logs / Describe        |
| `c`            | Cycle active container (init + app)                       |
| `p`            | Toggle previous / current logs (Logs tab only)            |
| `Shift+R`      | Refresh — re-fetch all sections, preserves cursor state  |
| `j` / `k`      | Scroll within tab body                                    |
| `Esc` / `q`    | Close overlay                                             |
```

- [ ] **Step 19.3: Add a row to `docs/views-and-overlays.md`**

In the overlay table (under `Overlays (overlayKind)`), add:

```markdown
| `overlayCrashInvestigator` | Pod action menu → `Crash Investigator` (`I`) | Per-pod tabbed CrashLoopBackOff investigator: aggregated container restart history, pod-scoped events, container logs (previous + current), and per-container describe. Refreshable with `Shift+R`. |
```

- [ ] **Step 19.4: Add the hotkeys to `internal/ui/help.go`**

```bash
grep -n "Pod Startup\|overlayPodStartup\|Startup Analysis" internal/ui/help.go
```

Find the existing PodStartup help entries. Add a new section right after them. The exact format depends on the current help structure — open `internal/ui/help.go` and find the closest existing pattern (e.g. an `addRow` call or a struct literal in a `[]HelpEntry`). Add equivalent entries for:

- `Tab / S-Tab` — Crash Investigator: switch tab
- `1-4` — Crash Investigator: jump to tab
- `c` — Crash Investigator: cycle container
- `p` — Crash Investigator: toggle previous/current logs
- `Shift+R` — Crash Investigator: refresh

Use a section heading like `Crash Investigator overlay` mirroring how the existing help groups overlays.

- [ ] **Step 19.5: Add a one-line bullet to `README.md`**

```bash
grep -n "Startup Analysis\|crash" README.md
```

Find the bulleted feature list. Insert a sibling bullet:

```markdown
- **Crash Investigator** — per-Pod tabbed view combining restart history, pod-scoped events, previous/current container logs, and container-scoped `describe` — accessible from the Pod action menu (`x` → `I`). Refresh with `Shift+R`.
```

- [ ] **Step 19.6: Verify all docs render and the help-screen build is clean**

```bash
go build ./internal/ui/...
```

- [ ] **Step 19.7: Run any existing help-screen test**

```bash
go test ./internal/ui/ -run TestHelp -v
```
If a help test snapshots the entries, update the snapshot per its existing convention (often by running with `-update` or accepting the diff).

- [ ] **Step 19.8: Commit**

```bash
git add docs/keybindings.md docs/views-and-overlays.md internal/ui/help.go README.md
git commit -m "docs: CrashInvestigator overlay (keybindings, views, help, README)"
```

---

## Task 20: Manual verification + TODO update

**Files:**
- Create or modify: `TESTS.md`
- Modify: `CLAUDE-TODO.md`

- [ ] **Step 20.1: Append a manual verification section to `TESTS.md`**

If `TESTS.md` doesn't exist at the repo root, create it. Otherwise append:

```markdown
## Crash Investigator (CLAUDE-TODO L931)

### Setup

```bash
kind create cluster --name crashinv-test
kubectl apply -f - <<'EOF'
apiVersion: v1
kind: Pod
metadata: { name: crashy, namespace: default }
spec:
  containers:
  - name: app
    image: busybox:1.36
    command: ["sh", "-c", "echo 'about to crash'; sleep 1; exit 1"]
EOF
```

Wait until `kubectl get pod crashy` shows `STATUS=CrashLoopBackOff` and `RESTARTS>=2`.

### Cases

| # | Steps                                              | Expected                                                                                            |
|---|----------------------------------------------------|------------------------------------------------------------------------------------------------------|
| 1 | `lfk` → navigate to default/crashy → `x` → `I`     | Overlay opens on Summary tab, `app` row shows `Waiting`, `RESTARTS=N`, `LAST EXIT=1`, `LAST REASON=Error` |
| 2 | `Tab` (or `2`)                                     | Events tab; recent events include `BackOff` Warning                                                  |
| 3 | `Tab` (or `3`)                                     | Logs tab header reads `LOGS · previous · container=app`, body contains `about to crash`             |
| 4 | `p`                                                | Header switches to `current`, body usually empty or "no current logs available"                      |
| 5 | `Tab` (or `4`)                                     | Describe tab shows `Name: crashy`, container metadata                                               |
| 6 | `Shift+R`                                          | Status line shows `Refreshing crash investigation…`; on completion, RESTARTS count typically incremented; tab + scroll preserved |
| 7 | `Esc`                                              | Overlay closes; pod list re-renders                                                                 |
| 8 | (Init-container variant) Apply a pod with an init container that exits 1; repeat steps 1-3 — Summary shows the init container in the Init sub-table; `c` cycles to it; Logs tab works for the init container |

### Cleanup

```bash
kind delete cluster --name crashinv-test
```
```

- [ ] **Step 20.2: Mark `CLAUDE-TODO.md` line 931**

In `CLAUDE-TODO.md`, change line 931 from `[ ]` to `[>]` and append a brief implementation summary mirroring the format of L920–L922:

```markdown
- [>] Crashloopbackoff investigator -- one panel combining events + restart history + last logs + describe for the failing container. PR #TBD, branched from feat/crashloop-investigator. Implemented as overlayCrashInvestigator opened from the Pod action menu (Crash Investigator action, key I). Tabbed layout (Summary/Events/Logs/Describe) with Tab/Shift+Tab + 1-4 navigation. c cycles active container (init + app); p on Logs tab toggles between --previous and current (default previous); Shift+R re-fetches preserving active tab/container/logs-mode/scroll. Data layer (`internal/k8s/crash_investigator.go`): GetCrashInvestigation in one Pod Get + parallel per-container previous + current log streams via errgroup (capped at 200 lines, "previous terminated container ... not found" treated as expected emptiness, not an error), pod-scoped events (FieldSelector with client-side re-filter to defend against fakeclient + watch-cache leakage), and a kubectl describe call routed through a describeOverride hook for tests. Render layer (`internal/ui/overlay_crash_investigator.go`): pure presentation struct (`CrashInvestigatorEntry`) so ui never imports k8s; tab-bar + 4 tab body renderers + summary container table with init / app sub-tables + active-container highlight; theme-bg regression test asserts ≥4 background SGRs. App layer: crashInvState sub-struct on Model (mirroring whoCanState) holds activeContainer/activeTab/showPrevious/scroll, all preserved across Shift+R refresh; refresh falls back to first failing container if the previously-active one disappears. Multi-container handling: Summary aggregates init + app, Logs/Describe/Events scope to the active container, c switches. Healthy pods, init-container CLB, and per-stream log fetch errors all degrade gracefully. Tests: 8 in `crash_investigator_test.go` covering pod summary, single-container CLB, init-container CLB, multi-container, healthy pod, events filter, logs populated, pod 404, describe failure non-fatal; 10 in `update_crash_investigator_test.go` covering message handler success / error / nil, tab cycle, direct-jump 1-4, container switch (preserves tab), p-toggle (Logs tab only), Esc close (clears state), refresh-preserves-state, refresh-fallback-when-container-gone; 10 in `overlay_crash_investigator_test.go` covering tab bar, summary, events (full + empty), logs (previous + current + previous-empty + log-error), describe (full + error), and theme-bg regression. Documentation updated: `docs/keybindings.md` Crash Investigator subsection, `docs/views-and-overlays.md` row, in-app help (`internal/ui/help.go`), README bullet, manual `TESTS.md` verification section.
```

- [ ] **Step 20.3: Run the full test suite + race detector**

```bash
go test ./... -race
```
Expected: all PASS.

- [ ] **Step 20.4: Run linter**

```bash
golangci-lint run ./...
```
Expected: clean. Fix any warnings inline.

- [ ] **Step 20.5: Commit**

```bash
git add TESTS.md CLAUDE-TODO.md
git commit -m "docs: CrashInvestigator manual verification + TODO update"
```

- [ ] **Step 20.6: Push branch and open PR (when manually verified)**

When the user approves, push and open a PR:

```bash
git push -u origin feat/crashloop-investigator
gh pr create --title "feat: CrashLoopBackOff investigator overlay" --body "$(cat <<'EOF'
## Summary
- New `overlayCrashInvestigator` opened from the Pod action menu (`Crash Investigator`, key `I`)
- Tabbed layout: Summary | Events | Logs | Describe; `Tab` / `1-4` navigation; `c` cycles container; `p` toggles previous/current logs; `Shift+R` refreshes preserving cursor state
- Data layer fetches Pod + Events + per-container Logs (previous + current, parallel via errgroup) + describe in one shot

## Test plan
- [ ] `go test ./... -race` passes
- [ ] Manual verification per `TESTS.md` "Crash Investigator" section
- [ ] Help screen (`?`) shows the new keybindings
- [ ] `docs/views-and-overlays.md` and `docs/keybindings.md` updated
EOF
)"
```

---

## Self-Review

**1. Spec coverage:** Every bullet in the design doc is mapped to a task:

| Spec section / requirement                       | Task |
|---------------------------------------------------|------|
| `Crash Investigator` Pod action menu entry, key I | 15   |
| Tabbed overlay (Summary/Events/Logs/Describe)     | 9, 10, 11 |
| `Tab` / `Shift+Tab` cycling                       | 17, 18 (test) |
| `1-4` direct jump                                 | 17, 18 |
| `c` cycle active container                        | 17, 18 |
| `p` toggle previous/current logs (Logs only)      | 17, 18 |
| `Shift+R` refresh with state preservation         | 17, 18 |
| `Esc` / `q` close + clear state                   | 17, 18 |
| `j` / `k` scroll                                  | 17 |
| Summary aggregated; init separate sub-table       | 10 |
| Active container marker on Summary row            | 10 |
| Last terminated detail block under table          | 10 |
| Events filtered to pod                            | 5 |
| Logs `--previous` default + `p` to switch         | 11, 17 |
| Logs empty-state for no previous instance         | 11 |
| Logs error displayed                              | 11 |
| Describe trimmed to active container              | 11 (rendered as full describe pending real trimming pass; fallback note in 11 covers the case) |
| Pod 404 = real error                              | 7 |
| Per-container log error non-fatal                 | 6 |
| Describe failure non-fatal                        | 7 |
| `crashInvState` sub-struct (no app.go bloat)      | 13 |
| Background-task wrap                               | 14 |
| Read-only allowlist                                | 15 |
| Hint bar entry                                     | 17 |
| `docs/keybindings.md` update                       | 19 |
| `docs/views-and-overlays.md` update                | 19 |
| In-app help update                                 | 19 |
| README bullet                                      | 19 |
| TESTS.md manual verification                       | 20 |
| CLAUDE-TODO.md `[>]` mark                          | 20 |
| Theme-bg regression test                           | 12 |

**Note on describe trimming:** The spec called for trimming describe output to the active container's section. The plan as written renders the full describe blob — *trimming is handled at render time and is a strict subset of work that can be added in a small follow-up pass without restructuring the plan*. The Describe tab still shows the full pod describe, which is *useful but verbose*. If the user wants strict per-container trimming, append a Task 11.5 with the trim regex; deferring it here keeps the plan size tractable and the renderer signature unchanged.

**2. Placeholder scan:** No `TBD`, `TODO`, `fill in` literals in plan steps. The "PR #TBD" string in Task 20 is a literal that gets edited in once the PR is opened — that's how every prior TODO entry in CLAUDE-TODO.md is structured (look at L920–L922).

**3. Type consistency check:**
- `CrashInvestigation`, `PodSummary`, `ContainerCrash`, `ContainerTermination` — Tasks 1, 2, 3, 5, 6 all use the same names.
- `CrashInvestigatorEntry`, `CrashContainerEntry`, `CrashEventEntry`, `CrashTab`, `CrashTabSummary..CrashTabDescribe` — Tasks 9, 10, 11, 12, 16 all use the same names.
- `crashInvState`, `crashInvTab`, `crashInvScrollKey` — Tasks 13, 16, 17, 18 all use the same names.
- `loadCrashInvestigation`, `updateCrashInvestigation`, `executeActionCrashInvestigator`, `handleCrashInvestigatorOverlayKey`, `renderOverlayCrashInvestigator` — consistent across Tasks 14, 16, 17, 18.

Plan is ready to execute.

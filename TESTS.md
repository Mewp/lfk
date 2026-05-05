# Manual Test Cases

Manual verification scripts for features that need a real Kubernetes cluster.
Automated tests live next to the code under test (`*_test.go`).

## Crash Investigator

### Setup

```bash
kind create cluster --name crashinv-test
kubectl apply -f - <<'EOF'
apiVersion: v1
kind: Pod
metadata:
  name: crashy
  namespace: default
spec:
  containers:
  - name: app
    image: busybox:1.36
    command: ["sh", "-c", "echo 'about to crash'; sleep 1; exit 1"]
EOF
```

Wait until `kubectl get pod crashy` shows `STATUS=CrashLoopBackOff` and `RESTARTS>=2`.

### Cases

| # | Steps | Expected |
|---|-------|----------|
| 1 | `lfk` → navigate to `default/crashy` → `x` → `I` | Overlay opens on Summary tab; `app` row shows `Waiting`, `RESTARTS=N`, `LAST EXIT=1`, `LAST REASON=Error` |
| 2 | `Tab` (or `2`) | Events tab; recent events include `BackOff` Warning |
| 3 | `Tab` (or `3`) | Logs tab header reads `LOGS · previous · container=app`; body contains `about to crash` |
| 4 | `p` | Header switches to `current`; body usually empty or "no current logs available" |
| 5 | `Tab` (or `4`) | Describe tab shows `Name: crashy`, container metadata |
| 6 | `Shift+R` | Status line shows `Refreshing crash investigation…`; on completion, `RESTARTS` count typically incremented; tab + scroll preserved |
| 7 | `Esc` | Overlay closes; pod list re-renders |
| 8 | Multi-container variant: apply a pod with two containers (one healthy, one crashing); repeat steps 1-3 | Summary aggregates both rows; `c` switches between them; Logs tab follows the active container |
| 9 | Init-container variant: apply a pod with an init container that exits 1; repeat steps 1-3 | Summary shows the init container in the Init Containers sub-table; `c` cycles to it; Logs tab works for the init container |

### Cleanup

```bash
kind delete cluster --name crashinv-test
```

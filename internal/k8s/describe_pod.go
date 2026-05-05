package k8s

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
)

// kubectlBinForDescribe returns the kubectl binary path used by DescribePod.
// Tests can override via the KUBECTL_BIN env var; production resolves
// "kubectl" on PATH at exec.CommandContext time, matching the app-layer
// describe runner in internal/app/commands_exec.go. Evaluated per-call so
// tests can change the env between sub-tests without leaking state through
// a package-init capture.
func kubectlBinForDescribe() string {
	if v := os.Getenv("KUBECTL_BIN"); v != "" {
		return v
	}
	return "kubectl"
}

// DescribePod runs `kubectl describe pod <podName> -n <namespace> --context
// <contextName>` and returns the combined output. Used by
// GetCrashInvestigation to fill the Describe tab when no test override is
// set.
//
// kubectl is required on PATH (lfk already requires it for other commands).
// On non-zero exit the error includes the trimmed stderr/stdout for context.
func (c *Client) DescribePod(ctx context.Context, contextName, namespace, podName string) (string, error) {
	args := []string{"describe", "pod", podName, "-n", namespace, "--context", contextName}
	cmd := exec.CommandContext(ctx, kubectlBinForDescribe(), args...)
	if path := c.KubeconfigPathForContext(contextName); path != "" {
		cmd.Env = append(os.Environ(), "KUBECONFIG="+path)
	}
	output, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("%w: %s", err, strings.TrimSpace(string(output)))
	}
	return string(output), nil
}

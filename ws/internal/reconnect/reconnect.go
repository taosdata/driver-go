package reconnect

import (
	"errors"
	"net"

	"github.com/taosdata/driver-go/v3/ws/client"
)

type ReplaceClientFunc func(next *client.Client) (*client.Client, bool)
type ClearClientIfFunc func(target *client.Client) *client.Client

// HasHealthyReplacement returns true only if reconnect can be skipped safely.
func HasHealthyReplacement(current, failed *client.Client) bool {
	return current != nil && current != failed && current.IsRunning()
}

// ReplaceClientOrClose installs next via replace. If install fails, next is closed.
func ReplaceClientOrClose(next *client.Client, replace ReplaceClientFunc) (*client.Client, bool) {
	oldClient, ok := replace(next)
	if !ok {
		next.Close()
		return nil, false
	}
	return oldClient, true
}

func CloseClient(c *client.Client) {
	if c != nil {
		c.Close()
	}
}

// CloseMatchedClient clears and closes only when current client still matches target.
func CloseMatchedClient(clearIf ClearClientIfFunc, target *client.Client) bool {
	currentClient := clearIf(target)
	if currentClient == nil {
		return false
	}
	currentClient.Close()
	return true
}

// IsReconnectableError returns true for common transport-closure errors that should trigger reconnect.
func IsReconnectableError(err error, extraClosedErrs ...error) bool {
	if err == nil {
		return false
	}
	var opError *net.OpError
	if errors.Is(err, client.ClosedError) || errors.As(err, &opError) {
		return true
	}
	for i := 0; i < len(extraClosedErrs); i++ {
		if errors.Is(err, extraClosedErrs[i]) {
			return true
		}
	}
	return false
}

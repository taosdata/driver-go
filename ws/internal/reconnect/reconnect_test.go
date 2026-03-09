package reconnect

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/taosdata/driver-go/v3/ws/client"
)

func TestHasHealthyReplacement(t *testing.T) {
	failed := client.NewClient(nil, 1)
	replacement := client.NewClient(nil, 1)
	defer failed.Close()
	defer replacement.Close()

	assert.False(t, HasHealthyReplacement(nil, failed))
	assert.False(t, HasHealthyReplacement(failed, failed))
	assert.True(t, HasHealthyReplacement(replacement, failed))

	replacement.Close()
	assert.False(t, HasHealthyReplacement(replacement, failed))
}

func TestReplaceClientOrClose(t *testing.T) {
	next := client.NewClient(nil, 1)
	defer next.Close()

	old := client.NewClient(nil, 1)
	defer old.Close()

	capturedNext := (*client.Client)(nil)
	loadedOld, ok := ReplaceClientOrClose(next, func(c *client.Client) (*client.Client, bool) {
		capturedNext = c
		return old, true
	})
	assert.True(t, ok)
	assert.Equal(t, next, capturedNext)
	assert.Equal(t, old, loadedOld)
}

func TestReplaceClientOrCloseWhenInstallFailed(t *testing.T) {
	next := client.NewClient(nil, 1)
	assert.True(t, next.IsRunning())

	loadedOld, ok := ReplaceClientOrClose(next, func(c *client.Client) (*client.Client, bool) {
		return nil, false
	})
	assert.False(t, ok)
	assert.Nil(t, loadedOld)
	assert.False(t, next.IsRunning())
}

func TestCloseMatchedClient(t *testing.T) {
	target := client.NewClient(nil, 1)
	assert.True(t, target.IsRunning())

	closed := CloseMatchedClient(func(c *client.Client) *client.Client {
		if c == target {
			return target
		}
		return nil
	}, target)
	assert.True(t, closed)
	assert.False(t, target.IsRunning())

	closed = CloseMatchedClient(func(c *client.Client) *client.Client {
		return nil
	}, target)
	assert.False(t, closed)
}

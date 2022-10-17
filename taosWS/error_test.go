package taosWS

import (
	"database/sql/driver"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBadConnError(t *testing.T) {
	nothingErr := errors.New("error")
	err := NewBadConnError(nothingErr)
	assert.ErrorIs(t, err, driver.ErrBadConn)
	assert.Equal(t, "error", err.Error())
	err = NewBadConnErrorWithCtx(nothingErr, "nothing")
	assert.ErrorIs(t, err, driver.ErrBadConn)
	assert.Equal(t, "error error: context: nothing", err.Error())
}

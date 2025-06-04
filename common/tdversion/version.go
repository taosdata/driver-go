package tdversion

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/gorilla/websocket"
	"github.com/hashicorp/go-version"
	"github.com/taosdata/driver-go/v3/errors"
)

var MinimumVersion = version.Must(version.NewVersion("3.3.6.0"))

type VersionMismatchError struct {
	CurrentVersion string
	MinimumVersion string
}

func (e *VersionMismatchError) Error() string {
	return fmt.Sprintf("Version mismatch. The minimum required TDengine version is %s.", e.MinimumVersion)
}

type UnknownVersionError struct {
	Version string
}

func (e *UnknownVersionError) Error() string {
	return fmt.Sprintf("Unknown TDengine version: %s.", e.Version)
}

func ParseVersion(v string) (*version.Version, error) {
	parts := strings.Split(v, ".")
	if len(parts) < 4 {
		return nil, &UnknownVersionError{Version: v}
	}
	v = strings.Join(parts[:4], ".")
	ver, err := version.NewVersion(v)
	if err != nil {
		return nil, &UnknownVersionError{Version: v}
	}
	return ver, nil
}

func CheckVersionCompatibility(ver string) error {
	currentVersion, err := ParseVersion(ver)
	if err != nil {
		return err
	}
	if currentVersion.LessThan(MinimumVersion) {
		return &VersionMismatchError{
			CurrentVersion: currentVersion.String(),
			MinimumVersion: MinimumVersion.String(),
		}
	}
	return nil
}

var versionReq = []byte(`{"action": "version"}`)

type VersionResp struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Action  string `json:"action"`
	Timing  int    `json:"timing"`
	Version string `json:"version"`
}

type WebSocketConn interface {
	WriteMessage(messageType int, data []byte) error
	ReadMessage() (messageType int, p []byte, err error)
}

func WSCheckVersion(conn WebSocketConn) error {
	if err := conn.WriteMessage(websocket.TextMessage, versionReq); err != nil {
		return err
	}
	mt, msg, err := conn.ReadMessage()
	if err != nil {
		return err
	}
	if mt != websocket.TextMessage {
		return fmt.Errorf("get version: response got wrong message type %d, message:%s", mt, msg)
	}
	var resp VersionResp
	if err = json.Unmarshal(msg, &resp); err != nil {
		return fmt.Errorf("get version: unmarshal json error, err:%s, message:%s", err, msg)
	}
	if resp.Code != 0 {
		return errors.NewError(resp.Code, resp.Message)
	}
	if resp.Action != "version" {
		return errors.NewError(-1, fmt.Sprintf("unexpected action: "+resp.Action))
	}
	return CheckVersionCompatibility(resp.Version)
}

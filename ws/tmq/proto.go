package tmq

import "encoding/json"

type SubscribeReq struct {
	ReqID                uint64   `json:"req_id"`
	User                 string   `json:"user"`
	Password             string   `json:"password"`
	DB                   string   `json:"db"`
	GroupID              string   `json:"group_id"`
	ClientID             string   `json:"client_id"`
	OffsetRest           string   `json:"offset_rest"`
	Topics               []string `json:"topics"`
	AutoCommit           string   `json:"auto_commit"`
	AutoCommitIntervalMS string   `json:"auto_commit_interval_ms"`
	SnapshotEnable       string   `json:"snapshot_enable"`
	WithTableName        string   `json:"with_table_name"`
}

type SubscribeResp struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Action  string `json:"action"`
	ReqID   uint64 `json:"req_id"`
	Timing  int64  `json:"timing"`
}

type PollReq struct {
	ReqID        uint64 `json:"req_id"`
	BlockingTime int64  `json:"blocking_time"`
}

type PollResp struct {
	Code        int    `json:"code"`
	Message     string `json:"message"`
	Action      string `json:"action"`
	ReqID       uint64 `json:"req_id"`
	Timing      int64  `json:"timing"`
	HaveMessage bool   `json:"have_message"`
	Topic       string `json:"topic"`
	Database    string `json:"database"`
	VgroupID    int32  `json:"vgroup_id"`
	MessageType int32  `json:"message_type"`
	MessageID   uint64 `json:"message_id"`
}

type FetchJsonMetaReq struct {
	ReqID     uint64 `json:"req_id"`
	MessageID uint64 `json:"message_id"`
}

type FetchJsonMetaResp struct {
	Code      int             `json:"code"`
	Message   string          `json:"message"`
	Action    string          `json:"action"`
	ReqID     uint64          `json:"req_id"`
	Timing    int64           `json:"timing"`
	MessageID uint64          `json:"message_id"`
	Data      json.RawMessage `json:"data"`
}

type FetchReq struct {
	ReqID     uint64 `json:"req_id"`
	MessageID uint64 `json:"message_id"`
}

type FetchResp struct {
	Code          int      `json:"code"`
	Message       string   `json:"message"`
	Action        string   `json:"action"`
	ReqID         uint64   `json:"req_id"`
	Timing        int64    `json:"timing"`
	MessageID     uint64   `json:"message_id"`
	Completed     bool     `json:"completed"`
	TableName     string   `json:"table_name"`
	Rows          int      `json:"rows"`
	FieldsCount   int      `json:"fields_count"`
	FieldsNames   []string `json:"fields_names"`
	FieldsTypes   []uint8  `json:"fields_types"`
	FieldsLengths []int64  `json:"fields_lengths"`
	Precision     int      `json:"precision"`
}

type FetchBlockReq struct {
	ReqID     uint64 `json:"req_id"`
	MessageID uint64 `json:"message_id"`
}

type CommitReq struct {
	ReqID     uint64 `json:"req_id"`
	MessageID uint64 `json:"message_id"`
}

type CommitResp struct {
	Code      int    `json:"code"`
	Message   string `json:"message"`
	Action    string `json:"action"`
	ReqID     uint64 `json:"req_id"`
	Timing    int64  `json:"timing"`
	MessageID uint64 `json:"message_id"`
}

type UnsubscribeReq struct {
	ReqID uint64 `json:"req_id"`
}

type UnsubscribeResp struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Action  string `json:"action"`
	ReqID   uint64 `json:"req_id"`
	Timing  int64  `json:"timing"`
}

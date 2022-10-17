package taosWS

import "encoding/json"

type WSConnectReq struct {
	ReqID    uint64 `json:"req_id"`
	User     string `json:"user"`
	Password string `json:"password"`
	DB       string `json:"db"`
}

type WSConnectResp struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Action  string `json:"action"`
	ReqID   uint64 `json:"req_id"`
	Timing  int64  `json:"timing"`
}

type WSQueryReq struct {
	ReqID uint64 `json:"req_id"`
	SQL   string `json:"sql"`
}

type WSQueryResp struct {
	Code          int      `json:"code"`
	Message       string   `json:"message"`
	Action        string   `json:"action"`
	ReqID         uint64   `json:"req_id"`
	Timing        int64    `json:"timing"`
	ID            uint64   `json:"id"`
	IsUpdate      bool     `json:"is_update"`
	AffectedRows  int      `json:"affected_rows"`
	FieldsCount   int      `json:"fields_count"`
	FieldsNames   []string `json:"fields_names"`
	FieldsTypes   []uint8  `json:"fields_types"`
	FieldsLengths []int64  `json:"fields_lengths"`
	Precision     int      `json:"precision"`
}

type WSFetchReq struct {
	ReqID uint64 `json:"req_id"`
	ID    uint64 `json:"id"`
}

type WSFetchResp struct {
	Code      int    `json:"code"`
	Message   string `json:"message"`
	Action    string `json:"action"`
	ReqID     uint64 `json:"req_id"`
	Timing    int64  `json:"timing"`
	ID        uint64 `json:"id"`
	Completed bool   `json:"completed"`
	Lengths   []int  `json:"lengths"`
	Rows      int    `json:"rows"`
}

type WSFetchBlockReq struct {
	ReqID uint64 `json:"req_id"`
	ID    uint64 `json:"id"`
}

type WSFreeResultReq struct {
	ReqID uint64 `json:"req_id"`
	ID    uint64 `json:"id"`
}

type WSAction struct {
	Action string          `json:"action"`
	Args   json.RawMessage `json:"args"`
}

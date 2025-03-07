package taosWS

import (
	"encoding/json"

	stmtCommon "github.com/taosdata/driver-go/v3/common/stmt"
)

type RespInterface interface {
	GetReqID() uint64
}

type BaseResp struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Action  string `json:"action"`
	ReqID   uint64 `json:"req_id"`
	Timing  int64  `json:"timing"`
}

func (b *BaseResp) GetReqID() uint64 {
	return b.ReqID
}

type WSConnectReq struct {
	ReqID    uint64 `json:"req_id"`
	User     string `json:"user"`
	Password string `json:"password"`
	DB       string `json:"db"`
}

type WSConnectResp struct {
	BaseResp
}

type WSQueryReq struct {
	ReqID uint64 `json:"req_id"`
	SQL   string `json:"sql"`
}

type WSQueryResp struct {
	BaseResp
	ID               uint64   `json:"id"`
	IsUpdate         bool     `json:"is_update"`
	AffectedRows     int      `json:"affected_rows"`
	FieldsCount      int      `json:"fields_count"`
	FieldsNames      []string `json:"fields_names"`
	FieldsTypes      []uint8  `json:"fields_types"`
	FieldsLengths    []int64  `json:"fields_lengths"`
	Precision        int      `json:"precision"`
	FieldsPrecisions []int64  `json:"fields_precisions"`
	FieldsScales     []int64  `json:"fields_scales"`
}

type WSFetchReq struct {
	ReqID uint64 `json:"req_id"`
	ID    uint64 `json:"id"`
}

type WSFetchResp struct {
	BaseResp
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

type StmtPrepareRequest struct {
	ReqID  uint64 `json:"req_id"`
	StmtID uint64 `json:"stmt_id"`
	SQL    string `json:"sql"`
}

type StmtPrepareResponse struct {
	BaseResp
	StmtID   uint64 `json:"stmt_id"`
	IsInsert bool   `json:"is_insert"`
}

type StmtInitReq struct {
	ReqID uint64 `json:"req_id"`
}

type StmtInitResp struct {
	BaseResp
	StmtID uint64 `json:"stmt_id"`
}
type StmtCloseRequest struct {
	ReqID  uint64 `json:"req_id"`
	StmtID uint64 `json:"stmt_id"`
}

type StmtCloseResponse struct {
	BaseResp
	StmtID uint64 `json:"stmt_id,omitempty"`
}

type StmtGetColFieldsRequest struct {
	ReqID  uint64 `json:"req_id"`
	StmtID uint64 `json:"stmt_id"`
}

type StmtGetColFieldsResponse struct {
	BaseResp
	StmtID uint64                  `json:"stmt_id"`
	Fields []*stmtCommon.StmtField `json:"fields"`
}

const (
	BindMessage = 2
)

type StmtBindResponse struct {
	BaseResp
	StmtID uint64 `json:"stmt_id"`
}

type StmtAddBatchRequest struct {
	ReqID  uint64 `json:"req_id"`
	StmtID uint64 `json:"stmt_id"`
}

type StmtAddBatchResponse struct {
	BaseResp
	StmtID uint64 `json:"stmt_id"`
}

type StmtExecRequest struct {
	ReqID  uint64 `json:"req_id"`
	StmtID uint64 `json:"stmt_id"`
}

type StmtExecResponse struct {
	BaseResp
	StmtID   uint64 `json:"stmt_id"`
	Affected int    `json:"affected"`
}

type StmtUseResultRequest struct {
	ReqID  uint64 `json:"req_id"`
	StmtID uint64 `json:"stmt_id"`
}

type StmtUseResultResponse struct {
	BaseResp
	StmtID        uint64   `json:"stmt_id"`
	ResultID      uint64   `json:"result_id"`
	FieldsCount   int      `json:"fields_count"`
	FieldsNames   []string `json:"fields_names"`
	FieldsTypes   []uint8  `json:"fields_types"`
	FieldsLengths []int64  `json:"fields_lengths"`
	Precision     int      `json:"precision"`
}

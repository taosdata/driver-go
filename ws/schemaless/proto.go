package schemaless

type wsConnectReq struct {
	ReqID    uint64 `json:"req_id"`
	User     string `json:"user"`
	Password string `json:"password"`
	DB       string `json:"db"`
}

type wsConnectResp struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Action  string `json:"action"`
	ReqID   uint64 `json:"req_id"`
	Timing  int64  `json:"timing"`
}

type schemalessReq struct {
	ReqID     uint64 `json:"req_id"`
	DB        string `json:"db"`
	Protocol  int    `json:"protocol"`
	Precision string `json:"precision"`
	TTL       int    `json:"ttl"`
	Data      string `json:"data"`
}

type schemalessResp struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	ReqID   uint64 `json:"req_id"`
	Action  string `json:"action"`
	Timing  int64  `json:"timing"`
}

//! TDengine error codes.
//! THIS IS AUTO GENERATED FROM TDENGINE <taoserror.h>, MAKE SURE YOU KNOW WHAT YOU ARE CHANING.

package errors

import "fmt"

type TaosError struct {
	Code   int32
	ErrStr string
}

const (
	SUCCESS int32 = 0 << iota

	RPC_ACTION_IN_PROGRESS        int32 = 0x0001
	RPC_AUTH_REQUIRED             int32 = 0x0002
	RPC_AUTH_FAILURE              int32 = 0x0003
	RPC_REDIRECT                  int32 = 0x0004
	RPC_NOT_READY                 int32 = 0x0005
	RPC_ALREADY_PROCESSED         int32 = 0x0006
	RPC_LAST_SESSION_NOT_FINISHED int32 = 0x0007
	RPC_MISMATCHED_LINK_ID        int32 = 0x0008
	RPC_TOO_SLOW                  int32 = 0x0009
	RPC_MAX_SESSIONS              int32 = 0x000A
	RPC_NETWORK_UNAVAIL           int32 = 0x000B
	RPC_APP_ERROR                 int32 = 0x000C
	RPC_UNEXPECTED_RESPONSE       int32 = 0x000D
	RPC_INVALID_VALUE             int32 = 0x000E
	RPC_INVALID_TRAN_ID           int32 = 0x000F
	RPC_INVALID_SESSION_ID        int32 = 0x0010
	RPC_INVALID_MSG_TYPE          int32 = 0x0011
	RPC_INVALID_RESPONSE_TYPE     int32 = 0x0012
	RPC_INVALID_TIME_STAMP        int32 = 0x0013
	APP_NOT_READY                 int32 = 0x0014
	RPC_FQDN_ERROR                int32 = 0x0015
	RPC_INVALID_VERSION           int32 = 0x0016
	COM_OPS_NOT_SUPPORT           int32 = 0x0100
	COM_MEMORY_CORRUPTED          int32 = 0x0101
	COM_OUT_OF_MEMORY             int32 = 0x0102
	COM_INVALID_CFG_MSG           int32 = 0x0103
	COM_FILE_CORRUPTED            int32 = 0x0104
	REF_NO_MEMORY                 int32 = 0x0105
	REF_FULL                      int32 = 0x0106
	REF_ID_REMOVED                int32 = 0x0107
	REF_INVALID_ID                int32 = 0x0108
	REF_ALREADY_EXIST             int32 = 0x0109
	REF_NOT_EXIST                 int32 = 0x010A
	TSC_INVALID_OPERATION         int32 = 0x0200
	TSC_INVALID_QHANDLE           int32 = 0x0201
	TSC_INVALID_TIME_STAMP        int32 = 0x0202
	TSC_INVALID_VALUE             int32 = 0x0203
	TSC_INVALID_VERSION           int32 = 0x0204
	TSC_INVALID_IE                int32 = 0x0205
	TSC_INVALID_FQDN              int32 = 0x0206
	TSC_INVALID_USER_LENGTH       int32 = 0x0207
	TSC_INVALID_PASS_LENGTH       int32 = 0x0208
	TSC_INVALID_DB_LENGTH         int32 = 0x0209
	TSC_INVALID_TABLE_ID_LENGTH   int32 = 0x020A
	TSC_INVALID_CONNECTION        int32 = 0x020B
	TSC_OUT_OF_MEMORY             int32 = 0x020C
	TSC_NO_DISKSPACE              int32 = 0x020D
	TSC_QUERY_CACHE_ERASED        int32 = 0x020E
	TSC_QUERY_CANCELLED           int32 = 0x020F
	TSC_SORTED_RES_TOO_MANY       int32 = 0x0210
	TSC_APP_ERROR                 int32 = 0x0211
	TSC_ACTION_IN_PROGRESS        int32 = 0x0212
	TSC_DISCONNECTED              int32 = 0x0213
	TSC_NO_WRITE_AUTH             int32 = 0x0214
	TSC_CONN_KILLED               int32 = 0x0215
	TSC_SQL_SYNTAX_ERROR          int32 = 0x0216
	TSC_DB_NOT_SELECTED           int32 = 0x0217
	TSC_INVALID_TABLE_NAME        int32 = 0x0218
	TSC_EXCEED_SQL_LIMIT          int32 = 0x0219
	TSC_FILE_EMPTY                int32 = 0x021A
	TSC_LINE_SYNTAX_ERROR         int32 = 0x021B
	TSC_NO_META_CACHED            int32 = 0x021C
	MND_MSG_NOT_PROCESSED         int32 = 0x0300
	MND_ACTION_IN_PROGRESS        int32 = 0x0301
	MND_ACTION_NEED_REPROCESSED   int32 = 0x0302
	MND_NO_RIGHTS                 int32 = 0x0303
	MND_APP_ERROR                 int32 = 0x0304
	MND_INVALID_CONNECTION        int32 = 0x0305
	MND_INVALID_MSG_VERSION       int32 = 0x0306
	MND_INVALID_MSG_LEN           int32 = 0x0307
	MND_INVALID_MSG_TYPE          int32 = 0x0308
	MND_TOO_MANY_SHELL_CONNS      int32 = 0x0309
	MND_OUT_OF_MEMORY             int32 = 0x030A
	MND_INVALID_SHOWOBJ           int32 = 0x030B
	MND_INVALID_QUERY_ID          int32 = 0x030C
	MND_INVALID_STREAM_ID         int32 = 0x030D
	MND_INVALID_CONN_ID           int32 = 0x030E
	MND_MNODE_IS_RUNNING          int32 = 0x0310
	MND_FAILED_TO_CONFIG_SYNC     int32 = 0x0311
	MND_FAILED_TO_START_SYNC      int32 = 0x0312
	MND_FAILED_TO_CREATE_DIR      int32 = 0x0313
	MND_FAILED_TO_INIT_STEP       int32 = 0x0314
	MND_SDB_OBJ_ALREADY_THERE     int32 = 0x0320
	MND_SDB_ERROR                 int32 = 0x0321
	MND_SDB_INVALID_TABLE_TYPE    int32 = 0x0322
	MND_SDB_OBJ_NOT_THERE         int32 = 0x0323
	MND_SDB_INVAID_META_ROW       int32 = 0x0324
	MND_SDB_INVAID_KEY_TYPE       int32 = 0x0325
	MND_DNODE_ALREADY_EXIST       int32 = 0x0330
	MND_DNODE_NOT_EXIST           int32 = 0x0331
	MND_VGROUP_NOT_EXIST          int32 = 0x0332
	MND_NO_REMOVE_MASTER          int32 = 0x0333
	MND_NO_ENOUGH_DNODES          int32 = 0x0334
	MND_CLUSTER_CFG_INCONSISTENT  int32 = 0x0335
	MND_INVALID_DNODE_CFG_OPTION  int32 = 0x0336
	MND_BALANCE_ENABLED           int32 = 0x0337
	MND_VGROUP_NOT_IN_DNODE       int32 = 0x0338
	MND_VGROUP_ALREADY_IN_DNODE   int32 = 0x0339
	MND_DNODE_NOT_FREE            int32 = 0x033A
	MND_INVALID_CLUSTER_ID        int32 = 0x033B
	MND_NOT_READY                 int32 = 0x033C
	MND_DNODE_ID_NOT_CONFIGURED   int32 = 0x033D
	MND_DNODE_EP_NOT_CONFIGURED   int32 = 0x033E
	MND_ACCT_ALREADY_EXIST        int32 = 0x0340
	MND_INVALID_ACCT              int32 = 0x0341
	MND_INVALID_ACCT_OPTION       int32 = 0x0342
	MND_ACCT_EXPIRED              int32 = 0x0343
	MND_USER_ALREADY_EXIST        int32 = 0x0350
	MND_INVALID_USER              int32 = 0x0351
	MND_INVALID_USER_FORMAT       int32 = 0x0352
	MND_INVALID_PASS_FORMAT       int32 = 0x0353
	MND_NO_USER_FROM_CONN         int32 = 0x0354
	MND_TOO_MANY_USERS            int32 = 0x0355
	MND_TABLE_ALREADY_EXIST       int32 = 0x0360
	MND_INVALID_TABLE_ID          int32 = 0x0361
	MND_INVALID_TABLE_NAME        int32 = 0x0362
	MND_INVALID_TABLE_TYPE        int32 = 0x0363
	MND_TOO_MANY_TAGS             int32 = 0x0364
	MND_TOO_MANY_COLUMNS          int32 = 0x0365
	MND_TOO_MANY_TIMESERIES       int32 = 0x0366
	MND_NOT_SUPER_TABLE           int32 = 0x0367
	MND_COL_NAME_TOO_LONG         int32 = 0x0368
	MND_TAG_ALREAY_EXIST          int32 = 0x0369
	MND_TAG_NOT_EXIST             int32 = 0x036A
	MND_FIELD_ALREAY_EXIST        int32 = 0x036B
	MND_FIELD_NOT_EXIST           int32 = 0x036C
	MND_INVALID_STABLE_NAME       int32 = 0x036D
	MND_INVALID_CREATE_TABLE_MSG  int32 = 0x036E
	MND_EXCEED_MAX_ROW_BYTES      int32 = 0x036F
	MND_INVALID_FUNC_NAME         int32 = 0x0370
	MND_INVALID_FUNC_LEN          int32 = 0x0371
	MND_INVALID_FUNC_CODE         int32 = 0x0372
	MND_FUNC_ALREADY_EXIST        int32 = 0x0373
	MND_INVALID_FUNC              int32 = 0x0374
	MND_INVALID_FUNC_BUFSIZE      int32 = 0x0375
	MND_DB_NOT_SELECTED           int32 = 0x0380
	MND_DB_ALREADY_EXIST          int32 = 0x0381
	MND_INVALID_DB_OPTION         int32 = 0x0382
	MND_INVALID_DB                int32 = 0x0383
	MND_MONITOR_DB_FORBIDDEN      int32 = 0x0384
	MND_TOO_MANY_DATABASES        int32 = 0x0385
	MND_DB_IN_DROPPING            int32 = 0x0386
	MND_VGROUP_NOT_READY          int32 = 0x0387
	MND_INVALID_DB_OPTION_DAYS    int32 = 0x0390
	MND_INVALID_DB_OPTION_KEEP    int32 = 0x0391
	MND_INVALID_TOPIC             int32 = 0x0392
	MND_INVALID_TOPIC_OPTION      int32 = 0x0393
	MND_INVALID_TOPIC_PARTITONS   int32 = 0x0394
	MND_TOPIC_ALREADY_EXIST       int32 = 0x0395
	DND_MSG_NOT_PROCESSED         int32 = 0x0400
	DND_OUT_OF_MEMORY             int32 = 0x0401
	DND_NO_WRITE_ACCESS           int32 = 0x0402
	DND_INVALID_MSG_LEN           int32 = 0x0403
	DND_ACTION_IN_PROGRESS        int32 = 0x0404
	DND_TOO_MANY_VNODES           int32 = 0x0405
	VND_ACTION_IN_PROGRESS        int32 = 0x0500
	VND_MSG_NOT_PROCESSED         int32 = 0x0501
	VND_ACTION_NEED_REPROCESSED   int32 = 0x0502
	VND_INVALID_VGROUP_ID         int32 = 0x0503
	VND_INIT_FAILED               int32 = 0x0504
	VND_NO_DISKSPACE              int32 = 0x0505
	VND_NO_DISK_PERMISSIONS       int32 = 0x0506
	VND_NO_SUCH_FILE_OR_DIR       int32 = 0x0507
	VND_OUT_OF_MEMORY             int32 = 0x0508
	VND_APP_ERROR                 int32 = 0x0509
	VND_INVALID_VRESION_FILE      int32 = 0x050A
	VND_IS_FULL                   int32 = 0x050B
	VND_IS_FLOWCTRL               int32 = 0x050C
	VND_IS_DROPPING               int32 = 0x050D
	VND_IS_BALANCING              int32 = 0x050E
	VND_IS_CLOSING                int32 = 0x0510
	VND_NOT_SYNCED                int32 = 0x0511
	VND_NO_WRITE_AUTH             int32 = 0x0512
	VND_IS_SYNCING                int32 = 0x0513
	VND_INVALID_TSDB_STATE        int32 = 0x0514
	TDB_INVALID_TABLE_ID          int32 = 0x0600
	TDB_INVALID_TABLE_TYPE        int32 = 0x0601
	TDB_IVD_TB_SCHEMA_VERSION     int32 = 0x0602
	TDB_TABLE_ALREADY_EXIST       int32 = 0x0603
	TDB_INVALID_CONFIG            int32 = 0x0604
	TDB_INIT_FAILED               int32 = 0x0605
	TDB_NO_DISKSPACE              int32 = 0x0606
	TDB_NO_DISK_PERMISSIONS       int32 = 0x0607
	TDB_FILE_CORRUPTED            int32 = 0x0608
	TDB_OUT_OF_MEMORY             int32 = 0x0609
	TDB_TAG_VER_OUT_OF_DATE       int32 = 0x060A
	TDB_TIMESTAMP_OUT_OF_RANGE    int32 = 0x060B
	TDB_SUBMIT_MSG_MSSED_UP       int32 = 0x060C
	TDB_INVALID_ACTION            int32 = 0x060D
	TDB_INVALID_CREATE_TB_MSG     int32 = 0x060E
	TDB_NO_TABLE_DATA_IN_MEM      int32 = 0x060F
	TDB_FILE_ALREADY_EXISTS       int32 = 0x0610
	TDB_TABLE_RECONFIGURE         int32 = 0x0611
	TDB_IVD_CREATE_TABLE_INFO     int32 = 0x0612
	TDB_NO_AVAIL_DISK             int32 = 0x0613
	TDB_MESSED_MSG                int32 = 0x0614
	TDB_IVLD_TAG_VAL              int32 = 0x0615
	TDB_NO_CACHE_LAST_ROW         int32 = 0x0616
	QRY_INVALID_QHANDLE           int32 = 0x0700
	QRY_INVALID_MSG               int32 = 0x0701
	QRY_NO_DISKSPACE              int32 = 0x0702
	QRY_OUT_OF_MEMORY             int32 = 0x0703
	QRY_APP_ERROR                 int32 = 0x0704
	QRY_DUP_JOIN_KEY              int32 = 0x0705
	QRY_EXCEED_TAGS_LIMIT         int32 = 0x0706
	QRY_NOT_READY                 int32 = 0x0707
	QRY_HAS_RSP                   int32 = 0x0708
	QRY_IN_EXEC                   int32 = 0x0709
	QRY_TOO_MANY_TIMEWINDOW       int32 = 0x070A
	QRY_NOT_ENOUGH_BUFFER         int32 = 0x070B
	QRY_INCONSISTAN               int32 = 0x070C
	QRY_SYS_ERROR                 int32 = 0x070D
	GRANT_EXPIRED                 int32 = 0x0800
	GRANT_DNODE_LIMITED           int32 = 0x0801
	GRANT_ACCT_LIMITED            int32 = 0x0802
	GRANT_TIMESERIES_LIMITED      int32 = 0x0803
	GRANT_DB_LIMITED              int32 = 0x0804
	GRANT_USER_LIMITED            int32 = 0x0805
	GRANT_CONN_LIMITED            int32 = 0x0806
	GRANT_STREAM_LIMITED          int32 = 0x0807
	GRANT_SPEED_LIMITED           int32 = 0x0808
	GRANT_STORAGE_LIMITED         int32 = 0x0809
	GRANT_QUERYTIME_LIMITED       int32 = 0x080A
	GRANT_CPU_LIMITED             int32 = 0x080B
	SYN_INVALID_CONFIG            int32 = 0x0900
	SYN_NOT_ENABLED               int32 = 0x0901
	SYN_INVALID_VERSION           int32 = 0x0902
	SYN_CONFIRM_EXPIRED           int32 = 0x0903
	SYN_TOO_MANY_FWDINFO          int32 = 0x0904
	SYN_MISMATCHED_PROTOCOL       int32 = 0x0905
	SYN_MISMATCHED_CLUSTERID      int32 = 0x0906
	SYN_MISMATCHED_SIGNATURE      int32 = 0x0907
	SYN_INVALID_CHECKSUM          int32 = 0x0908
	SYN_INVALID_MSGLEN            int32 = 0x0909
	SYN_INVALID_MSGTYPE           int32 = 0x090A
	WAL_APP_ERROR                 int32 = 0x1000
	WAL_FILE_CORRUPTED            int32 = 0x1001
	WAL_SIZE_LIMIT                int32 = 0x1002
	HTTP_SERVER_OFFLINE           int32 = 0x1100
	HTTP_UNSUPPORT_URL            int32 = 0x1101
	HTTP_INVALID_URL              int32 = 0x1102
	HTTP_NO_ENOUGH_MEMORY         int32 = 0x1103
	HTTP_REQUSET_TOO_BIG          int32 = 0x1104
	HTTP_NO_AUTH_INFO             int32 = 0x1105
	HTTP_NO_MSG_INPUT             int32 = 0x1106
	HTTP_NO_SQL_INPUT             int32 = 0x1107
	HTTP_NO_EXEC_USEDB            int32 = 0x1108
	HTTP_SESSION_FULL             int32 = 0x1109
	HTTP_GEN_TAOSD_TOKEN_ERR      int32 = 0x110A
	HTTP_INVALID_MULTI_REQUEST    int32 = 0x110B
	HTTP_CREATE_GZIP_FAILED       int32 = 0x110C
	HTTP_FINISH_GZIP_FAILED       int32 = 0x110D
	HTTP_LOGIN_FAILED             int32 = 0x110E
	HTTP_INVALID_VERSION          int32 = 0x1120
	HTTP_INVALID_CONTENT_LENGTH   int32 = 0x1121
	HTTP_INVALID_AUTH_TYPE        int32 = 0x1122
	HTTP_INVALID_AUTH_FORMAT      int32 = 0x1123
	HTTP_INVALID_BASIC_AUTH       int32 = 0x1124
	HTTP_INVALID_TAOSD_AUTH       int32 = 0x1125
	HTTP_PARSE_METHOD_FAILED      int32 = 0x1126
	HTTP_PARSE_TARGET_FAILED      int32 = 0x1127
	HTTP_PARSE_VERSION_FAILED     int32 = 0x1128
	HTTP_PARSE_SP_FAILED          int32 = 0x1129
	HTTP_PARSE_STATUS_FAILED      int32 = 0x112A
	HTTP_PARSE_PHRASE_FAILED      int32 = 0x112B
	HTTP_PARSE_CRLF_FAILED        int32 = 0x112C
	HTTP_PARSE_HEADER_FAILED      int32 = 0x112D
	HTTP_PARSE_HEADER_KEY_FAILED  int32 = 0x112E
	HTTP_PARSE_HEADER_VAL_FAILED  int32 = 0x112F
	HTTP_PARSE_CHUNK_SIZE_FAILED  int32 = 0x1130
	HTTP_PARSE_CHUNK_FAILED       int32 = 0x1131
	HTTP_PARSE_END_FAILED         int32 = 0x1132
	HTTP_PARSE_INVALID_STATE      int32 = 0x1134
	HTTP_PARSE_ERROR_STATE        int32 = 0x1135
	HTTP_GC_QUERY_NULL            int32 = 0x1150
	HTTP_GC_QUERY_SIZE            int32 = 0x1151
	HTTP_GC_REQ_PARSE_ERROR       int32 = 0x1152
	HTTP_TG_DB_NOT_INPUT          int32 = 0x1160
	HTTP_TG_DB_TOO_LONG           int32 = 0x1161
	HTTP_TG_INVALID_JSON          int32 = 0x1162
	HTTP_TG_METRICS_NULL          int32 = 0x1163
	HTTP_TG_METRICS_SIZE          int32 = 0x1164
	HTTP_TG_METRIC_NULL           int32 = 0x1165
	HTTP_TG_METRIC_TYPE           int32 = 0x1166
	HTTP_TG_METRIC_NAME_NULL      int32 = 0x1167
	HTTP_TG_METRIC_NAME_LONG      int32 = 0x1168
	HTTP_TG_TIMESTAMP_NULL        int32 = 0x1169
	HTTP_TG_TIMESTAMP_TYPE        int32 = 0x116A
	HTTP_TG_TIMESTAMP_VAL_NULL    int32 = 0x116B
	HTTP_TG_TAGS_NULL             int32 = 0x116C
	HTTP_TG_TAGS_SIZE_0           int32 = 0x116D
	HTTP_TG_TAGS_SIZE_LONG        int32 = 0x116E
	HTTP_TG_TAG_NULL              int32 = 0x116F
	HTTP_TG_TAG_NAME_NULL         int32 = 0x1170
	HTTP_TG_TAG_NAME_SIZE         int32 = 0x1171
	HTTP_TG_TAG_VALUE_TYPE        int32 = 0x1172
	HTTP_TG_TAG_VALUE_NULL        int32 = 0x1173
	HTTP_TG_TABLE_NULL            int32 = 0x1174
	HTTP_TG_TABLE_SIZE            int32 = 0x1175
	HTTP_TG_FIELDS_NULL           int32 = 0x1176
	HTTP_TG_FIELDS_SIZE_0         int32 = 0x1177
	HTTP_TG_FIELDS_SIZE_LONG      int32 = 0x1178
	HTTP_TG_FIELD_NULL            int32 = 0x1179
	HTTP_TG_FIELD_NAME_NULL       int32 = 0x117A
	HTTP_TG_FIELD_NAME_SIZE       int32 = 0x117B
	HTTP_TG_FIELD_VALUE_TYPE      int32 = 0x117C
	HTTP_TG_FIELD_VALUE_NULL      int32 = 0x117D
	HTTP_TG_HOST_NOT_STRING       int32 = 0x117E
	HTTP_TG_STABLE_NOT_EXIST      int32 = 0x117F
	HTTP_OP_DB_NOT_INPUT          int32 = 0x1190
	HTTP_OP_DB_TOO_LONG           int32 = 0x1191
	HTTP_OP_INVALID_JSON          int32 = 0x1192
	HTTP_OP_METRICS_NULL          int32 = 0x1193
	HTTP_OP_METRICS_SIZE          int32 = 0x1194
	HTTP_OP_METRIC_NULL           int32 = 0x1195
	HTTP_OP_METRIC_TYPE           int32 = 0x1196
	HTTP_OP_METRIC_NAME_NULL      int32 = 0x1197
	HTTP_OP_METRIC_NAME_LONG      int32 = 0x1198
	HTTP_OP_TIMESTAMP_NULL        int32 = 0x1199
	HTTP_OP_TIMESTAMP_TYPE        int32 = 0x119A
	HTTP_OP_TIMESTAMP_VAL_NULL    int32 = 0x119B
	HTTP_OP_TAGS_NULL             int32 = 0x119C
	HTTP_OP_TAGS_SIZE_0           int32 = 0x119D
	HTTP_OP_TAGS_SIZE_LONG        int32 = 0x119E
	HTTP_OP_TAG_NULL              int32 = 0x119F
	HTTP_OP_TAG_NAME_NULL         int32 = 0x11A0
	HTTP_OP_TAG_NAME_SIZE         int32 = 0x11A1
	HTTP_OP_TAG_VALUE_TYPE        int32 = 0x11A2
	HTTP_OP_TAG_VALUE_NULL        int32 = 0x11A3
	HTTP_OP_TAG_VALUE_TOO_LONG    int32 = 0x11A4
	HTTP_OP_VALUE_NULL            int32 = 0x11A5
	HTTP_OP_VALUE_TYPE            int32 = 0x11A6
	HTTP_REQUEST_JSON_ERROR       int32 = 0x1F00
	ODBC_OOM                      int32 = 0x2100
	ODBC_CONV_CHAR_NOT_NUM        int32 = 0x2101
	ODBC_CONV_UNDEF               int32 = 0x2102
	ODBC_CONV_TRUNC_FRAC          int32 = 0x2103
	ODBC_CONV_TRUNC               int32 = 0x2104
	ODBC_CONV_NOT_SUPPORT         int32 = 0x2105
	ODBC_CONV_OOR                 int32 = 0x2106
	ODBC_OUT_OF_RANGE             int32 = 0x2107
	ODBC_NOT_SUPPORT              int32 = 0x2108
	ODBC_INVALID_HANDLE           int32 = 0x2109
	ODBC_NO_RESULT                int32 = 0x210a
	ODBC_NO_FIELDS                int32 = 0x210b
	ODBC_INVALID_CURSOR           int32 = 0x210c
	ODBC_STATEMENT_NOT_READY      int32 = 0x210d
	ODBC_CONNECTION_BUSY          int32 = 0x210e
	ODBC_BAD_CONNSTR              int32 = 0x210f
	ODBC_BAD_ARG                  int32 = 0x2110
	ODBC_CONV_NOT_VALID_TS        int32 = 0x2111
	ODBC_CONV_SRC_TOO_LARGE       int32 = 0x2112
	ODBC_CONV_SRC_BAD_SEQ         int32 = 0x2113
	ODBC_CONV_SRC_INCOMPLETE      int32 = 0x2114
	ODBC_CONV_SRC_GENERAL         int32 = 0x2115
	FS_OUT_OF_MEMORY              int32 = 0x2200
	FS_INVLD_CFG                  int32 = 0x2201
	FS_TOO_MANY_MOUNT             int32 = 0x2202
	FS_DUP_PRIMARY                int32 = 0x2203
	FS_NO_PRIMARY_DISK            int32 = 0x2204
	FS_NO_MOUNT_AT_TIER           int32 = 0x2205
	FS_FILE_ALREADY_EXISTS        int32 = 0x2206
	FS_INVLD_LEVEL                int32 = 0x2207
	FS_NO_VALID_DISK              int32 = 0x2208
	MON_CONNECTION_INVALID        int32 = 0x2300

	UNKNOWN int32 = 0xffff
)

func (e *TaosError) Error() string {
	if e.Code != UNKNOWN {
		return fmt.Sprintf("[0x%x] %s", e.Code, e.ErrStr)
	}
	return e.ErrStr
}
func (e *TaosError) IsError(r *TaosError) bool {
	return e.Code == r.Code
}

var (
	ErrRpcActionInProgress = &TaosError{
		Code:   RPC_ACTION_IN_PROGRESS,
		ErrStr: "Action in progress",
	}
	ErrRpcAuthRequired = &TaosError{
		Code:   RPC_AUTH_REQUIRED,
		ErrStr: "Authentication required",
	}
	ErrRpcAuthFailure = &TaosError{
		Code:   RPC_AUTH_FAILURE,
		ErrStr: "Authentication failure",
	}
	ErrRpcRedirect = &TaosError{
		Code:   RPC_REDIRECT,
		ErrStr: "Redirect",
	}
	ErrRpcNotReady = &TaosError{
		Code:   RPC_NOT_READY,
		ErrStr: "System not ready",
	}
	ErrRpcAlreadyProcessed = &TaosError{
		Code:   RPC_ALREADY_PROCESSED,
		ErrStr: "Message already processed",
	}
	ErrRpcLastSessionNotFinished = &TaosError{
		Code:   RPC_LAST_SESSION_NOT_FINISHED,
		ErrStr: "Last session not finished",
	}
	ErrRpcMismatchedLinkId = &TaosError{
		Code:   RPC_MISMATCHED_LINK_ID,
		ErrStr: "Mismatched meter id",
	}
	ErrRpcTooSlow = &TaosError{
		Code:   RPC_TOO_SLOW,
		ErrStr: "Processing of request timed out",
	}
	ErrRpcMaxSessions = &TaosError{
		Code:   RPC_MAX_SESSIONS,
		ErrStr: "Number of sessions reached limit",
	}
	ErrRpcNetworkUnavail = &TaosError{
		Code:   RPC_NETWORK_UNAVAIL,
		ErrStr: "Unable to establish connection",
	}
	ErrRpcAppError = &TaosError{
		Code:   RPC_APP_ERROR,
		ErrStr: "Unexpected generic error in RPC",
	}
	ErrRpcUnexpectedResponse = &TaosError{
		Code:   RPC_UNEXPECTED_RESPONSE,
		ErrStr: "Unexpected response",
	}
	ErrRpcInvalidValue = &TaosError{
		Code:   RPC_INVALID_VALUE,
		ErrStr: "Invalid value",
	}
	ErrRpcInvalidTranId = &TaosError{
		Code:   RPC_INVALID_TRAN_ID,
		ErrStr: "Invalid transaction id",
	}
	ErrRpcInvalidSessionId = &TaosError{
		Code:   RPC_INVALID_SESSION_ID,
		ErrStr: "Invalid session id",
	}
	ErrRpcInvalidMsgType = &TaosError{
		Code:   RPC_INVALID_MSG_TYPE,
		ErrStr: "Invalid message type",
	}
	ErrRpcInvalidResponseType = &TaosError{
		Code:   RPC_INVALID_RESPONSE_TYPE,
		ErrStr: "Invalid response type",
	}
	ErrRpcInvalidTimeStamp = &TaosError{
		Code:   RPC_INVALID_TIME_STAMP,
		ErrStr: "Client and server's time is not synchronized",
	}
	ErrAppNotReady = &TaosError{
		Code:   APP_NOT_READY,
		ErrStr: "Database not ready",
	}
	ErrRpcFqdnError = &TaosError{
		Code:   RPC_FQDN_ERROR,
		ErrStr: "Unable to resolve FQDN",
	}
	ErrRpcInvalidVersion = &TaosError{
		Code:   RPC_INVALID_VERSION,
		ErrStr: "Invalid app version",
	}
	ErrComOpsNotSupport = &TaosError{
		Code:   COM_OPS_NOT_SUPPORT,
		ErrStr: "Operation not supported",
	}
	ErrComMemoryCorrupted = &TaosError{
		Code:   COM_MEMORY_CORRUPTED,
		ErrStr: "Memory corrupted",
	}
	ErrComOutOfMemory = &TaosError{
		Code:   COM_OUT_OF_MEMORY,
		ErrStr: "Out of memory",
	}
	ErrComInvalidCfgMsg = &TaosError{
		Code:   COM_INVALID_CFG_MSG,
		ErrStr: "Invalid config message",
	}
	ErrComFileCorrupted = &TaosError{
		Code:   COM_FILE_CORRUPTED,
		ErrStr: "Data file corrupted",
	}
	ErrRefNoMemory = &TaosError{
		Code:   REF_NO_MEMORY,
		ErrStr: "Ref out of memory",
	}
	ErrRefFull = &TaosError{
		Code:   REF_FULL,
		ErrStr: "too many Ref Objs",
	}
	ErrRefIdRemoved = &TaosError{
		Code:   REF_ID_REMOVED,
		ErrStr: "Ref ID is removed",
	}
	ErrRefInvalidId = &TaosError{
		Code:   REF_INVALID_ID,
		ErrStr: "Invalid Ref ID",
	}
	ErrRefAlreadyExist = &TaosError{
		Code:   REF_ALREADY_EXIST,
		ErrStr: "Ref is already there",
	}
	ErrRefNotExist = &TaosError{
		Code:   REF_NOT_EXIST,
		ErrStr: "Ref is not there",
	}
	ErrTscInvalidOperation = &TaosError{
		Code:   TSC_INVALID_OPERATION,
		ErrStr: "Invalid Operation",
	}
	ErrTscInvalidQhandle = &TaosError{
		Code:   TSC_INVALID_QHANDLE,
		ErrStr: "Invalid qhandle",
	}
	ErrTscInvalidTimeStamp = &TaosError{
		Code:   TSC_INVALID_TIME_STAMP,
		ErrStr: "Invalid combination of client/service time",
	}
	ErrTscInvalidValue = &TaosError{
		Code:   TSC_INVALID_VALUE,
		ErrStr: "Invalid value in client",
	}
	ErrTscInvalidVersion = &TaosError{
		Code:   TSC_INVALID_VERSION,
		ErrStr: "Invalid client version",
	}
	ErrTscInvalidIe = &TaosError{
		Code:   TSC_INVALID_IE,
		ErrStr: "Invalid client ie",
	}
	ErrTscInvalidFqdn = &TaosError{
		Code:   TSC_INVALID_FQDN,
		ErrStr: "Invalid host name",
	}
	ErrTscInvalidUserLength = &TaosError{
		Code:   TSC_INVALID_USER_LENGTH,
		ErrStr: "Invalid user name",
	}
	ErrTscInvalidPassLength = &TaosError{
		Code:   TSC_INVALID_PASS_LENGTH,
		ErrStr: "Invalid password",
	}
	ErrTscInvalidDbLength = &TaosError{
		Code:   TSC_INVALID_DB_LENGTH,
		ErrStr: "Database name too long",
	}
	ErrTscInvalidTableIdLength = &TaosError{
		Code:   TSC_INVALID_TABLE_ID_LENGTH,
		ErrStr: "Table name too long",
	}
	ErrTscInvalidConnection = &TaosError{
		Code:   TSC_INVALID_CONNECTION,
		ErrStr: "Invalid connection",
	}
	ErrTscOutOfMemory = &TaosError{
		Code:   TSC_OUT_OF_MEMORY,
		ErrStr: "System out of memory",
	}
	ErrTscNoDiskspace = &TaosError{
		Code:   TSC_NO_DISKSPACE,
		ErrStr: "System out of disk space",
	}
	ErrTscQueryCacheErased = &TaosError{
		Code:   TSC_QUERY_CACHE_ERASED,
		ErrStr: "Query cache erased",
	}
	ErrTscQueryCancelled = &TaosError{
		Code:   TSC_QUERY_CANCELLED,
		ErrStr: "Query terminated",
	}
	ErrTscSortedResTooMany = &TaosError{
		Code:   TSC_SORTED_RES_TOO_MANY,
		ErrStr: "Result set too large to be sorted",
	}
	ErrTscAppError = &TaosError{
		Code:   TSC_APP_ERROR,
		ErrStr: "Application error",
	}
	ErrTscActionInProgress = &TaosError{
		Code:   TSC_ACTION_IN_PROGRESS,
		ErrStr: "Action in progress",
	}
	ErrTscDisconnected = &TaosError{
		Code:   TSC_DISCONNECTED,
		ErrStr: "Disconnected from service",
	}
	ErrTscNoWriteAuth = &TaosError{
		Code:   TSC_NO_WRITE_AUTH,
		ErrStr: "No write permission",
	}
	ErrTscConnKilled = &TaosError{
		Code:   TSC_CONN_KILLED,
		ErrStr: "Connection killed",
	}
	ErrTscSqlSyntaxError = &TaosError{
		Code:   TSC_SQL_SYNTAX_ERROR,
		ErrStr: "Syntax error in SQL",
	}
	ErrTscDbNotSelected = &TaosError{
		Code:   TSC_DB_NOT_SELECTED,
		ErrStr: "Database not specified or available",
	}
	ErrTscInvalidTableName = &TaosError{
		Code:   TSC_INVALID_TABLE_NAME,
		ErrStr: "Table does not exist",
	}
	ErrTscExceedSqlLimit = &TaosError{
		Code:   TSC_EXCEED_SQL_LIMIT,
		ErrStr: "SQL statement too long check maxSQLLength config",
	}
	ErrTscFileEmpty = &TaosError{
		Code:   TSC_FILE_EMPTY,
		ErrStr: "File is empty",
	}
	ErrTscLineSyntaxError = &TaosError{
		Code:   TSC_LINE_SYNTAX_ERROR,
		ErrStr: "Syntax error in Line",
	}
	ErrTscNoMetaCached = &TaosError{
		Code:   TSC_NO_META_CACHED,
		ErrStr: "No table meta cached",
	}
	ErrMndMsgNotProcessed = &TaosError{
		Code:   MND_MSG_NOT_PROCESSED,
		ErrStr: "Message not processed",
	}
	ErrMndActionInProgress = &TaosError{
		Code:   MND_ACTION_IN_PROGRESS,
		ErrStr: "Message is progressing",
	}
	ErrMndActionNeedReprocessed = &TaosError{
		Code:   MND_ACTION_NEED_REPROCESSED,
		ErrStr: "Messag need to be reprocessed",
	}
	ErrMndNoRights = &TaosError{
		Code:   MND_NO_RIGHTS,
		ErrStr: "Insufficient privilege for operation",
	}
	ErrMndAppError = &TaosError{
		Code:   MND_APP_ERROR,
		ErrStr: "Unexpected generic error in mnode",
	}
	ErrMndInvalidConnection = &TaosError{
		Code:   MND_INVALID_CONNECTION,
		ErrStr: "Invalid message connection",
	}
	ErrMndInvalidMsgVersion = &TaosError{
		Code:   MND_INVALID_MSG_VERSION,
		ErrStr: "Incompatible protocol version",
	}
	ErrMndInvalidMsgLen = &TaosError{
		Code:   MND_INVALID_MSG_LEN,
		ErrStr: "Invalid message length",
	}
	ErrMndInvalidMsgType = &TaosError{
		Code:   MND_INVALID_MSG_TYPE,
		ErrStr: "Invalid message type",
	}
	ErrMndTooManyShellConns = &TaosError{
		Code:   MND_TOO_MANY_SHELL_CONNS,
		ErrStr: "Too many connections",
	}
	ErrMndOutOfMemory = &TaosError{
		Code:   MND_OUT_OF_MEMORY,
		ErrStr: "Out of memory in mnode",
	}
	ErrMndInvalidShowobj = &TaosError{
		Code:   MND_INVALID_SHOWOBJ,
		ErrStr: "Data expired",
	}
	ErrMndInvalidQueryId = &TaosError{
		Code:   MND_INVALID_QUERY_ID,
		ErrStr: "Invalid query id",
	}
	ErrMndInvalidStreamId = &TaosError{
		Code:   MND_INVALID_STREAM_ID,
		ErrStr: "Invalid stream id",
	}
	ErrMndInvalidConnId = &TaosError{
		Code:   MND_INVALID_CONN_ID,
		ErrStr: "Invalid connection id",
	}
	ErrMndMnodeIsRunning = &TaosError{
		Code:   MND_MNODE_IS_RUNNING,
		ErrStr: "mnode is alreay running",
	}
	ErrMndFailedToConfigSync = &TaosError{
		Code:   MND_FAILED_TO_CONFIG_SYNC,
		ErrStr: "failed to config sync",
	}
	ErrMndFailedToStartSync = &TaosError{
		Code:   MND_FAILED_TO_START_SYNC,
		ErrStr: "failed to start sync",
	}
	ErrMndFailedToCreateDir = &TaosError{
		Code:   MND_FAILED_TO_CREATE_DIR,
		ErrStr: "failed to create mnode dir",
	}
	ErrMndFailedToInitStep = &TaosError{
		Code:   MND_FAILED_TO_INIT_STEP,
		ErrStr: "failed to init components",
	}
	ErrMndSdbObjAlreadyThere = &TaosError{
		Code:   MND_SDB_OBJ_ALREADY_THERE,
		ErrStr: "Object already there",
	}
	ErrMndSdbError = &TaosError{
		Code:   MND_SDB_ERROR,
		ErrStr: "Unexpected generic error in sdb",
	}
	ErrMndSdbInvalidTableType = &TaosError{
		Code:   MND_SDB_INVALID_TABLE_TYPE,
		ErrStr: "Invalid table type",
	}
	ErrMndSdbObjNotThere = &TaosError{
		Code:   MND_SDB_OBJ_NOT_THERE,
		ErrStr: "Object not there",
	}
	ErrMndSdbInvaidMetaRow = &TaosError{
		Code:   MND_SDB_INVAID_META_ROW,
		ErrStr: "Invalid meta row",
	}
	ErrMndSdbInvaidKeyType = &TaosError{
		Code:   MND_SDB_INVAID_KEY_TYPE,
		ErrStr: "Invalid key type",
	}
	ErrMndDnodeAlreadyExist = &TaosError{
		Code:   MND_DNODE_ALREADY_EXIST,
		ErrStr: "DNode already exists",
	}
	ErrMndDnodeNotExist = &TaosError{
		Code:   MND_DNODE_NOT_EXIST,
		ErrStr: "DNode does not exist",
	}
	ErrMndVgroupNotExist = &TaosError{
		Code:   MND_VGROUP_NOT_EXIST,
		ErrStr: "VGroup does not exist",
	}
	ErrMndNoRemoveMaster = &TaosError{
		Code:   MND_NO_REMOVE_MASTER,
		ErrStr: "Master DNode cannot be removed",
	}
	ErrMndNoEnoughDnodes = &TaosError{
		Code:   MND_NO_ENOUGH_DNODES,
		ErrStr: "Out of DNodes",
	}
	ErrMndClusterCfgInconsistent = &TaosError{
		Code:   MND_CLUSTER_CFG_INCONSISTENT,
		ErrStr: "Cluster cfg inconsistent",
	}
	ErrMndInvalidDnodeCfgOption = &TaosError{
		Code:   MND_INVALID_DNODE_CFG_OPTION,
		ErrStr: "Invalid dnode cfg option",
	}
	ErrMndBalanceEnabled = &TaosError{
		Code:   MND_BALANCE_ENABLED,
		ErrStr: "Balance already enabled",
	}
	ErrMndVgroupNotInDnode = &TaosError{
		Code:   MND_VGROUP_NOT_IN_DNODE,
		ErrStr: "Vgroup not in dnode",
	}
	ErrMndVgroupAlreadyInDnode = &TaosError{
		Code:   MND_VGROUP_ALREADY_IN_DNODE,
		ErrStr: "Vgroup already in dnode",
	}
	ErrMndDnodeNotFree = &TaosError{
		Code:   MND_DNODE_NOT_FREE,
		ErrStr: "Dnode not avaliable",
	}
	ErrMndInvalidClusterId = &TaosError{
		Code:   MND_INVALID_CLUSTER_ID,
		ErrStr: "Cluster id not match",
	}
	ErrMndNotReady = &TaosError{
		Code:   MND_NOT_READY,
		ErrStr: "Cluster not ready",
	}
	ErrMndDnodeIdNotConfigured = &TaosError{
		Code:   MND_DNODE_ID_NOT_CONFIGURED,
		ErrStr: "Dnode Id not configured",
	}
	ErrMndDnodeEpNotConfigured = &TaosError{
		Code:   MND_DNODE_EP_NOT_CONFIGURED,
		ErrStr: "Dnode Ep not configured",
	}
	ErrMndAcctAlreadyExist = &TaosError{
		Code:   MND_ACCT_ALREADY_EXIST,
		ErrStr: "Account already exists",
	}
	ErrMndInvalidAcct = &TaosError{
		Code:   MND_INVALID_ACCT,
		ErrStr: "Invalid account",
	}
	ErrMndInvalidAcctOption = &TaosError{
		Code:   MND_INVALID_ACCT_OPTION,
		ErrStr: "Invalid account options",
	}
	ErrMndAcctExpired = &TaosError{
		Code:   MND_ACCT_EXPIRED,
		ErrStr: "Account authorization has expired",
	}
	ErrMndUserAlreadyExist = &TaosError{
		Code:   MND_USER_ALREADY_EXIST,
		ErrStr: "User already exists",
	}
	ErrMndInvalidUser = &TaosError{
		Code:   MND_INVALID_USER,
		ErrStr: "Invalid user",
	}
	ErrMndInvalidUserFormat = &TaosError{
		Code:   MND_INVALID_USER_FORMAT,
		ErrStr: "Invalid user format",
	}
	ErrMndInvalidPassFormat = &TaosError{
		Code:   MND_INVALID_PASS_FORMAT,
		ErrStr: "Invalid password format",
	}
	ErrMndNoUserFromConn = &TaosError{
		Code:   MND_NO_USER_FROM_CONN,
		ErrStr: "Can not get user from conn",
	}
	ErrMndTooManyUsers = &TaosError{
		Code:   MND_TOO_MANY_USERS,
		ErrStr: "Too many users",
	}
	ErrMndTableAlreadyExist = &TaosError{
		Code:   MND_TABLE_ALREADY_EXIST,
		ErrStr: "Table already exists",
	}
	ErrMndInvalidTableId = &TaosError{
		Code:   MND_INVALID_TABLE_ID,
		ErrStr: "Table name too long",
	}
	ErrMndInvalidTableName = &TaosError{
		Code:   MND_INVALID_TABLE_NAME,
		ErrStr: "Table does not exist",
	}
	ErrMndInvalidTableType = &TaosError{
		Code:   MND_INVALID_TABLE_TYPE,
		ErrStr: "Invalid table type in tsdb",
	}
	ErrMndTooManyTags = &TaosError{
		Code:   MND_TOO_MANY_TAGS,
		ErrStr: "Too many tags",
	}
	ErrMndTooManyColumns = &TaosError{
		Code:   MND_TOO_MANY_COLUMNS,
		ErrStr: "Too many columns",
	}
	ErrMndTooManyTimeseries = &TaosError{
		Code:   MND_TOO_MANY_TIMESERIES,
		ErrStr: "Too many time series",
	}
	ErrMndNotSuperTable = &TaosError{
		Code:   MND_NOT_SUPER_TABLE,
		ErrStr: "Not super table",
	}
	ErrMndColNameTooLong = &TaosError{
		Code:   MND_COL_NAME_TOO_LONG,
		ErrStr: "Tag name too long",
	}
	ErrMndTagAlreayExist = &TaosError{
		Code:   MND_TAG_ALREAY_EXIST,
		ErrStr: "Tag already exists",
	}
	ErrMndTagNotExist = &TaosError{
		Code:   MND_TAG_NOT_EXIST,
		ErrStr: "Tag does not exist",
	}
	ErrMndFieldAlreayExist = &TaosError{
		Code:   MND_FIELD_ALREAY_EXIST,
		ErrStr: "Field already exists",
	}
	ErrMndFieldNotExist = &TaosError{
		Code:   MND_FIELD_NOT_EXIST,
		ErrStr: "Field does not exist",
	}
	ErrMndInvalidStableName = &TaosError{
		Code:   MND_INVALID_STABLE_NAME,
		ErrStr: "Super table does not exist",
	}
	ErrMndInvalidCreateTableMsg = &TaosError{
		Code:   MND_INVALID_CREATE_TABLE_MSG,
		ErrStr: "Invalid create table message",
	}
	ErrMndExceedMaxRowBytes = &TaosError{
		Code:   MND_EXCEED_MAX_ROW_BYTES,
		ErrStr: "Exceed max row bytes",
	}
	ErrMndInvalidFuncName = &TaosError{
		Code:   MND_INVALID_FUNC_NAME,
		ErrStr: "Invalid func name",
	}
	ErrMndInvalidFuncLen = &TaosError{
		Code:   MND_INVALID_FUNC_LEN,
		ErrStr: "Invalid func length",
	}
	ErrMndInvalidFuncCode = &TaosError{
		Code:   MND_INVALID_FUNC_CODE,
		ErrStr: "Invalid func code",
	}
	ErrMndFuncAlreadyExist = &TaosError{
		Code:   MND_FUNC_ALREADY_EXIST,
		ErrStr: "Func already exists",
	}
	ErrMndInvalidFunc = &TaosError{
		Code:   MND_INVALID_FUNC,
		ErrStr: "Invalid func",
	}
	ErrMndInvalidFuncBufsize = &TaosError{
		Code:   MND_INVALID_FUNC_BUFSIZE,
		ErrStr: "Invalid func bufSize",
	}
	ErrMndDbNotSelected = &TaosError{
		Code:   MND_DB_NOT_SELECTED,
		ErrStr: "Database not specified or available",
	}
	ErrMndDbAlreadyExist = &TaosError{
		Code:   MND_DB_ALREADY_EXIST,
		ErrStr: "Database already exists",
	}
	ErrMndInvalidDbOption = &TaosError{
		Code:   MND_INVALID_DB_OPTION,
		ErrStr: "Invalid database options",
	}
	ErrMndInvalidDb = &TaosError{
		Code:   MND_INVALID_DB,
		ErrStr: "Invalid database name",
	}
	ErrMndMonitorDbForbidden = &TaosError{
		Code:   MND_MONITOR_DB_FORBIDDEN,
		ErrStr: "Cannot delete monitor database",
	}
	ErrMndTooManyDatabases = &TaosError{
		Code:   MND_TOO_MANY_DATABASES,
		ErrStr: "Too many databases for account",
	}
	ErrMndDbInDropping = &TaosError{
		Code:   MND_DB_IN_DROPPING,
		ErrStr: "Database not available",
	}
	ErrMndVgroupNotReady = &TaosError{
		Code:   MND_VGROUP_NOT_READY,
		ErrStr: "Database unsynced",
	}
	ErrMndInvalidDbOptionDays = &TaosError{
		Code:   MND_INVALID_DB_OPTION_DAYS,
		ErrStr: "Invalid database option: days out of range",
	}
	ErrMndInvalidDbOptionKeep = &TaosError{
		Code:   MND_INVALID_DB_OPTION_KEEP,
		ErrStr: "Invalid database option: keep >= keep1 >= keep0 >= days",
	}
	ErrMndInvalidTopic = &TaosError{
		Code:   MND_INVALID_TOPIC,
		ErrStr: "Invalid topic nam",
	}
	ErrMndInvalidTopicOption = &TaosError{
		Code:   MND_INVALID_TOPIC_OPTION,
		ErrStr: "Invalid topic optio",
	}
	ErrMndInvalidTopicPartitons = &TaosError{
		Code:   MND_INVALID_TOPIC_PARTITONS,
		ErrStr: "Invalid topic partitons num, valid range: [1, 1000",
	}
	ErrMndTopicAlreadyExist = &TaosError{
		Code:   MND_TOPIC_ALREADY_EXIST,
		ErrStr: "Topic already exist",
	}
	ErrDndMsgNotProcessed = &TaosError{
		Code:   DND_MSG_NOT_PROCESSED,
		ErrStr: "Message not processed",
	}
	ErrDndOutOfMemory = &TaosError{
		Code:   DND_OUT_OF_MEMORY,
		ErrStr: "Dnode out of memory",
	}
	ErrDndNoWriteAccess = &TaosError{
		Code:   DND_NO_WRITE_ACCESS,
		ErrStr: "No permission for disk files in dnode",
	}
	ErrDndInvalidMsgLen = &TaosError{
		Code:   DND_INVALID_MSG_LEN,
		ErrStr: "Invalid message length",
	}
	ErrDndActionInProgress = &TaosError{
		Code:   DND_ACTION_IN_PROGRESS,
		ErrStr: "Action in progress",
	}
	ErrDndTooManyVnodes = &TaosError{
		Code:   DND_TOO_MANY_VNODES,
		ErrStr: "Too many vnode directories",
	}
	ErrVndActionInProgress = &TaosError{
		Code:   VND_ACTION_IN_PROGRESS,
		ErrStr: "Action in progress",
	}
	ErrVndMsgNotProcessed = &TaosError{
		Code:   VND_MSG_NOT_PROCESSED,
		ErrStr: "Message not processed",
	}
	ErrVndActionNeedReprocessed = &TaosError{
		Code:   VND_ACTION_NEED_REPROCESSED,
		ErrStr: "Action need to be reprocessed",
	}
	ErrVndInvalidVgroupId = &TaosError{
		Code:   VND_INVALID_VGROUP_ID,
		ErrStr: "Invalid Vgroup ID",
	}
	ErrVndInitFailed = &TaosError{
		Code:   VND_INIT_FAILED,
		ErrStr: "Vnode initialization failed",
	}
	ErrVndNoDiskspace = &TaosError{
		Code:   VND_NO_DISKSPACE,
		ErrStr: "System out of disk space",
	}
	ErrVndNoDiskPermissions = &TaosError{
		Code:   VND_NO_DISK_PERMISSIONS,
		ErrStr: "No write permission for disk files",
	}
	ErrVndNoSuchFileOrDir = &TaosError{
		Code:   VND_NO_SUCH_FILE_OR_DIR,
		ErrStr: "Missing data file",
	}
	ErrVndOutOfMemory = &TaosError{
		Code:   VND_OUT_OF_MEMORY,
		ErrStr: "Out of memory",
	}
	ErrVndAppError = &TaosError{
		Code:   VND_APP_ERROR,
		ErrStr: "Unexpected generic error in vnode",
	}
	ErrVndInvalidVresionFile = &TaosError{
		Code:   VND_INVALID_VRESION_FILE,
		ErrStr: "Invalid version file",
	}
	ErrVndIsFull = &TaosError{
		Code:   VND_IS_FULL,
		ErrStr: "Database memory is full for commit failed",
	}
	ErrVndIsFlowctrl = &TaosError{
		Code:   VND_IS_FLOWCTRL,
		ErrStr: "Database memory is full for waiting commit",
	}
	ErrVndIsDropping = &TaosError{
		Code:   VND_IS_DROPPING,
		ErrStr: "Database is dropping",
	}
	ErrVndIsBalancing = &TaosError{
		Code:   VND_IS_BALANCING,
		ErrStr: "Database is balancing",
	}
	ErrVndIsClosing = &TaosError{
		Code:   VND_IS_CLOSING,
		ErrStr: "Database is closing",
	}
	ErrVndNotSynced = &TaosError{
		Code:   VND_NOT_SYNCED,
		ErrStr: "Database suspended",
	}
	ErrVndNoWriteAuth = &TaosError{
		Code:   VND_NO_WRITE_AUTH,
		ErrStr: "Database write operation denied",
	}
	ErrVndIsSyncing = &TaosError{
		Code:   VND_IS_SYNCING,
		ErrStr: "Database is syncing",
	}
	ErrVndInvalidTsdbState = &TaosError{
		Code:   VND_INVALID_TSDB_STATE,
		ErrStr: "Invalid tsdb state",
	}
	ErrTdbInvalidTableId = &TaosError{
		Code:   TDB_INVALID_TABLE_ID,
		ErrStr: "Invalid table ID",
	}
	ErrTdbInvalidTableType = &TaosError{
		Code:   TDB_INVALID_TABLE_TYPE,
		ErrStr: "Invalid table type",
	}
	ErrTdbIvdTbSchemaVersion = &TaosError{
		Code:   TDB_IVD_TB_SCHEMA_VERSION,
		ErrStr: "Invalid table schema version",
	}
	ErrTdbTableAlreadyExist = &TaosError{
		Code:   TDB_TABLE_ALREADY_EXIST,
		ErrStr: "Table already exists",
	}
	ErrTdbInvalidConfig = &TaosError{
		Code:   TDB_INVALID_CONFIG,
		ErrStr: "Invalid configuration",
	}
	ErrTdbInitFailed = &TaosError{
		Code:   TDB_INIT_FAILED,
		ErrStr: "Tsdb init failed",
	}
	ErrTdbNoDiskspace = &TaosError{
		Code:   TDB_NO_DISKSPACE,
		ErrStr: "No diskspace for tsdb",
	}
	ErrTdbNoDiskPermissions = &TaosError{
		Code:   TDB_NO_DISK_PERMISSIONS,
		ErrStr: "No permission for disk files",
	}
	ErrTdbFileCorrupted = &TaosError{
		Code:   TDB_FILE_CORRUPTED,
		ErrStr: "Data file(s) corrupted",
	}
	ErrTdbOutOfMemory = &TaosError{
		Code:   TDB_OUT_OF_MEMORY,
		ErrStr: "Out of memory",
	}
	ErrTdbTagVerOutOfDate = &TaosError{
		Code:   TDB_TAG_VER_OUT_OF_DATE,
		ErrStr: "Tag too old",
	}
	ErrTdbTimestampOutOfRange = &TaosError{
		Code:   TDB_TIMESTAMP_OUT_OF_RANGE,
		ErrStr: "Timestamp data out of range",
	}
	ErrTdbSubmitMsgMssedUp = &TaosError{
		Code:   TDB_SUBMIT_MSG_MSSED_UP,
		ErrStr: "Submit message is messed up",
	}
	ErrTdbInvalidAction = &TaosError{
		Code:   TDB_INVALID_ACTION,
		ErrStr: "Invalid operation",
	}
	ErrTdbInvalidCreateTbMsg = &TaosError{
		Code:   TDB_INVALID_CREATE_TB_MSG,
		ErrStr: "Invalid creation of table",
	}
	ErrTdbNoTableDataInMem = &TaosError{
		Code:   TDB_NO_TABLE_DATA_IN_MEM,
		ErrStr: "No table data in memory skiplist",
	}
	ErrTdbFileAlreadyExists = &TaosError{
		Code:   TDB_FILE_ALREADY_EXISTS,
		ErrStr: "File already exists",
	}
	ErrTdbTableReconfigure = &TaosError{
		Code:   TDB_TABLE_RECONFIGURE,
		ErrStr: "Need to reconfigure table",
	}
	ErrTdbIvdCreateTableInfo = &TaosError{
		Code:   TDB_IVD_CREATE_TABLE_INFO,
		ErrStr: "Invalid information to create table",
	}
	ErrTdbNoAvailDisk = &TaosError{
		Code:   TDB_NO_AVAIL_DISK,
		ErrStr: "No available disk",
	}
	ErrTdbMessedMsg = &TaosError{
		Code:   TDB_MESSED_MSG,
		ErrStr: "TSDB messed message",
	}
	ErrTdbIvldTagVal = &TaosError{
		Code:   TDB_IVLD_TAG_VAL,
		ErrStr: "TSDB invalid tag value",
	}
	ErrTdbNoCacheLastRow = &TaosError{
		Code:   TDB_NO_CACHE_LAST_ROW,
		ErrStr: "TSDB no cache last row data",
	}
	ErrQryInvalidQhandle = &TaosError{
		Code:   QRY_INVALID_QHANDLE,
		ErrStr: "Invalid handle",
	}
	ErrQryInvalidMsg = &TaosError{
		Code:   QRY_INVALID_MSG,
		ErrStr: "Invalid message",
	}
	ErrQryNoDiskspace = &TaosError{
		Code:   QRY_NO_DISKSPACE,
		ErrStr: "No diskspace for query",
	}
	ErrQryOutOfMemory = &TaosError{
		Code:   QRY_OUT_OF_MEMORY,
		ErrStr: "System out of memory",
	}
	ErrQryAppError = &TaosError{
		Code:   QRY_APP_ERROR,
		ErrStr: "Unexpected generic error in query",
	}
	ErrQryDupJoinKey = &TaosError{
		Code:   QRY_DUP_JOIN_KEY,
		ErrStr: "Duplicated join key",
	}
	ErrQryExceedTagsLimit = &TaosError{
		Code:   QRY_EXCEED_TAGS_LIMIT,
		ErrStr: "Tag conditon too many",
	}
	ErrQryNotReady = &TaosError{
		Code:   QRY_NOT_READY,
		ErrStr: "Query not ready",
	}
	ErrQryHasRsp = &TaosError{
		Code:   QRY_HAS_RSP,
		ErrStr: "Query should response",
	}
	ErrQryInExec = &TaosError{
		Code:   QRY_IN_EXEC,
		ErrStr: "Multiple retrieval of this query",
	}
	ErrQryTooManyTimewindow = &TaosError{
		Code:   QRY_TOO_MANY_TIMEWINDOW,
		ErrStr: "Too many time window in query",
	}
	ErrQryNotEnoughBuffer = &TaosError{
		Code:   QRY_NOT_ENOUGH_BUFFER,
		ErrStr: "Query buffer limit has reached",
	}
	ErrQryInconsistan = &TaosError{
		Code:   QRY_INCONSISTAN,
		ErrStr: "File inconsistency in replica",
	}
	ErrQrySysError = &TaosError{
		Code:   QRY_SYS_ERROR,
		ErrStr: "System error",
	}
	ErrGrantExpired = &TaosError{
		Code:   GRANT_EXPIRED,
		ErrStr: "License expired",
	}
	ErrGrantDnodeLimited = &TaosError{
		Code:   GRANT_DNODE_LIMITED,
		ErrStr: "DNode creation limited by licence",
	}
	ErrGrantAcctLimited = &TaosError{
		Code:   GRANT_ACCT_LIMITED,
		ErrStr: "Account creation limited by license",
	}
	ErrGrantTimeseriesLimited = &TaosError{
		Code:   GRANT_TIMESERIES_LIMITED,
		ErrStr: "Table creation limited by license",
	}
	ErrGrantDbLimited = &TaosError{
		Code:   GRANT_DB_LIMITED,
		ErrStr: "DB creation limited by license",
	}
	ErrGrantUserLimited = &TaosError{
		Code:   GRANT_USER_LIMITED,
		ErrStr: "User creation limited by license",
	}
	ErrGrantConnLimited = &TaosError{
		Code:   GRANT_CONN_LIMITED,
		ErrStr: "Conn creation limited by license",
	}
	ErrGrantStreamLimited = &TaosError{
		Code:   GRANT_STREAM_LIMITED,
		ErrStr: "Stream creation limited by license",
	}
	ErrGrantSpeedLimited = &TaosError{
		Code:   GRANT_SPEED_LIMITED,
		ErrStr: "Write speed limited by license",
	}
	ErrGrantStorageLimited = &TaosError{
		Code:   GRANT_STORAGE_LIMITED,
		ErrStr: "Storage capacity limited by license",
	}
	ErrGrantQuerytimeLimited = &TaosError{
		Code:   GRANT_QUERYTIME_LIMITED,
		ErrStr: "Query time limited by license",
	}
	ErrGrantCpuLimited = &TaosError{
		Code:   GRANT_CPU_LIMITED,
		ErrStr: "CPU cores limited by license",
	}
	ErrSynInvalidConfig = &TaosError{
		Code:   SYN_INVALID_CONFIG,
		ErrStr: "Invalid Sync Configuration",
	}
	ErrSynNotEnabled = &TaosError{
		Code:   SYN_NOT_ENABLED,
		ErrStr: "Sync module not enabled",
	}
	ErrSynInvalidVersion = &TaosError{
		Code:   SYN_INVALID_VERSION,
		ErrStr: "Invalid Sync version",
	}
	ErrSynConfirmExpired = &TaosError{
		Code:   SYN_CONFIRM_EXPIRED,
		ErrStr: "Sync confirm expired",
	}
	ErrSynTooManyFwdinfo = &TaosError{
		Code:   SYN_TOO_MANY_FWDINFO,
		ErrStr: "Too many sync fwd infos",
	}
	ErrSynMismatchedProtocol = &TaosError{
		Code:   SYN_MISMATCHED_PROTOCOL,
		ErrStr: "Mismatched protocol",
	}
	ErrSynMismatchedClusterid = &TaosError{
		Code:   SYN_MISMATCHED_CLUSTERID,
		ErrStr: "Mismatched clusterId",
	}
	ErrSynMismatchedSignature = &TaosError{
		Code:   SYN_MISMATCHED_SIGNATURE,
		ErrStr: "Mismatched signature",
	}
	ErrSynInvalidChecksum = &TaosError{
		Code:   SYN_INVALID_CHECKSUM,
		ErrStr: "Invalid msg checksum",
	}
	ErrSynInvalidMsglen = &TaosError{
		Code:   SYN_INVALID_MSGLEN,
		ErrStr: "Invalid msg length",
	}
	ErrSynInvalidMsgtype = &TaosError{
		Code:   SYN_INVALID_MSGTYPE,
		ErrStr: "Invalid msg type",
	}
	ErrWalAppError = &TaosError{
		Code:   WAL_APP_ERROR,
		ErrStr: "Unexpected generic error in wal",
	}
	ErrWalFileCorrupted = &TaosError{
		Code:   WAL_FILE_CORRUPTED,
		ErrStr: "WAL file is corrupted",
	}
	ErrWalSizeLimit = &TaosError{
		Code:   WAL_SIZE_LIMIT,
		ErrStr: "WAL size exceeds limit",
	}
	ErrHttpServerOffline = &TaosError{
		Code:   HTTP_SERVER_OFFLINE,
		ErrStr: "http server is not onlin",
	}
	ErrHttpUnsupportUrl = &TaosError{
		Code:   HTTP_UNSUPPORT_URL,
		ErrStr: "url is not support",
	}
	ErrHttpInvalidUrl = &TaosError{
		Code:   HTTP_INVALID_URL,
		ErrStr: "nvalid url format",
	}
	ErrHttpNoEnoughMemory = &TaosError{
		Code:   HTTP_NO_ENOUGH_MEMORY,
		ErrStr: "no enough memory",
	}
	ErrHttpRequsetTooBig = &TaosError{
		Code:   HTTP_REQUSET_TOO_BIG,
		ErrStr: "request size is too big",
	}
	ErrHttpNoAuthInfo = &TaosError{
		Code:   HTTP_NO_AUTH_INFO,
		ErrStr: "no auth info input",
	}
	ErrHttpNoMsgInput = &TaosError{
		Code:   HTTP_NO_MSG_INPUT,
		ErrStr: "request is empty",
	}
	ErrHttpNoSqlInput = &TaosError{
		Code:   HTTP_NO_SQL_INPUT,
		ErrStr: "no sql input",
	}
	ErrHttpNoExecUsedb = &TaosError{
		Code:   HTTP_NO_EXEC_USEDB,
		ErrStr: "no need to execute use db cmd",
	}
	ErrHttpSessionFull = &TaosError{
		Code:   HTTP_SESSION_FULL,
		ErrStr: "session list was full",
	}
	ErrHttpGenTaosdTokenErr = &TaosError{
		Code:   HTTP_GEN_TAOSD_TOKEN_ERR,
		ErrStr: "generate taosd token error",
	}
	ErrHttpInvalidMultiRequest = &TaosError{
		Code:   HTTP_INVALID_MULTI_REQUEST,
		ErrStr: "size of multi request is 0",
	}
	ErrHttpCreateGzipFailed = &TaosError{
		Code:   HTTP_CREATE_GZIP_FAILED,
		ErrStr: "failed to create gzip",
	}
	ErrHttpFinishGzipFailed = &TaosError{
		Code:   HTTP_FINISH_GZIP_FAILED,
		ErrStr: "failed to finish gzip",
	}
	ErrHttpLoginFailed = &TaosError{
		Code:   HTTP_LOGIN_FAILED,
		ErrStr: "failed to login",
	}
	ErrHttpInvalidVersion = &TaosError{
		Code:   HTTP_INVALID_VERSION,
		ErrStr: "invalid http version",
	}
	ErrHttpInvalidContentLength = &TaosError{
		Code:   HTTP_INVALID_CONTENT_LENGTH,
		ErrStr: "invalid content length",
	}
	ErrHttpInvalidAuthType = &TaosError{
		Code:   HTTP_INVALID_AUTH_TYPE,
		ErrStr: "invalid type of Authorization",
	}
	ErrHttpInvalidAuthFormat = &TaosError{
		Code:   HTTP_INVALID_AUTH_FORMAT,
		ErrStr: "invalid format of Authorization",
	}
	ErrHttpInvalidBasicAuth = &TaosError{
		Code:   HTTP_INVALID_BASIC_AUTH,
		ErrStr: "invalid basic Authorization",
	}
	ErrHttpInvalidTaosdAuth = &TaosError{
		Code:   HTTP_INVALID_TAOSD_AUTH,
		ErrStr: "invalid taosd Authorization",
	}
	ErrHttpParseMethodFailed = &TaosError{
		Code:   HTTP_PARSE_METHOD_FAILED,
		ErrStr: "failed to parse method",
	}
	ErrHttpParseTargetFailed = &TaosError{
		Code:   HTTP_PARSE_TARGET_FAILED,
		ErrStr: "failed to parse target",
	}
	ErrHttpParseVersionFailed = &TaosError{
		Code:   HTTP_PARSE_VERSION_FAILED,
		ErrStr: "failed to parse http version",
	}
	ErrHttpParseSpFailed = &TaosError{
		Code:   HTTP_PARSE_SP_FAILED,
		ErrStr: "failed to parse sp",
	}
	ErrHttpParseStatusFailed = &TaosError{
		Code:   HTTP_PARSE_STATUS_FAILED,
		ErrStr: "failed to parse status",
	}
	ErrHttpParsePhraseFailed = &TaosError{
		Code:   HTTP_PARSE_PHRASE_FAILED,
		ErrStr: "failed to parse phrase",
	}
	ErrHttpParseCrlfFailed = &TaosError{
		Code:   HTTP_PARSE_CRLF_FAILED,
		ErrStr: "failed to parse crlf",
	}
	ErrHttpParseHeaderFailed = &TaosError{
		Code:   HTTP_PARSE_HEADER_FAILED,
		ErrStr: "failed to parse header",
	}
	ErrHttpParseHeaderKeyFailed = &TaosError{
		Code:   HTTP_PARSE_HEADER_KEY_FAILED,
		ErrStr: "failed to parse header key",
	}
	ErrHttpParseHeaderValFailed = &TaosError{
		Code:   HTTP_PARSE_HEADER_VAL_FAILED,
		ErrStr: "failed to parse header val",
	}
	ErrHttpParseChunkSizeFailed = &TaosError{
		Code:   HTTP_PARSE_CHUNK_SIZE_FAILED,
		ErrStr: "failed to parse chunk size",
	}
	ErrHttpParseChunkFailed = &TaosError{
		Code:   HTTP_PARSE_CHUNK_FAILED,
		ErrStr: "failed to parse chunk",
	}
	ErrHttpParseEndFailed = &TaosError{
		Code:   HTTP_PARSE_END_FAILED,
		ErrStr: "failed to parse end section",
	}
	ErrHttpParseInvalidState = &TaosError{
		Code:   HTTP_PARSE_INVALID_STATE,
		ErrStr: "invalid parse state",
	}
	ErrHttpParseErrorState = &TaosError{
		Code:   HTTP_PARSE_ERROR_STATE,
		ErrStr: "failed to parse error section",
	}
	ErrHttpGcQueryNull = &TaosError{
		Code:   HTTP_GC_QUERY_NULL,
		ErrStr: "query size is 0",
	}
	ErrHttpGcQuerySize = &TaosError{
		Code:   HTTP_GC_QUERY_SIZE,
		ErrStr: "query size can not more than 100",
	}
	ErrHttpGcReqParseError = &TaosError{
		Code:   HTTP_GC_REQ_PARSE_ERROR,
		ErrStr: "parse grafana json error",
	}
	ErrHttpTgDbNotInput = &TaosError{
		Code:   HTTP_TG_DB_NOT_INPUT,
		ErrStr: "database name can not be null",
	}
	ErrHttpTgDbTooLong = &TaosError{
		Code:   HTTP_TG_DB_TOO_LONG,
		ErrStr: "database name too long",
	}
	ErrHttpTgInvalidJson = &TaosError{
		Code:   HTTP_TG_INVALID_JSON,
		ErrStr: "invalid telegraf json fromat",
	}
	ErrHttpTgMetricsNull = &TaosError{
		Code:   HTTP_TG_METRICS_NULL,
		ErrStr: "metrics size is 0",
	}
	ErrHttpTgMetricsSize = &TaosError{
		Code:   HTTP_TG_METRICS_SIZE,
		ErrStr: "metrics size can not more than 1K",
	}
	ErrHttpTgMetricNull = &TaosError{
		Code:   HTTP_TG_METRIC_NULL,
		ErrStr: "metric name not find",
	}
	ErrHttpTgMetricType = &TaosError{
		Code:   HTTP_TG_METRIC_TYPE,
		ErrStr: "metric name type should be string",
	}
	ErrHttpTgMetricNameNull = &TaosError{
		Code:   HTTP_TG_METRIC_NAME_NULL,
		ErrStr: "metric name length is 0",
	}
	ErrHttpTgMetricNameLong = &TaosError{
		Code:   HTTP_TG_METRIC_NAME_LONG,
		ErrStr: "metric name length too long",
	}
	ErrHttpTgTimestampNull = &TaosError{
		Code:   HTTP_TG_TIMESTAMP_NULL,
		ErrStr: "timestamp not find",
	}
	ErrHttpTgTimestampType = &TaosError{
		Code:   HTTP_TG_TIMESTAMP_TYPE,
		ErrStr: "timestamp type should be integer",
	}
	ErrHttpTgTimestampValNull = &TaosError{
		Code:   HTTP_TG_TIMESTAMP_VAL_NULL,
		ErrStr: "timestamp value smaller than 0",
	}
	ErrHttpTgTagsNull = &TaosError{
		Code:   HTTP_TG_TAGS_NULL,
		ErrStr: "tags not find",
	}
	ErrHttpTgTagsSize0 = &TaosError{
		Code:   HTTP_TG_TAGS_SIZE_0,
		ErrStr: "tags size is 0",
	}
	ErrHttpTgTagsSizeLong = &TaosError{
		Code:   HTTP_TG_TAGS_SIZE_LONG,
		ErrStr: "tags size too long",
	}
	ErrHttpTgTagNull = &TaosError{
		Code:   HTTP_TG_TAG_NULL,
		ErrStr: "tag is null",
	}
	ErrHttpTgTagNameNull = &TaosError{
		Code:   HTTP_TG_TAG_NAME_NULL,
		ErrStr: "tag name is null",
	}
	ErrHttpTgTagNameSize = &TaosError{
		Code:   HTTP_TG_TAG_NAME_SIZE,
		ErrStr: "tag name length too long",
	}
	ErrHttpTgTagValueType = &TaosError{
		Code:   HTTP_TG_TAG_VALUE_TYPE,
		ErrStr: "tag value type should be number or string",
	}
	ErrHttpTgTagValueNull = &TaosError{
		Code:   HTTP_TG_TAG_VALUE_NULL,
		ErrStr: "tag value is null",
	}
	ErrHttpTgTableNull = &TaosError{
		Code:   HTTP_TG_TABLE_NULL,
		ErrStr: "table is null",
	}
	ErrHttpTgTableSize = &TaosError{
		Code:   HTTP_TG_TABLE_SIZE,
		ErrStr: "table name length too long",
	}
	ErrHttpTgFieldsNull = &TaosError{
		Code:   HTTP_TG_FIELDS_NULL,
		ErrStr: "fields not find",
	}
	ErrHttpTgFieldsSize0 = &TaosError{
		Code:   HTTP_TG_FIELDS_SIZE_0,
		ErrStr: "fields size is 0",
	}
	ErrHttpTgFieldsSizeLong = &TaosError{
		Code:   HTTP_TG_FIELDS_SIZE_LONG,
		ErrStr: "fields size too long",
	}
	ErrHttpTgFieldNull = &TaosError{
		Code:   HTTP_TG_FIELD_NULL,
		ErrStr: "field is null",
	}
	ErrHttpTgFieldNameNull = &TaosError{
		Code:   HTTP_TG_FIELD_NAME_NULL,
		ErrStr: "field name is null",
	}
	ErrHttpTgFieldNameSize = &TaosError{
		Code:   HTTP_TG_FIELD_NAME_SIZE,
		ErrStr: "field name length too long",
	}
	ErrHttpTgFieldValueType = &TaosError{
		Code:   HTTP_TG_FIELD_VALUE_TYPE,
		ErrStr: "field value type should be number or string",
	}
	ErrHttpTgFieldValueNull = &TaosError{
		Code:   HTTP_TG_FIELD_VALUE_NULL,
		ErrStr: "field value is null",
	}
	ErrHttpTgHostNotString = &TaosError{
		Code:   HTTP_TG_HOST_NOT_STRING,
		ErrStr: "host type should be string",
	}
	ErrHttpTgStableNotExist = &TaosError{
		Code:   HTTP_TG_STABLE_NOT_EXIST,
		ErrStr: "stable not exist",
	}
	ErrHttpOpDbNotInput = &TaosError{
		Code:   HTTP_OP_DB_NOT_INPUT,
		ErrStr: "database name can not be null",
	}
	ErrHttpOpDbTooLong = &TaosError{
		Code:   HTTP_OP_DB_TOO_LONG,
		ErrStr: "database name too long",
	}
	ErrHttpOpInvalidJson = &TaosError{
		Code:   HTTP_OP_INVALID_JSON,
		ErrStr: "invalid opentsdb json fromat",
	}
	ErrHttpOpMetricsNull = &TaosError{
		Code:   HTTP_OP_METRICS_NULL,
		ErrStr: "metrics size is 0",
	}
	ErrHttpOpMetricsSize = &TaosError{
		Code:   HTTP_OP_METRICS_SIZE,
		ErrStr: "metrics size can not more than 10K",
	}
	ErrHttpOpMetricNull = &TaosError{
		Code:   HTTP_OP_METRIC_NULL,
		ErrStr: "metric name not find",
	}
	ErrHttpOpMetricType = &TaosError{
		Code:   HTTP_OP_METRIC_TYPE,
		ErrStr: "metric name type should be string",
	}
	ErrHttpOpMetricNameNull = &TaosError{
		Code:   HTTP_OP_METRIC_NAME_NULL,
		ErrStr: "metric name length is 0",
	}
	ErrHttpOpMetricNameLong = &TaosError{
		Code:   HTTP_OP_METRIC_NAME_LONG,
		ErrStr: "metric name length can not more than 22",
	}
	ErrHttpOpTimestampNull = &TaosError{
		Code:   HTTP_OP_TIMESTAMP_NULL,
		ErrStr: "timestamp not find",
	}
	ErrHttpOpTimestampType = &TaosError{
		Code:   HTTP_OP_TIMESTAMP_TYPE,
		ErrStr: "timestamp type should be integer",
	}
	ErrHttpOpTimestampValNull = &TaosError{
		Code:   HTTP_OP_TIMESTAMP_VAL_NULL,
		ErrStr: "timestamp value smaller than 0",
	}
	ErrHttpOpTagsNull = &TaosError{
		Code:   HTTP_OP_TAGS_NULL,
		ErrStr: "tags not find",
	}
	ErrHttpOpTagsSize0 = &TaosError{
		Code:   HTTP_OP_TAGS_SIZE_0,
		ErrStr: "tags size is 0",
	}
	ErrHttpOpTagsSizeLong = &TaosError{
		Code:   HTTP_OP_TAGS_SIZE_LONG,
		ErrStr: "tags size too long",
	}
	ErrHttpOpTagNull = &TaosError{
		Code:   HTTP_OP_TAG_NULL,
		ErrStr: "tag is null",
	}
	ErrHttpOpTagNameNull = &TaosError{
		Code:   HTTP_OP_TAG_NAME_NULL,
		ErrStr: "tag name is null",
	}
	ErrHttpOpTagNameSize = &TaosError{
		Code:   HTTP_OP_TAG_NAME_SIZE,
		ErrStr: "tag name length too long",
	}
	ErrHttpOpTagValueType = &TaosError{
		Code:   HTTP_OP_TAG_VALUE_TYPE,
		ErrStr: "tag value type should be boolean number or string",
	}
	ErrHttpOpTagValueNull = &TaosError{
		Code:   HTTP_OP_TAG_VALUE_NULL,
		ErrStr: "tag value is null",
	}
	ErrHttpOpTagValueTooLong = &TaosError{
		Code:   HTTP_OP_TAG_VALUE_TOO_LONG,
		ErrStr: "tag value can not more than 64",
	}
	ErrHttpOpValueNull = &TaosError{
		Code:   HTTP_OP_VALUE_NULL,
		ErrStr: "value not find",
	}
	ErrHttpOpValueType = &TaosError{
		Code:   HTTP_OP_VALUE_TYPE,
		ErrStr: "value type should be boolean number or string",
	}
	ErrHttpRequestJsonError = &TaosError{
		Code:   HTTP_REQUEST_JSON_ERROR,
		ErrStr: "http request json error",
	}
	ErrOdbcOom = &TaosError{
		Code:   ODBC_OOM,
		ErrStr: "out of memory",
	}
	ErrOdbcConvCharNotNum = &TaosError{
		Code:   ODBC_CONV_CHAR_NOT_NUM,
		ErrStr: "convertion not a valid literal input",
	}
	ErrOdbcConvUndef = &TaosError{
		Code:   ODBC_CONV_UNDEF,
		ErrStr: "convertion undefined",
	}
	ErrOdbcConvTruncFrac = &TaosError{
		Code:   ODBC_CONV_TRUNC_FRAC,
		ErrStr: "convertion fractional truncated",
	}
	ErrOdbcConvTrunc = &TaosError{
		Code:   ODBC_CONV_TRUNC,
		ErrStr: "convertion truncated",
	}
	ErrOdbcConvNotSupport = &TaosError{
		Code:   ODBC_CONV_NOT_SUPPORT,
		ErrStr: "convertion not supported",
	}
	ErrOdbcConvOor = &TaosError{
		Code:   ODBC_CONV_OOR,
		ErrStr: "convertion numeric value out of range",
	}
	ErrOdbcOutOfRange = &TaosError{
		Code:   ODBC_OUT_OF_RANGE,
		ErrStr: "out of range",
	}
	ErrOdbcNotSupport = &TaosError{
		Code:   ODBC_NOT_SUPPORT,
		ErrStr: "not supported yet",
	}
	ErrOdbcInvalidHandle = &TaosError{
		Code:   ODBC_INVALID_HANDLE,
		ErrStr: "invalid handle",
	}
	ErrOdbcNoResult = &TaosError{
		Code:   ODBC_NO_RESULT,
		ErrStr: "no result set",
	}
	ErrOdbcNoFields = &TaosError{
		Code:   ODBC_NO_FIELDS,
		ErrStr: "no fields returned",
	}
	ErrOdbcInvalidCursor = &TaosError{
		Code:   ODBC_INVALID_CURSOR,
		ErrStr: "invalid cursor",
	}
	ErrOdbcStatementNotReady = &TaosError{
		Code:   ODBC_STATEMENT_NOT_READY,
		ErrStr: "statement not ready",
	}
	ErrOdbcConnectionBusy = &TaosError{
		Code:   ODBC_CONNECTION_BUSY,
		ErrStr: "connection still busy",
	}
	ErrOdbcBadConnstr = &TaosError{
		Code:   ODBC_BAD_CONNSTR,
		ErrStr: "bad connection string",
	}
	ErrOdbcBadArg = &TaosError{
		Code:   ODBC_BAD_ARG,
		ErrStr: "bad argument",
	}
	ErrOdbcConvNotValidTs = &TaosError{
		Code:   ODBC_CONV_NOT_VALID_TS,
		ErrStr: "not a valid timestamp",
	}
	ErrOdbcConvSrcTooLarge = &TaosError{
		Code:   ODBC_CONV_SRC_TOO_LARGE,
		ErrStr: "src too large",
	}
	ErrOdbcConvSrcBadSeq = &TaosError{
		Code:   ODBC_CONV_SRC_BAD_SEQ,
		ErrStr: "src bad sequence",
	}
	ErrOdbcConvSrcIncomplete = &TaosError{
		Code:   ODBC_CONV_SRC_INCOMPLETE,
		ErrStr: "src incomplete",
	}
	ErrOdbcConvSrcGeneral = &TaosError{
		Code:   ODBC_CONV_SRC_GENERAL,
		ErrStr: "src general",
	}
	ErrFsOutOfMemory = &TaosError{
		Code:   FS_OUT_OF_MEMORY,
		ErrStr: "tfs out of memory",
	}
	ErrFsInvldCfg = &TaosError{
		Code:   FS_INVLD_CFG,
		ErrStr: "tfs invalid mount config",
	}
	ErrFsTooManyMount = &TaosError{
		Code:   FS_TOO_MANY_MOUNT,
		ErrStr: "tfs too many mount",
	}
	ErrFsDupPrimary = &TaosError{
		Code:   FS_DUP_PRIMARY,
		ErrStr: "tfs duplicate primary mount",
	}
	ErrFsNoPrimaryDisk = &TaosError{
		Code:   FS_NO_PRIMARY_DISK,
		ErrStr: "tfs no primary mount",
	}
	ErrFsNoMountAtTier = &TaosError{
		Code:   FS_NO_MOUNT_AT_TIER,
		ErrStr: "tfs no mount at tier",
	}
	ErrFsFileAlreadyExists = &TaosError{
		Code:   FS_FILE_ALREADY_EXISTS,
		ErrStr: "tfs file already exists",
	}
	ErrFsInvldLevel = &TaosError{
		Code:   FS_INVLD_LEVEL,
		ErrStr: "tfs invalid level",
	}
	ErrFsNoValidDisk = &TaosError{
		Code:   FS_NO_VALID_DISK,
		ErrStr: "tfs no valid disk",
	}
	ErrMonConnectionInvalid = &TaosError{
		Code:   MON_CONNECTION_INVALID,
		ErrStr: "monitor invalid monitor db connection",
	}
)

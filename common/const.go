package common

const (
	MaxTaosSqlLen   = 1048576
	DefaultUser     = "root"
	DefaultPassword = "taosdata"
)

const (
	PrecisionMilliSecond = 0
	PrecisionMicroSecond = 1
	PrecisionNanoSecond  = 2
)

const (
	TSDB_OPTION_LOCALE = iota
	TSDB_OPTION_CHARSET
	TSDB_OPTION_TIMEZONE
	TSDB_OPTION_CONFIGDIR
	TSDB_OPTION_SHELL_ACTIVITY_TIMER
	TSDB_MAX_OPTIONS
)

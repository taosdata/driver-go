package common

import (
	"reflect"
)

type DBType struct {
	IsVarData   bool
	ID          int
	Length      int
	Name        string
	ReflectType reflect.Type
}

var NullType = DBType{
	ID:          TSDB_DATA_TYPE_NULL,
	Name:        TSDB_DATA_TYPE_NULL_Str,
	Length:      0,
	ReflectType: UnknownType,
	IsVarData:   false,
}

var BoolType = DBType{
	ID:          TSDB_DATA_TYPE_BOOL,
	Name:        TSDB_DATA_TYPE_BOOL_Str,
	Length:      1,
	ReflectType: NullBool,
	IsVarData:   false,
}

var TinyIntType = DBType{
	ID:          TSDB_DATA_TYPE_TINYINT,
	Name:        TSDB_DATA_TYPE_TINYINT_Str,
	Length:      1,
	ReflectType: NullInt8,
	IsVarData:   false,
}

var SmallIntType = DBType{
	ID:          TSDB_DATA_TYPE_SMALLINT,
	Name:        TSDB_DATA_TYPE_SMALLINT_Str,
	Length:      2,
	ReflectType: NullInt16,
	IsVarData:   false,
}

var IntType = DBType{
	ID:          TSDB_DATA_TYPE_INT,
	Name:        TSDB_DATA_TYPE_INT_Str,
	Length:      4,
	ReflectType: NullInt32,
	IsVarData:   false,
}

var BigIntType = DBType{
	ID:          TSDB_DATA_TYPE_BIGINT,
	Name:        TSDB_DATA_TYPE_BIGINT_Str,
	Length:      8,
	ReflectType: NullInt64,
	IsVarData:   false,
}

var UTinyIntType = DBType{
	ID:          TSDB_DATA_TYPE_UTINYINT,
	Name:        TSDB_DATA_TYPE_UTINYINT_Str,
	Length:      1,
	ReflectType: NullUInt8,
	IsVarData:   false,
}

var USmallIntType = DBType{
	ID:          TSDB_DATA_TYPE_USMALLINT,
	Name:        TSDB_DATA_TYPE_USMALLINT_Str,
	Length:      2,
	ReflectType: NullUInt16,
	IsVarData:   false,
}

var UIntType = DBType{
	ID:          TSDB_DATA_TYPE_UINT,
	Name:        TSDB_DATA_TYPE_UINT_Str,
	Length:      4,
	ReflectType: NullUInt32,
	IsVarData:   false,
}

var UBigIntType = DBType{
	ID:          TSDB_DATA_TYPE_UBIGINT,
	Name:        TSDB_DATA_TYPE_UBIGINT_Str,
	Length:      8,
	ReflectType: NullUInt64,
	IsVarData:   false,
}

var FloatType = DBType{
	ID:          TSDB_DATA_TYPE_FLOAT,
	Name:        TSDB_DATA_TYPE_FLOAT_Str,
	Length:      4,
	ReflectType: NullFloat32,
	IsVarData:   false,
}

var DoubleType = DBType{
	ID:          TSDB_DATA_TYPE_DOUBLE,
	Name:        TSDB_DATA_TYPE_DOUBLE_Str,
	Length:      8,
	ReflectType: NullFloat64,
	IsVarData:   false,
}

var BinaryType = DBType{
	ID:          TSDB_DATA_TYPE_BINARY,
	Name:        TSDB_DATA_TYPE_BINARY_Str,
	Length:      0,
	ReflectType: NullString,
	IsVarData:   true,
}

var NcharType = DBType{
	ID:          TSDB_DATA_TYPE_NCHAR,
	Name:        TSDB_DATA_TYPE_NCHAR_Str,
	Length:      0,
	ReflectType: NullString,
	IsVarData:   true,
}

var TimestampType = DBType{
	ID:          TSDB_DATA_TYPE_TIMESTAMP,
	Name:        TSDB_DATA_TYPE_TIMESTAMP_Str,
	Length:      8,
	ReflectType: NullTime,
	IsVarData:   false,
}

var JsonType = DBType{
	ID:          TSDB_DATA_TYPE_JSON,
	Name:        TSDB_DATA_TYPE_JSON_Str,
	Length:      0,
	ReflectType: NullJson,
	IsVarData:   true,
}

var VarBinaryType = DBType{
	ID:          TSDB_DATA_TYPE_VARBINARY,
	Name:        TSDB_DATA_TYPE_VARBINARY_Str,
	Length:      0,
	ReflectType: NullString,
	IsVarData:   true,
}

var GeometryType = DBType{
	ID:          TSDB_DATA_TYPE_GEOMETRY,
	Name:        TSDB_DATA_TYPE_GEOMETRY_Str,
	Length:      0,
	ReflectType: NullString,
	IsVarData:   true,
}

var allType = [21]*DBType{
	//TSDB_DATA_TYPE_NULL       = 0
	&NullType,
	//TSDB_DATA_TYPE_BOOL       = 1
	&BoolType,
	//TSDB_DATA_TYPE_TINYINT    = 2
	&TinyIntType,
	//TSDB_DATA_TYPE_SMALLINT   = 3
	&SmallIntType,
	//TSDB_DATA_TYPE_INT        = 4
	&IntType,
	//TSDB_DATA_TYPE_BIGINT     = 5
	&BigIntType,
	//TSDB_DATA_TYPE_FLOAT      = 6
	&FloatType,
	//TSDB_DATA_TYPE_DOUBLE     = 7
	&DoubleType,
	//TSDB_DATA_TYPE_BINARY     = 8
	&BinaryType,
	//TSDB_DATA_TYPE_TIMESTAMP  = 9
	&TimestampType,
	//TSDB_DATA_TYPE_NCHAR      = 10
	&NcharType,
	//TSDB_DATA_TYPE_UTINYINT   = 11
	&UTinyIntType,
	//TSDB_DATA_TYPE_USMALLINT  = 12
	&USmallIntType,
	//TSDB_DATA_TYPE_UINT       = 13
	&UIntType,
	//TSDB_DATA_TYPE_UBIGINT    = 14
	&UBigIntType,
	//TSDB_DATA_TYPE_JSON       = 15
	&JsonType,
	//TSDB_DATA_TYPE_VARBINARY  = 16
	&VarBinaryType,
	//TSDB_DATA_TYPE_DECIMAL    = 17
	nil,
	//TSDB_DATA_TYPE_BLOB       = 18
	nil,
	//TSDB_DATA_TYPE_MEDIUMBLOB = 19
	nil,
	//TSDB_DATA_TYPE_GEOMETRY   = 20
	&GeometryType,
}

const (
	TSDB_DATA_TYPE_NULL       = 0  // 1 bytes
	TSDB_DATA_TYPE_BOOL       = 1  // 1 bytes
	TSDB_DATA_TYPE_TINYINT    = 2  // 1 byte
	TSDB_DATA_TYPE_SMALLINT   = 3  // 2 bytes
	TSDB_DATA_TYPE_INT        = 4  // 4 bytes
	TSDB_DATA_TYPE_BIGINT     = 5  // 8 bytes
	TSDB_DATA_TYPE_FLOAT      = 6  // 4 bytes
	TSDB_DATA_TYPE_DOUBLE     = 7  // 8 bytes
	TSDB_DATA_TYPE_BINARY     = 8  // string
	TSDB_DATA_TYPE_TIMESTAMP  = 9  // 8 bytes
	TSDB_DATA_TYPE_NCHAR      = 10 // unicode string
	TSDB_DATA_TYPE_UTINYINT   = 11 // 1 byte
	TSDB_DATA_TYPE_USMALLINT  = 12 // 2 bytes
	TSDB_DATA_TYPE_UINT       = 13 // 4 bytes
	TSDB_DATA_TYPE_UBIGINT    = 14 // 8 bytes
	TSDB_DATA_TYPE_JSON       = 15
	TSDB_DATA_TYPE_VARBINARY  = 16
	TSDB_DATA_TYPE_DECIMAL    = 17
	TSDB_DATA_TYPE_BLOB       = 18
	TSDB_DATA_TYPE_MEDIUMBLOB = 19
	TSDB_DATA_TYPE_GEOMETRY   = 20
)

const (
	TSDB_DATA_TYPE_NULL_Str      = "NULL"
	TSDB_DATA_TYPE_BOOL_Str      = "BOOL"
	TSDB_DATA_TYPE_TINYINT_Str   = "TINYINT"
	TSDB_DATA_TYPE_SMALLINT_Str  = "SMALLINT"
	TSDB_DATA_TYPE_INT_Str       = "INT"
	TSDB_DATA_TYPE_BIGINT_Str    = "BIGINT"
	TSDB_DATA_TYPE_FLOAT_Str     = "FLOAT"
	TSDB_DATA_TYPE_DOUBLE_Str    = "DOUBLE"
	TSDB_DATA_TYPE_BINARY_Str    = "VARCHAR"
	TSDB_DATA_TYPE_TIMESTAMP_Str = "TIMESTAMP"
	TSDB_DATA_TYPE_NCHAR_Str     = "NCHAR"
	TSDB_DATA_TYPE_UTINYINT_Str  = "TINYINT UNSIGNED"
	TSDB_DATA_TYPE_USMALLINT_Str = "SMALLINT UNSIGNED"
	TSDB_DATA_TYPE_UINT_Str      = "INT UNSIGNED"
	TSDB_DATA_TYPE_UBIGINT_Str   = "BIGINT UNSIGNED"
	TSDB_DATA_TYPE_JSON_Str      = "JSON"
	TSDB_DATA_TYPE_VARBINARY_Str = "VARBINARY"
	TSDB_DATA_TYPE_GEOMETRY_Str  = "GEOMETRY"
)

var TypeNameMap = map[int]string{
	TSDB_DATA_TYPE_NULL:      TSDB_DATA_TYPE_NULL_Str,
	TSDB_DATA_TYPE_BOOL:      TSDB_DATA_TYPE_BOOL_Str,
	TSDB_DATA_TYPE_TINYINT:   TSDB_DATA_TYPE_TINYINT_Str,
	TSDB_DATA_TYPE_SMALLINT:  TSDB_DATA_TYPE_SMALLINT_Str,
	TSDB_DATA_TYPE_INT:       TSDB_DATA_TYPE_INT_Str,
	TSDB_DATA_TYPE_BIGINT:    TSDB_DATA_TYPE_BIGINT_Str,
	TSDB_DATA_TYPE_FLOAT:     TSDB_DATA_TYPE_FLOAT_Str,
	TSDB_DATA_TYPE_DOUBLE:    TSDB_DATA_TYPE_DOUBLE_Str,
	TSDB_DATA_TYPE_BINARY:    TSDB_DATA_TYPE_BINARY_Str,
	TSDB_DATA_TYPE_TIMESTAMP: TSDB_DATA_TYPE_TIMESTAMP_Str,
	TSDB_DATA_TYPE_NCHAR:     TSDB_DATA_TYPE_NCHAR_Str,
	TSDB_DATA_TYPE_UTINYINT:  TSDB_DATA_TYPE_UTINYINT_Str,
	TSDB_DATA_TYPE_USMALLINT: TSDB_DATA_TYPE_USMALLINT_Str,
	TSDB_DATA_TYPE_UINT:      TSDB_DATA_TYPE_UINT_Str,
	TSDB_DATA_TYPE_UBIGINT:   TSDB_DATA_TYPE_UBIGINT_Str,
	TSDB_DATA_TYPE_JSON:      TSDB_DATA_TYPE_JSON_Str,
	TSDB_DATA_TYPE_VARBINARY: TSDB_DATA_TYPE_VARBINARY_Str,
	TSDB_DATA_TYPE_GEOMETRY:  TSDB_DATA_TYPE_GEOMETRY_Str,
}

var NameTypeMap = map[string]int{
	TSDB_DATA_TYPE_NULL_Str:      TSDB_DATA_TYPE_NULL,
	TSDB_DATA_TYPE_BOOL_Str:      TSDB_DATA_TYPE_BOOL,
	TSDB_DATA_TYPE_TINYINT_Str:   TSDB_DATA_TYPE_TINYINT,
	TSDB_DATA_TYPE_SMALLINT_Str:  TSDB_DATA_TYPE_SMALLINT,
	TSDB_DATA_TYPE_INT_Str:       TSDB_DATA_TYPE_INT,
	TSDB_DATA_TYPE_BIGINT_Str:    TSDB_DATA_TYPE_BIGINT,
	TSDB_DATA_TYPE_FLOAT_Str:     TSDB_DATA_TYPE_FLOAT,
	TSDB_DATA_TYPE_DOUBLE_Str:    TSDB_DATA_TYPE_DOUBLE,
	TSDB_DATA_TYPE_BINARY_Str:    TSDB_DATA_TYPE_BINARY,
	TSDB_DATA_TYPE_TIMESTAMP_Str: TSDB_DATA_TYPE_TIMESTAMP,
	TSDB_DATA_TYPE_NCHAR_Str:     TSDB_DATA_TYPE_NCHAR,
	TSDB_DATA_TYPE_UTINYINT_Str:  TSDB_DATA_TYPE_UTINYINT,
	TSDB_DATA_TYPE_USMALLINT_Str: TSDB_DATA_TYPE_USMALLINT,
	TSDB_DATA_TYPE_UINT_Str:      TSDB_DATA_TYPE_UINT,
	TSDB_DATA_TYPE_UBIGINT_Str:   TSDB_DATA_TYPE_UBIGINT,
	TSDB_DATA_TYPE_JSON_Str:      TSDB_DATA_TYPE_JSON,
	TSDB_DATA_TYPE_VARBINARY_Str: TSDB_DATA_TYPE_VARBINARY,
	TSDB_DATA_TYPE_GEOMETRY_Str:  TSDB_DATA_TYPE_GEOMETRY,
}

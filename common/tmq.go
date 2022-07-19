package common

type Meta struct {
	Type      string `json:"type"`
	TableName string `json:"tableName"`
	TableType string `json:"tableType"`
	Columns   []struct {
		Name   string `json:"name"`
		Type   int    `json:"type"`
		Length int    `json:"length"`
	} `json:"columns"`
	Using string `json:"using"`
	Tags  []struct {
		Name  string      `json:"name"`
		Type  int         `json:"type"`
		Value interface{} `json:"value"`
	} `json:"tags"`
	TableNameList []string `json:"tableNameList"`
	AlterType     int      `json:"alterType"`
	ColName       string   `json:"colName"`
	ColNewName    string   `json:"colNewName"`
	ColType       int      `json:"colType"`
	ColLength     int      `json:"colLength"`
	ColValue      string   `json:"colValue"`
	ColValueNull  bool     `json:"colValueNull"`
}

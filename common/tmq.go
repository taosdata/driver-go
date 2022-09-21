package common

type Meta struct {
	Type          string        `json:"type"`
	TableName     string        `json:"tableName"`
	TableType     string        `json:"tableType"`
	CreateList    []*CreateItem `json:"createList"`
	Columns       []*Column     `json:"columns"`
	Using         string        `json:"using"`
	TagNum        int           `json:"tagNum"`
	Tags          []*Tag        `json:"tags"`
	TableNameList []string      `json:"tableNameList"`
	AlterType     int           `json:"alterType"`
	ColName       string        `json:"colName"`
	ColNewName    string        `json:"colNewName"`
	ColType       int           `json:"colType"`
	ColLength     int           `json:"colLength"`
	ColValue      string        `json:"colValue"`
	ColValueNull  bool          `json:"colValueNull"`
}

type Tag struct {
	Name  string      `json:"name"`
	Type  int         `json:"type"`
	Value interface{} `json:"value"`
}

type Column struct {
	Name   string `json:"name"`
	Type   int    `json:"type"`
	Length int    `json:"length"`
}

type CreateItem struct {
	TableName string `json:"tableName"`
	Using     string `json:"using"`
	TagNum    int    `json:"tagNum"`
	Tags      []*Tag `json:"tags"`
}

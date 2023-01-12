package tmq

import (
	"encoding/json"
	"testing"
)

const createJson = `{
    "type": "create",
    "tableName": "t1",
    "tableType": "super", 
    "columns": [ 
        {
            "name": "c1", 
            "type": 0, 
            "length": 0 
        },
        {
            "name": "c2",
            "type": 8,
            "length": 8
        }
    ],
    "tags": [
        {
            "name": "t1",
            "type": 0,
            "length": 0
        },
        {
            "name": "t2",
            "type": 8,
            "length": 8
        }
    ]
}`
const dropJson = `{
  "type":"drop",            
  "tableName":"t1",         
  "tableType":"super",      
  "tableNameList":["t1", "t2"]
}`

func TestCreateJson(t *testing.T) {
	var obj Meta
	err := json.Unmarshal([]byte(createJson), &obj)
	if err != nil {
		t.Log(err)
		return
	}
	t.Log(obj)
}

func TestDropJson(t *testing.T) {
	var obj Meta
	err := json.Unmarshal([]byte(dropJson), &obj)
	if err != nil {
		t.Log(err)
		return
	}
	t.Log(obj)
}

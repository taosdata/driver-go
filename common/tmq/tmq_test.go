package tmq

import (
	"encoding/json"
	"errors"
	"reflect"
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

// @author: xftan
// @date: 2023/10/13 11:19
// @description: test json
func TestCreateJson(t *testing.T) {
	var obj Meta
	err := json.Unmarshal([]byte(createJson), &obj)
	if err != nil {
		t.Log(err)
		return
	}
	t.Log(obj)
}

// @author: xftan
// @date: 2023/10/13 11:19
// @description: test drop json
func TestDropJson(t *testing.T) {
	var obj Meta
	err := json.Unmarshal([]byte(dropJson), &obj)
	if err != nil {
		t.Log(err)
		return
	}
	t.Log(obj)
}

const createBaseOnJson = `{
    "type": "create",
    "tableName": "vst_child",
    "tableType": "super",
    "columns": [
        {"name": "ts", "type": 9, "length": 0},
        {"name": "own_c", "type": 4, "length": 0}
    ],
    "tags": [
        {"name": "own_t", "type": 4, "length": 0}
    ],
    "baseOn": ["vst_parent_a", "vst_parent_b"],
    "ownColStart": 2,
    "ownTagStart": 1
}`

const alterBaseOnJson = `{
    "type": "alter",
    "tableName": "vst_child",
    "tableType": "super",
    "alterType": 22,
    "baseOn": ["vst_parent_a"]
}`

// VST inheritance (BASE ON) fields must deserialize into Meta.
func TestCreateBaseOnJson(t *testing.T) {
	var obj Meta
	if err := json.Unmarshal([]byte(createBaseOnJson), &obj); err != nil {
		t.Fatalf("unmarshal create base-on meta: %v", err)
	}
	if !reflect.DeepEqual(obj.BaseOn, []string{"vst_parent_a", "vst_parent_b"}) {
		t.Fatalf("BaseOn = %v, want [vst_parent_a vst_parent_b]", obj.BaseOn)
	}
	if obj.OwnColStart != 2 {
		t.Fatalf("OwnColStart = %d, want 2", obj.OwnColStart)
	}
	if obj.OwnTagStart != 1 {
		t.Fatalf("OwnTagStart = %d, want 1", obj.OwnTagStart)
	}
}

// A non-inherited create meta leaves BASE ON fields at their zero values.
func TestCreateJsonNoBaseOn(t *testing.T) {
	var obj Meta
	if err := json.Unmarshal([]byte(createJson), &obj); err != nil {
		t.Fatalf("unmarshal create meta: %v", err)
	}
	if obj.BaseOn != nil {
		t.Fatalf("BaseOn = %v, want nil for non-inherited table", obj.BaseOn)
	}
}

// alterType 22/23 (ADD/DROP BASE ON) deserialize with their parent list.
func TestAlterBaseOnJson(t *testing.T) {
	var obj Meta
	if err := json.Unmarshal([]byte(alterBaseOnJson), &obj); err != nil {
		t.Fatalf("unmarshal alter base-on meta: %v", err)
	}
	if obj.AlterType != 22 {
		t.Fatalf("AlterType = %d, want 22", obj.AlterType)
	}
	if !reflect.DeepEqual(obj.BaseOn, []string{"vst_parent_a"}) {
		t.Fatalf("BaseOn = %v, want [vst_parent_a]", obj.BaseOn)
	}
}

func TestOffset_String(t *testing.T) {
	tests := []struct {
		name string
		o    Offset
		want string
	}{
		{
			name: "Valid Offset",
			o:    100,
			want: "100",
		},
		{
			name: "Invalid Offset",
			o:    OffsetInvalid,
			want: "unset",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.o.String(); got != tt.want {
				t.Errorf("Offset.String() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestOffset_Valid(t *testing.T) {
	tests := []struct {
		name string
		o    Offset
		want bool
	}{
		{
			name: "Valid Offset",
			o:    100,
			want: true,
		},
		{
			name: "Invalid Offset",
			o:    OffsetInvalid,
			want: true,
		},
		{
			name: "Negative Offset",
			o:    -100,
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.o.Valid(); got != tt.want {
				t.Errorf("Offset.Valid() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTopicPartition_String(t *testing.T) {
	tests := []struct {
		name string
		tp   TopicPartition
		want string
	}{
		{
			name: "With Error",
			tp: TopicPartition{
				Topic:     stringPtr("test-topic"),
				Partition: 0,
				Offset:    100,
				Error:     errors.New("error message"),
			},
			want: "test-topic[0]@100(error message)",
		},
		{
			name: "Without Error",
			tp: TopicPartition{
				Topic:     stringPtr("test-topic"),
				Partition: 0,
				Offset:    100,
			},
			want: "test-topic[0]@100",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.tp.String(); got != tt.want {
				t.Errorf("TopicPartition.String() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAssignment_MarshalJSON(t *testing.T) {
	tests := []struct {
		name string
		a    Assignment
		want string
	}{
		{
			name: "Marshal Assignment",
			a: Assignment{
				VGroupID: 1,
				Offset:   100,
				Begin:    50,
				End:      150,
			},
			want: `{"vgroup_id":1,"offset":100,"begin":50,"end":150}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := json.Marshal(tt.a)
			if err != nil {
				t.Errorf("MarshalJSON error: %v", err)
				return
			}
			if !reflect.DeepEqual(string(got), tt.want) {
				t.Errorf("MarshalJSON = %v, want %v", string(got), tt.want)
			}
		})
	}
}

func stringPtr(s string) *string {
	return &s
}

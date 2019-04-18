package query

import (
	"encoding/json"
	"fmt"
	"strings"
)

// Value - represents any value of any value type.
type Value struct {
	value     interface{}
	valueType Type
}

// IsNull - returns whether value is null or not.
func (value *Value) IsNull() bool {
	return value == nil
}

// String - represents value as string.
func (value *Value) String() string {
	if value.value == nil {
		if value.valueType == Null {
			return "NULL"
		}

		return "<nil>"
	}

	switch value.valueType {
	case String:
		return fmt.Sprintf("%v", value.value)
	case Array:
		var valueStrings []string
		for _, v := range value.value.([]*Value) {
			valueStrings = append(valueStrings, fmt.Sprintf("%v", v))
		}
		return fmt.Sprintf("[%v]", strings.Join(valueStrings, ","))
	}

	return fmt.Sprintf("%v", value.value)
}

// MarshalJSON - encodes to JSON data.
func (value *Value) MarshalJSON() ([]byte, error) {
	return json.Marshal(value.value)
}

// BoolValue - returns underlying bool value. It panics if value is not Bool type.
func (value *Value) BoolValue() bool {
	if value.valueType == Bool {
		return value.value.(bool)
	}

	panic(fmt.Sprintf("requested bool value but found %T type", value.value))
}

// IntValue - returns underlying int value. It panics if value is not Int type.
func (value *Value) IntValue() int64 {
	if value.valueType == Int {
		return value.value.(int64)
	}

	panic(fmt.Sprintf("requested int value but found %T type", value.value))
}

// FloatValue - returns underlying int/float value as float64. It panics if value is not Int/Float type.
func (value *Value) FloatValue() float64 {
	switch value.valueType {
	case Int:
		return float64(value.value.(int64))
	case Float:
		return value.value.(float64)
	}

	panic(fmt.Sprintf("requested float value but found %T type", value.value))
}

// StringValue - returns underlying string value. It panics if value is not String type.
func (value *Value) StringValue() string {
	if value.valueType == String {
		return value.value.(string)
	}

	panic(fmt.Sprintf("requested string value but found %T type", value.value))
}

// ArrayValue - returns underlying value array. It panics if value is not Array type.
func (value *Value) ArrayValue() []*Value {
	if value.valueType == Array {
		return value.value.([]*Value)
	}

	panic(fmt.Sprintf("requested array value but found %T type", value.value))
}

// Type - returns value type.
func (value *Value) Type() Type {
	return value.valueType
}

// Value - returns underneath value interface.
func (value *Value) Value() interface{} {
	return value.value
}

// NewNull - creates new null value.
func NewNull() *Value {
	return &Value{nil, Null}
}

// NewBool - creates new Bool value of b.
func NewBool(b bool) *Value {
	return &Value{b, Bool}
}

// NewInt - creates new Int value of i.
func NewInt(i int64) *Value {
	return &Value{i, Int}
}

// NewFloat - creates new Float value of f.
func NewFloat(f float64) *Value {
	return &Value{f, Float}
}

// NewString - creates new Sring value of s.
func NewString(s string) *Value {
	return &Value{s, String}
}

// NewArray - creates new Array value of values.
func NewArray(values []*Value) *Value {
	return &Value{values, Array}
}

/*
 * Minio Cloud Storage, (C) 2019 Minio, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package parquet

import (
	"fmt"
	"reflect"
	"strings"
	"unicode"
	"unsafe"

	"github.com/minio/parquet-go/gen-go/parquet"
)

func getValue(t reflect.Type, v reflect.Value) (interface{}, *parquet.Type, *parquet.ConvertedType) {
	if !v.CanInterface() {
		return nil, nil, nil
	}

	switch t.Kind() {
	case reflect.Bool:
		var rv *bool
		if v.Kind() == reflect.Bool {
			b := v.Interface().(bool)
			rv = &b
		}
		return rv, parquet.TypePtr(parquet.Type_BOOLEAN), nil

	case reflect.Int:
		if *intParquetType == parquet.Type_INT32 {
			var rv *int32
			if v.Kind() == reflect.Int {
				i32 := int32(v.Interface().(int))
				rv = &i32
			}
			return rv, intParquetType, parquet.ConvertedTypePtr(parquet.ConvertedType_INT_32)
		}

		var rv *int64
		if v.Kind() == reflect.Int {
			i64 := int64(v.Interface().(int))
			rv = &i64
		}
		return rv, intParquetType, parquet.ConvertedTypePtr(parquet.ConvertedType_INT_64)

	case reflect.Int8:
		var rv *int32
		if v.Kind() == reflect.Int8 {
			i32 := int32(v.Interface().(int8))
			rv = &i32
		}
		return rv, parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_INT_8)

	case reflect.Int16:
		var rv *int32
		if v.Kind() == reflect.Int16 {
			i32 := int32(v.Interface().(int16))
			rv = &i32
		}
		return rv, parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_INT_16)

	case reflect.Int32:
		var rv *int32
		if v.Kind() == reflect.Int32 {
			i32 := v.Interface().(int32)
			rv = &i32
		}
		return rv, parquet.TypePtr(parquet.Type_INT32), nil

	case reflect.Int64:
		var rv *int64
		if v.Kind() == reflect.Int64 {
			i64 := v.Interface().(int64)
			rv = &i64
		}
		return rv, parquet.TypePtr(parquet.Type_INT64), nil

	case reflect.Uint:
		if *intParquetType == parquet.Type_INT32 {
			var rv *int32
			if v.Kind() == reflect.Uint {
				i32 := int32(v.Interface().(uint))
				rv = &i32
			}
			return rv, intParquetType, parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_32)
		}

		var rv *int64
		if v.Kind() == reflect.Uint {
			i64 := int64(v.Interface().(uint))
			rv = &i64
		}
		return rv, intParquetType, parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_64)

	case reflect.Uint8:
		var rv *int32
		if v.Kind() == reflect.Uint8 {
			i32 := int32(v.Interface().(uint8))
			rv = &i32
		}
		return rv, parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_8)

	case reflect.Uint16:
		var rv *int32
		if v.Kind() == reflect.Uint16 {
			i32 := int32(v.Interface().(uint16))
			rv = &i32
		}
		return rv, parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_16)

	case reflect.Uint32:
		var rv *int32
		if v.Kind() == reflect.Uint32 {
			i32 := int32(v.Interface().(uint32))
			rv = &i32
		}
		return rv, parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_32)

	case reflect.Uint64:
		var rv *int64
		if v.Kind() == reflect.Uint64 {
			i64 := int64(v.Interface().(uint64))
			rv = &i64
		}
		return rv, parquet.TypePtr(parquet.Type_INT64), parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_64)

	case reflect.Float32:
		var rv *float32
		if v.Kind() == reflect.Float32 {
			f32 := v.Interface().(float32)
			rv = &f32
		}
		return rv, parquet.TypePtr(parquet.Type_FLOAT), nil

	case reflect.Float64:
		var rv *float64
		if v.Kind() == reflect.Float64 {
			f64 := v.Interface().(float64)
			rv = &f64
		}
		return rv, parquet.TypePtr(parquet.Type_DOUBLE), nil

	case reflect.Array:
		// Only byte arrays are supported.
		if t.Elem().Kind() != reflect.Uint8 {
			return nil, nil, nil
		}

		var slice []uint8
		if v.Kind() == reflect.Array {
			slice = make([]uint8, v.Len())
			reflect.Copy(reflect.ValueOf(slice), v)
		}

		return &slice, parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY), nil

	case reflect.Slice:
		// Only byte slices are supported.
		if t.Elem().Kind() != reflect.Uint8 {
			return nil, nil, nil
		}

		rv := v.Interface()
		if v.Kind() == reflect.Slice {
			bytes := v.Bytes()
			rv = &bytes
		}

		return rv, parquet.TypePtr(parquet.Type_BYTE_ARRAY), nil

	case reflect.String:
		var array []byte
		if v.Kind() == reflect.String {
			array = []byte(v.Interface().(string))
		}
		return &array, parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_UTF8)

	case reflect.Ptr:
		t = t.Elem()

		if v.IsNil() {
			v = reflect.NewAt(t, nil)
		} else {
			v = v.Elem()
		}

		return getValue(t, v)
	}

	return nil, nil, nil
}

func getParquetValue(i interface{}) (interface{}, *parquet.Type, *parquet.ConvertedType) {
	return getValue(reflect.TypeOf(i), reflect.ValueOf(i))
}

type Tag struct {
	Name               string
	Encoding           *parquet.Encoding
	ValueEncoding      *parquet.Encoding
	Repetition         *parquet.FieldRepetitionType
	ValueRepetition    *parquet.FieldRepetitionType
	Compression        *parquet.CompressionCodec
	ValueCompression   *parquet.CompressionCodec
	Type               *parquet.Type
	ConvertedType      *parquet.ConvertedType
	ValueType          *parquet.Type
	ValueConvertedType *parquet.ConvertedType
	NameTagMap         map[string]*Tag
}

// supported tag format is `parquet:"<KEY>=<VALUE>[,<KEY>=<VALUE>,...]"`
// supported keys are
//   - name
//   - encoding
//   - valueEncoding
//   - repetition
//   - valueRepetition
//   - compression
//   - valueCompression
//   - type
//   - valueType
//   - convertedType
//   - valueConvertedType
func structFieldToTag(field reflect.StructField) (*Tag, error) {
	tag := &Tag{
		Name:             field.Name,
		Encoding:         parquet.EncodingPtr(parquet.Encoding_PLAIN),
		ValueEncoding:    parquet.EncodingPtr(parquet.Encoding_PLAIN),
		Repetition:       parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_REQUIRED),
		ValueRepetition:  parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_REQUIRED),
		Compression:      parquet.CompressionCodecPtr(parquet.CompressionCodec_SNAPPY),
		ValueCompression: parquet.CompressionCodecPtr(parquet.CompressionCodec_SNAPPY),
	}

	if field.Type.Kind() == reflect.Ptr {
		tag.Repetition = parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_OPTIONAL)
		tag.ValueRepetition = parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_OPTIONAL)
	}

	tagString, ok := field.Tag.Lookup("parquet")
	if !ok {
		return tag, nil
	}

	tagFields := strings.Split(tagString, ",")
	for _, tagField := range tagFields {
		tokens := strings.SplitN(tagField, "=", 2)
		if len(tokens) != 2 {
			return nil, fmt.Errorf("struct field %v: invalid parquet tag key/value %v", field.Name, tagField)
		}

		key := strings.TrimSpace(tokens[0])
		value := strings.TrimSpace(tokens[1])

		switch key {
		case "name":
			if value == "" {
				return nil, fmt.Errorf("struct field %v: invalid name value %v in parquet tag", field.Name, value)
			}

			if value == "-" {
				return nil, nil
			}

			tag.Name = value

		case "encoding":
			encoding, err := parquet.EncodingFromString(value)
			if err != nil {
				return nil, fmt.Errorf("struct field %v: invalid encoding value %v in parquet tag", field.Name, value)
			}
			tag.Encoding = &encoding

		case "valueEncoding":
			valueEncoding, err := parquet.EncodingFromString(value)
			if err != nil {
				return nil, fmt.Errorf("struct field %v: invalid valueEncoding value %v in parquet tag", field.Name, value)
			}
			tag.ValueEncoding = &valueEncoding

		case "repetition":
			repetition, err := parquet.FieldRepetitionTypeFromString(value)
			if err != nil {
				return nil, fmt.Errorf("struct field %v: invalid repetition value %v in parquet tag", field.Name, value)
			}
			tag.Repetition = &repetition

		case "valueRepetition":
			valueRepetition, err := parquet.FieldRepetitionTypeFromString(value)
			if err != nil {
				return nil, fmt.Errorf("struct field %v: invalid valueRepetition value %v in parquet tag", field.Name, value)
			}
			tag.ValueRepetition = &valueRepetition

		case "compression":
			compression, err := parquet.CompressionCodecFromString(value)
			if err != nil {
				return nil, fmt.Errorf("struct field %v: invalid compression value %v in parquet tag", field.Name, value)
			}
			tag.Compression = &compression

		case "valueCompression":
			valueCompression, err := parquet.CompressionCodecFromString(value)
			if err != nil {
				return nil, fmt.Errorf("struct field %v: invalid valueCompression value %v in parquet tag", field.Name, value)
			}
			tag.ValueCompression = &valueCompression

		case "type":
			dataType, err := parquet.TypeFromString(value)
			if err != nil {
				return nil, fmt.Errorf("struct field %v: invalid type value %v in parquet tag", field.Name, value)
			}
			tag.Type = &dataType

		case "valueType":
			valueType, err := parquet.TypeFromString(value)
			if err != nil {
				return nil, fmt.Errorf("struct field %v: invalid valueType value %v in parquet tag", field.Name, value)
			}
			tag.ValueType = &valueType

		case "convertedType":
			convertedType, err := parquet.ConvertedTypeFromString(value)
			if err != nil {
				return nil, fmt.Errorf("struct field %v: invalid convertedType value %v in parquet tag", field.Name, value)
			}
			tag.ConvertedType = &convertedType

		case "valueConvertedType":
			valueConvertedType, err := parquet.ConvertedTypeFromString(value)
			if err != nil {
				return nil, fmt.Errorf("struct field %v: invalid valueConvertedType value %v in parquet tag", field.Name, value)
			}
			tag.ValueConvertedType = &valueConvertedType

		default:
			return nil, fmt.Errorf("struct field %v: invalid parquet tag key %v", field.Name, key)
		}
	}

	return tag, nil
}

const intSize = unsafe.Sizeof(int(0))

var intParquetType *parquet.Type

func init() {
	switch {
	case intSize <= 4:
		intParquetType = parquet.TypePtr(parquet.Type_INT32)
	case intSize <= 8:
		intParquetType = parquet.TypePtr(parquet.Type_INT64)
	default:
		panic(fmt.Errorf("odd int size %v to convert to parquet type", intSize))
	}
}

func fillPrimitiveType(t reflect.Type, tag *Tag) *Tag {
	if tag == nil {
		return nil
	}

	switch t.Kind() {
	case reflect.Bool:
		tag.Type = parquet.TypePtr(parquet.Type_BOOLEAN)

	case reflect.Int:
		tag.Type = intParquetType
		tag.ConvertedType = parquet.ConvertedTypePtr(parquet.ConvertedType_INT_64)
		if *tag.Type == parquet.Type_INT32 {
			tag.ConvertedType = parquet.ConvertedTypePtr(parquet.ConvertedType_INT_32)
		}

	case reflect.Int8:
		tag.Type = parquet.TypePtr(parquet.Type_INT32)
		tag.ConvertedType = parquet.ConvertedTypePtr(parquet.ConvertedType_INT_8)

	case reflect.Int16:
		tag.Type = parquet.TypePtr(parquet.Type_INT32)
		tag.ConvertedType = parquet.ConvertedTypePtr(parquet.ConvertedType_INT_16)

	case reflect.Int32:
		tag.Type = parquet.TypePtr(parquet.Type_INT32)

	case reflect.Int64:
		tag.Type = parquet.TypePtr(parquet.Type_INT64)

	case reflect.Uint:
		tag.Type = intParquetType
		tag.ConvertedType = parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_64)
		if *tag.Type == parquet.Type_INT32 {
			tag.ConvertedType = parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_32)
		}

	case reflect.Uint8:
		tag.Type = parquet.TypePtr(parquet.Type_INT32)
		tag.ConvertedType = parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_8)

	case reflect.Uint16:
		tag.Type = parquet.TypePtr(parquet.Type_INT32)
		tag.ConvertedType = parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_16)

	case reflect.Uint32:
		tag.Type = parquet.TypePtr(parquet.Type_INT32)
		tag.ConvertedType = parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_32)

	case reflect.Uint64:
		tag.Type = parquet.TypePtr(parquet.Type_INT64)
		tag.ConvertedType = parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_64)

	case reflect.Float32:
		tag.Type = parquet.TypePtr(parquet.Type_FLOAT)

	case reflect.Float64:
		tag.Type = parquet.TypePtr(parquet.Type_DOUBLE)

	case reflect.Array:
		// Only byte arrays are supported.
		if t.Elem().Kind() == reflect.Uint8 {
			tag.Type = parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY)
		} else {
			tag = nil
		}

	case reflect.Ptr:
		return fillPrimitiveType(t.Elem(), tag)

	case reflect.Slice:
		// Only byte slices are supported.
		if t.Elem().Kind() == reflect.Uint8 {
			tag.Type = parquet.TypePtr(parquet.Type_BYTE_ARRAY)
		} else {
			tag = nil
		}

	case reflect.String:
		tag.Type = parquet.TypePtr(parquet.Type_BYTE_ARRAY)
		tag.ConvertedType = parquet.ConvertedTypePtr(parquet.ConvertedType_UTF8)

	default:
		tag = nil
	}

	return tag
}

func structToTagMap(t reflect.Type, structTag *Tag) (map[string]*Tag, error) {
	if t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("%v is not %v", t.Kind(), reflect.Struct)
	}

	tagMap := make(map[string]*Tag)
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if unicode.IsLower([]rune(field.Name)[0]) {
			continue
		}

		tag, err := structFieldToTag(field)
		if err != nil {
			return nil, err
		}

		fieldType := field.Type
		for fieldType.Kind() == reflect.Ptr {
			fieldType = fieldType.Elem()
		}

		if fillPrimitiveType(fieldType, tag) == nil {
			var err error
			switch fieldType.Kind() {
			case reflect.Struct:
				_, err = structToTagMap(fieldType, tag)
			case reflect.Map:
				err = mapToTagMap(fieldType, tag)
			default:
				err = fmt.Errorf("struct field %v: unsupported field type %v", field.Name, field.Type)
			}
			if err != nil {
				return nil, err
			}
		}

		tagMap[field.Name] = tag
	}

	if structTag != nil {
		structTag.NameTagMap = tagMap
	}

	return tagMap, nil
}

func mapToTagMap(t reflect.Type, mapTag *Tag) error {
	if t.Kind() != reflect.Map {
		return fmt.Errorf("%v is not %v", t.Kind(), reflect.Map)
	}

	if mapTag == nil {
		return fmt.Errorf("nil map tag")
	}

	keyValueTag := &Tag{
		Name:             "key_value",
		Encoding:         mapTag.Encoding,
		ValueEncoding:    mapTag.ValueEncoding,
		Repetition:       parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_REPEATED),
		ValueRepetition:  parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_REPEATED),
		Compression:      mapTag.Compression,
		ValueCompression: mapTag.ValueCompression,
		ConvertedType:    parquet.ConvertedTypePtr(parquet.ConvertedType_MAP_KEY_VALUE),
		NameTagMap:       make(map[string]*Tag),
	}

	keyType := t.Key()
	for keyType.Kind() == reflect.Ptr {
		keyType = keyType.Elem()
	}
	keyTag := &Tag{
		Name:             "key",
		Encoding:         mapTag.Encoding,
		ValueEncoding:    mapTag.ValueEncoding,
		Repetition:       mapTag.Repetition,
		ValueRepetition:  mapTag.ValueRepetition,
		Compression:      mapTag.Compression,
		ValueCompression: mapTag.ValueCompression,
	}
	if t.Key().Kind() == reflect.Ptr {
		keyTag.Repetition = parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_OPTIONAL)
		keyTag.ValueRepetition = parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_OPTIONAL)
	}
	if fillPrimitiveType(keyType, keyTag) == nil {
		var err error
		switch keyType.Kind() {
		case reflect.Struct:
			_, err = structToTagMap(keyType, keyTag)
		case reflect.Map:
			err = mapToTagMap(keyType, keyTag)
		default:
			err = fmt.Errorf("unsupported map key %v", t.Key())
		}
		if err != nil {
			return err
		}
	}
	keyValueTag.NameTagMap["key"] = keyTag

	valueType := t.Elem()
	for valueType.Kind() == reflect.Ptr {
		valueType = valueType.Elem()
	}
	valueTag := &Tag{
		Name:             "value",
		Encoding:         mapTag.Encoding,
		ValueEncoding:    mapTag.ValueEncoding,
		Repetition:       mapTag.Repetition,
		ValueRepetition:  mapTag.ValueRepetition,
		Compression:      mapTag.Compression,
		ValueCompression: mapTag.ValueCompression,
	}
	if t.Elem().Kind() == reflect.Ptr {
		valueTag.Repetition = parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_OPTIONAL)
		valueTag.ValueRepetition = parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_OPTIONAL)
	}
	if fillPrimitiveType(valueType, valueTag) == nil {
		var err error
		switch valueType.Kind() {
		case reflect.Struct:
			_, err = structToTagMap(valueType, valueTag)
		case reflect.Map:
			err = mapToTagMap(valueType, valueTag)
		default:
			err = fmt.Errorf("unsupported map value %v", t.Elem())
		}
		if err != nil {
			return err
		}
	}
	keyValueTag.NameTagMap["value"] = valueTag

	mapTag.ConvertedType = parquet.ConvertedTypePtr(parquet.ConvertedType_MAP)
	if mapTag.NameTagMap == nil {
		mapTag.NameTagMap = make(map[string]*Tag)
	}

	mapTag.NameTagMap["key_value"] = keyValueTag
	return nil
}

func sliceToTagMap(t reflect.Type, sliceTag *Tag) error {
	if t.Kind() != reflect.Slice {
		return fmt.Errorf("%v is not %v", t.Kind(), reflect.Slice)
	}

	if sliceTag == nil {
		return fmt.Errorf("nil slice tag")
	}

	elementTag := &Tag{
		Name:               "element",
		Encoding:           sliceTag.Encoding,
		ValueEncoding:      sliceTag.ValueEncoding,
		Repetition:         sliceTag.Repetition,
		ValueRepetition:    sliceTag.ValueRepetition,
		Compression:        sliceTag.Compression,
		ValueCompression:   sliceTag.ValueCompression,
		Type:               sliceTag.Type,
		ConvertedType:      sliceTag.ConvertedType,
		ValueType:          sliceTag.ValueType,
		ValueConvertedType: sliceTag.ValueConvertedType,
	}
	elementType = t.Elem()
	if elementType.Kind() == reflect.Ptr {
		elementTag.Repetition = parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_OPTIONAL)
		elementTag.ValueRepetition = parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_OPTIONAL)
	} else {
		elementTag.Repetition = parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_REQUIRED)
		elementTag.ValueRepetition = parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_REQUIRED)
	}

	if fillPrimitiveType(valueType, valueTag) == nil {
		var err error
		switch valueType.Kind() {
		case reflect.Struct:
			_, err = structToTagMap(valueType, valueTag)
		case reflect.Map:
			err = mapToTagMap(valueType, valueTag)
		default:
			err = fmt.Errorf("unsupported map value %v", t.Elem())
		}
		if err != nil {
			return err
		}
	}
	keyValueTag.NameTagMap["value"] = valueTag
}

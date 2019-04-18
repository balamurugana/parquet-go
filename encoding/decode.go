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

package encoding

import (
	"bytes"
	"fmt"

	"github.com/minio/parquet-go/gen-go/parquet"
)

func Decode(bytesReader *bytes.Reader, encoding parquet.Encoding, parquetType parquet.Type, count, bitWidth uint64) (result interface{}, err error) {
	switch encoding {
	case parquet.Encoding_PLAIN:
		return PlainDecode(bytesReader, parquetType, count, bitWidth)

	case parquet.Encoding_PLAIN_DICTIONARY, parquet.Encoding_RLE:
		switch parquetType {
		case parquet.Type_INT32, parquet.Type_INT64:
		default:
			panic(fmt.Errorf("unsupported parquet type %v for plain dictionary encoding", parquetType))
		}

		length := uint64(0)
		if encoding == parquet.Encoding_PLAIN_DICTIONARY {
			b, err := bytesReader.ReadByte()
			if err != nil {
				return nil, err
			}

			length = uint64(bytesReader.Len())
			bitWidth = uint64(b)
		}

		i64s, err := RLEBitPackedHybridDecode(bytesReader, length, bitWidth)
		if err != nil {
			return nil, err
		}

		i64s = i64s[:count]

		if parquetType == parquet.Type_INT32 {
			return i64sToi32s(i64s), nil
		}

		return i64s, nil

	case parquet.Encoding_BIT_PACKED:
		return nil, fmt.Errorf("unsupported deprecated parquet encoding %v", parquet.Encoding_BIT_PACKED)

	case parquet.Encoding_DELTA_BINARY_PACKED:
		switch parquetType {
		case parquet.Type_INT32, parquet.Type_INT64:
		default:
			panic(fmt.Errorf("unsupported parquet type %v for delta binary packed encoding", parquetType))
		}

		i64s, err := DeltaDecode(bytesReader)
		if err != nil {
			return nil, err
		}

		i64s = i64s[:count]

		if parquetType == parquet.Type_INT32 {
			return i64sToi32s(i64s), nil
		}

		return i64s, nil

	case parquet.Encoding_DELTA_LENGTH_BYTE_ARRAY:
		switch parquetType {
		case parquet.Type_BYTE_ARRAY, parquet.Type_FIXED_LEN_BYTE_ARRAY:
		default:
			panic(fmt.Errorf("unsupported parquet type %v for delta length byte array encoding", parquetType))
		}

		byteSlices, err := DeltaLengthByteArrayDecode(bytesReader)
		if err != nil {
			return nil, err
		}

		return byteSlices[:count], nil

	case parquet.Encoding_DELTA_BYTE_ARRAY:
		switch parquetType {
		case parquet.Type_BYTE_ARRAY, parquet.Type_FIXED_LEN_BYTE_ARRAY:
		default:
			panic(fmt.Errorf("unsupported parquet type %v for delta byte array encoding", parquetType))
		}

		byteSlices, err := DeltaByteArrayDecode(bytesReader)
		if err != nil {
			return nil, err
		}

		return byteSlices[:count], nil
	}

	return nil, fmt.Errorf("unsupported parquet encoding %v", encoding)
}

func toStrings(values interface{}, parquetType parquet.Type) (strs []string) {
	switch parquetType {
	case parquet.Type_BYTE_ARRAY, parquet.Type_FIXED_LEN_BYTE_ARRAY:
		byteSlices, ok := values.([][]byte)
		if !ok {
			panic(fmt.Errorf("expected slice of byte arrays"))
		}

		for i := range byteSlices {
			strs = append(strs, string(byteSlices[i]))
		}

		return strs
	}

	panic(fmt.Errorf("parquet type %v cannot be converted to uint8", parquetType))
}

func toUint8s(values interface{}, parquetType parquet.Type) (ui8s []uint8) {
	switch parquetType {
	case parquet.Type_INT32:
		i32s, ok := values.([]int32)
		if !ok {
			panic(fmt.Errorf("expected slice of int32"))
		}

		for i := range i32s {
			ui8s = append(ui8s, uint8(i32s[i]))
		}

		return ui8s

	case parquet.Type_INT64:
		i64s, ok := values.([]int64)
		if !ok {
			panic(fmt.Errorf("expected slice of int64"))
		}

		for i := range i64s {
			ui8s = append(ui8s, uint8(i64s[i]))
		}

		return ui8s
	}

	panic(fmt.Errorf("parquet type %v cannot be converted to uint8", parquetType))
}

func toUint16s(values interface{}, parquetType parquet.Type) (ui16s []uint16) {
	switch parquetType {
	case parquet.Type_INT32:
		i32s, ok := values.([]int32)
		if !ok {
			panic(fmt.Errorf("expected slice of int32"))
		}

		for i := range i32s {
			ui16s = append(ui16s, uint16(i32s[i]))
		}

		return ui16s

	case parquet.Type_INT64:
		i64s, ok := values.([]int64)
		if !ok {
			panic(fmt.Errorf("expected slice of int64"))
		}

		for i := range i64s {
			ui16s = append(ui16s, uint16(i64s[i]))
		}

		return ui16s
	}

	panic(fmt.Errorf("parquet type %v cannot be converted to uint16", parquetType))
}

func toUint32s(values interface{}, parquetType parquet.Type) (ui32s []uint32) {
	switch parquetType {
	case parquet.Type_INT32:
		i32s, ok := values.([]int32)
		if !ok {
			panic(fmt.Errorf("expected slice of int32"))
		}

		for i := range i32s {
			ui32s = append(ui32s, uint32(i32s[i]))
		}

		return ui32s

	case parquet.Type_INT64:
		i64s, ok := values.([]int64)
		if !ok {
			panic(fmt.Errorf("expected slice of int64"))
		}

		for i := range i64s {
			ui32s = append(ui32s, uint32(i64s[i]))
		}

		return ui32s
	}

	panic(fmt.Errorf("parquet type %v cannot be converted to uint32", parquetType))
}

func toUint64s(values interface{}, parquetType parquet.Type) (ui64s []uint64) {
	switch parquetType {
	case parquet.Type_INT32:
		i32s, ok := values.([]int32)
		if !ok {
			panic(fmt.Errorf("expected slice of int32"))
		}

		for i := range i32s {
			ui64s = append(ui64s, uint64(i32s[i]))
		}

		return ui64s

	case parquet.Type_INT64:
		i64s, ok := values.([]int64)
		if !ok {
			panic(fmt.Errorf("expected slice of int64"))
		}

		for i := range i64s {
			ui64s = append(ui64s, uint64(i64s[i]))
		}

		return ui64s
	}

	panic(fmt.Errorf("parquet type %v cannot be converted to uint64", parquetType))
}

func toInt8s(values interface{}, parquetType parquet.Type) (i8s []int8) {
	switch parquetType {
	case parquet.Type_INT32:
		i32s, ok := values.([]int32)
		if !ok {
			panic(fmt.Errorf("expected slice of int32"))
		}

		for i := range i32s {
			i8s = append(i8s, int8(i32s[i]))
		}

		return i8s

	case parquet.Type_INT64:
		i64s, ok := values.([]int64)
		if !ok {
			panic(fmt.Errorf("expected slice of int64"))
		}

		for i := range i64s {
			i8s = append(i8s, int8(i64s[i]))
		}

		return i8s
	}

	panic(fmt.Errorf("parquet type %v cannot be converted to int8", parquetType))
}

func toInt16s(values interface{}, parquetType parquet.Type) (i16s []int16) {
	switch parquetType {
	case parquet.Type_INT32:
		i32s, ok := values.([]int32)
		if !ok {
			panic(fmt.Errorf("expected slice of int32"))
		}

		for i := range i32s {
			i16s = append(i16s, int16(i32s[i]))
		}

		return i16s

	case parquet.Type_INT64:
		i64s, ok := values.([]int64)
		if !ok {
			panic(fmt.Errorf("expected slice of int64"))
		}

		for i := range i64s {
			i16s = append(i16s, int16(i64s[i]))
		}

		return i16s
	}

	panic(fmt.Errorf("parquet type %v cannot be converted to int16", parquetType))
}

func toInt32s(values interface{}, parquetType parquet.Type) (i32s []int32) {
	switch parquetType {
	case parquet.Type_INT32:
		i32s, ok := values.([]int32)
		if !ok {
			panic(fmt.Errorf("expected slice of int32"))
		}

		return i32s

	case parquet.Type_INT64:
		i64s, ok := values.([]int64)
		if !ok {
			panic(fmt.Errorf("expected slice of int64"))
		}

		for i := range i64s {
			i32s = append(i32s, int32(i64s[i]))
		}

		return i32s
	}

	panic(fmt.Errorf("parquet type %v cannot be converted to int32", parquetType))
}

func toInt64s(values interface{}, parquetType parquet.Type) (i64s []int64) {
	switch parquetType {
	case parquet.Type_INT32:
		i32s, ok := values.([]int32)
		if !ok {
			panic(fmt.Errorf("expected slice of int32"))
		}

		for i := range i32s {
			i64s = append(i64s, int64(i32s[i]))
		}

		return i64s

	case parquet.Type_INT64:
		i64s, ok := values.([]int64)
		if !ok {
			panic(fmt.Errorf("expected slice of int64"))
		}

		return i64s
	}

	panic(fmt.Errorf("parquet type %v cannot be converted to int64", parquetType))
}

func ToConvertedValues(values interface{}, parquetType parquet.Type, convertedType *parquet.ConvertedType) (result []interface{}) {
	if convertedType == nil {
		switch parquetType {
		case parquet.Type_BOOLEAN:
			bs, ok := values.([]bool)
			if !ok {
				panic(fmt.Errorf("expected slice of bool"))
			}

			for i := range bs {
				result = append(result, bs[i])
			}

			return result

		case parquet.Type_INT32:
			i32s, ok := values.([]int32)
			if !ok {
				panic(fmt.Errorf("expected slice of int32"))
			}

			for i := range i32s {
				result = append(result, i32s[i])
			}

			return result

		case parquet.Type_INT64:
			i64s, ok := values.([]int64)
			if !ok {
				panic(fmt.Errorf("expected slice of int64"))
			}

			for i := range i64s {
				result = append(result, i64s[i])
			}

			return result

		case parquet.Type_INT96, parquet.Type_BYTE_ARRAY, parquet.Type_FIXED_LEN_BYTE_ARRAY:
			byteSlices, ok := values.([][]byte)
			if !ok {
				panic(fmt.Errorf("expected slice of bytes for %v", parquetType))
			}

			for i := range byteSlices {
				result = append(result, byteSlices[i])
			}

			return result
		}

		panic(fmt.Errorf("unsupported parquet type %v", parquetType))
	}

	switch *convertedType {
	case parquet.ConvertedType_UTF8:
		for _, v := range toStrings(values, parquetType) {
			result = append(result, v)
		}
		return result

	case parquet.ConvertedType_UINT_8:
		for _, v := range toUint8s(values, parquetType) {
			result = append(result, v)
		}
		return result

	case parquet.ConvertedType_UINT_16:
		for _, v := range toUint16s(values, parquetType) {
			result = append(result, v)
		}
		return result

	case parquet.ConvertedType_UINT_32:
		for _, v := range toUint32s(values, parquetType) {
			result = append(result, v)
		}
		return result

	case parquet.ConvertedType_UINT_64:
		for _, v := range toUint64s(values, parquetType) {
			result = append(result, v)
		}
		return result

	case parquet.ConvertedType_INT_8:
		for _, v := range toInt8s(values, parquetType) {
			result = append(result, v)
		}
		return result

	case parquet.ConvertedType_INT_16:
		for _, v := range toInt16s(values, parquetType) {
			result = append(result, v)
		}
		return result

	case parquet.ConvertedType_INT_32:
		for _, v := range toInt32s(values, parquetType) {
			result = append(result, v)
		}
		return result

	case parquet.ConvertedType_INT_64:
		for _, v := range toInt64s(values, parquetType) {
			result = append(result, v)
		}
		return result
	}

	// FIXME: handle below converted types.
	// ConvertedType_ENUM
	// ConvertedType_DECIMAL
	// ConvertedType_DATE
	// ConvertedType_TIME_MILLIS
	// ConvertedType_TIME_MICROS
	// ConvertedType_TIMESTAMP_MILLIS
	// ConvertedType_TIMESTAMP_MICROS
	// ConvertedType_JSON
	// ConvertedType_BSON
	// ConvertedType_INTERVAL

	panic(fmt.Errorf("unsupported converted type %v", convertedType))
}

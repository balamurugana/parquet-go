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
	"encoding/binary"
	"fmt"
	"math"

	"github.com/minio/parquet-go/gen-go/parquet"
)

func bytesToUint64(buf []byte) uint64 {
	return binary.LittleEndian.Uint64(buf)
}

func plainDecodeBools(reader *bytes.Reader, count uint64) (result []bool, err error) {
	i64s, err := bitPackedDecode(reader, count, 1)
	if err != nil {
		return nil, err
	}

	var i uint64
	for i = 0; i < count; i++ {
		result = append(result, i64s[i] > 0)
	}

	return result, nil
}

func plainDecodeInt32s(reader *bytes.Reader, count uint64) (result []int32, err error) {
	buf := make([]byte, 4)

	var i uint64
	for i = 0; i < count; i++ {
		if _, err = reader.Read(buf); err != nil {
			return nil, err
		}

		result = append(result, int32(bytesToUint32(buf)))
	}

	return result, nil
}

func plainDecodeInt64s(reader *bytes.Reader, count uint64) (result []int64, err error) {
	buf := make([]byte, 8)

	var i uint64
	for i = 0; i < count; i++ {
		if _, err = reader.Read(buf); err != nil {
			return nil, err
		}

		result = append(result, int64(bytesToUint64(buf)))
	}

	return result, nil
}

func plainDecodeInt96s(reader *bytes.Reader, count uint64) (result [][]byte, err error) {
	var i uint64
	for i = 0; i < count; i++ {
		buf := make([]byte, 12)

		if _, err = reader.Read(buf); err != nil {
			return nil, err
		}

		result = append(result, buf)
	}

	return result, nil
}

func plainDecodeFloat32s(reader *bytes.Reader, count uint64) (result []float32, err error) {
	buf := make([]byte, 4)

	var i uint64
	for i = 0; i < count; i++ {
		if _, err = reader.Read(buf); err != nil {
			return nil, err
		}

		result = append(result, math.Float32frombits(bytesToUint32(buf)))
	}

	return result, nil
}

func plainDecodeFloat64s(reader *bytes.Reader, count uint64) (result []float64, err error) {
	buf := make([]byte, 8)

	var i uint64
	for i = 0; i < count; i++ {
		if _, err = reader.Read(buf); err != nil {
			return nil, err
		}

		result = append(result, math.Float64frombits(bytesToUint64(buf)))
	}

	return result, nil
}

func plainDecodeBytesSlices(reader *bytes.Reader, count uint64) (result [][]byte, err error) {
	buf := make([]byte, 4)
	var length uint32
	var data []byte

	var i uint64
	for i = 0; i < count; i++ {
		if _, err = reader.Read(buf); err != nil {
			return nil, err
		}

		length = bytesToUint32(buf)
		data = make([]byte, length)
		if length > 0 {
			if _, err = reader.Read(data); err != nil {
				return nil, err
			}
		}

		result = append(result, data)
	}

	return result, nil
}

func plainDecodeByteArrays(reader *bytes.Reader, count, length uint64) (result [][]byte, err error) {
	var i uint64
	for i = 0; i < count; i++ {
		data := make([]byte, length)
		if _, err = reader.Read(data); err != nil {
			return nil, err
		}

		result = append(result, data)
	}

	return result, nil
}

// PlainDecode decodes values specified in https://github.com/apache/parquet-format/blob/master/Encodings.md#plain-plain--0
//
// Supported Types: BOOLEAN, INT32, INT64, INT96, FLOAT, DOUBLE, BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY
func PlainDecode(reader *bytes.Reader, parquetType parquet.Type, count, length uint64) (interface{}, error) {
	switch parquetType {
	case parquet.Type_BOOLEAN:
		return plainDecodeBools(reader, count)
	case parquet.Type_INT32:
		return plainDecodeInt32s(reader, count)
	case parquet.Type_INT64:
		return plainDecodeInt64s(reader, count)
	case parquet.Type_INT96:
		return plainDecodeInt96s(reader, count)
	case parquet.Type_FLOAT:
		return plainDecodeFloat32s(reader, count)
	case parquet.Type_DOUBLE:
		return plainDecodeFloat64s(reader, count)
	case parquet.Type_BYTE_ARRAY:
		return plainDecodeBytesSlices(reader, count)
	case parquet.Type_FIXED_LEN_BYTE_ARRAY:
		return plainDecodeByteArrays(reader, count, length)
	}

	panic(fmt.Errorf("unknown parquet type %v", parquetType))
}

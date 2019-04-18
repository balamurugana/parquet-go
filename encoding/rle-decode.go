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

import "bytes"

func rleDecode(reader *bytes.Reader, count, bitWidth uint64) (result []int64, err error) {
	width := (bitWidth + 7) / 8
	data := make([]byte, width)
	if width > 0 {
		if _, err = reader.Read(data); err != nil {
			return nil, err
		}
	}

	if width < 4 {
		data = append(data, make([]byte, 4-width)...)
	}

	val := int64(bytesToUint32(data))

	result = make([]int64, count)
	for i := range result {
		result[i] = val
	}

	return result, nil
}

// RLEBitPackedHybridDecode decodes values specified in https://github.com/apache/parquet-format/blob/master/Encodings.md#run-length-encoding--bit-packing-hybrid-rle--3
//
// Supported Types: INT32, INT64
func RLEBitPackedHybridDecode(reader *bytes.Reader, length, bitWidth uint64) (result []int64, err error) {
	if length <= 0 {
		var i32s []int32
		i32s, err = plainDecodeInt32s(reader, 1)
		if err != nil {
			return nil, err
		}
		length = uint64(i32s[0])
	}

	buf := make([]byte, length)
	if _, err = reader.Read(buf); err != nil {
		return nil, err
	}

	reader = bytes.NewReader(buf)
	for reader.Len() > 0 {
		header, err := varIntDecode(reader)
		if err != nil {
			return nil, err
		}
		count := header >> 1

		var i64s []int64
		if header&1 == 0 {
			i64s, err = rleDecode(reader, count, bitWidth)
		} else {
			i64s, err = bitPackedDecode(reader, count, bitWidth)
		}

		if err != nil {
			return nil, err
		}

		result = append(result, i64s...)
	}

	return result, nil
}

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
)

// DeltaDecode decodes values specified in https://github.com/apache/parquet-format/blob/master/Encodings.md#delta-encoding-delta_binary_packed--5
//
// Supported Types: INT32, INT64.
func DeltaDecode(reader *bytes.Reader) (result []int64, err error) {
	blockSize, err := varIntDecode(reader)
	if err != nil {
		return nil, err
	}

	numMiniblocksInBlock, err := varIntDecode(reader)
	if err != nil {
		return nil, err
	}

	numValues, err := varIntDecode(reader)
	if err != nil {
		return nil, err
	}

	firstValueZigZag, err := varIntDecode(reader)
	if err != nil {
		return nil, err
	}

	v := int64(firstValueZigZag>>1) ^ (-int64(firstValueZigZag & 1))
	result = append(result, v)

	numValuesInMiniBlock := blockSize / numMiniblocksInBlock

	bitWidths := make([]uint64, numMiniblocksInBlock)
	for uint64(len(result)) < numValues {
		minDeltaZigZag, err := varIntDecode(reader)
		if err != nil {
			return nil, err
		}

		for i := uint64(0); i < numMiniblocksInBlock; i++ {
			b, err := reader.ReadByte()
			if err != nil {
				return nil, err
			}
			bitWidths[i] = uint64(b)
		}

		minDelta := int64(minDeltaZigZag>>1) ^ (-int64(minDeltaZigZag & 1))
		for i := uint64(0); i < numMiniblocksInBlock; i++ {
			i64s, err := bitPackedDecode(reader, numValuesInMiniBlock/8, bitWidths[i])
			if err != nil {
				return nil, err
			}

			for j := range i64s {
				v += i64s[j] + minDelta
				result = append(result, v)
			}
		}
	}

	return result[:numValues], nil
}

func DeltaLengthByteArrayDecode(reader *bytes.Reader) (result [][]byte, err error) {
	i64s, err := DeltaDecode(reader)
	if err != nil {
		return nil, err
	}

	for i := 0; i < len(i64s); i++ {
		arrays, err := plainDecodeByteArrays(reader, 1, uint64(i64s[i]))
		if err != nil {
			return nil, err
		}

		result = append(result, arrays[0])
	}

	return result, nil
}

func DeltaByteArrayDecode(reader *bytes.Reader) (result [][]byte, err error) {
	i64s, err := DeltaDecode(reader)
	if err != nil {
		return nil, err
	}

	suffixes, err := DeltaLengthByteArrayDecode(reader)
	if err != nil {
		return nil, err
	}

	result = append(result, suffixes[0])
	for i := 1; i < len(i64s); i++ {
		prefixLength := i64s[i]
		val := append([]byte{}, result[i-1][:prefixLength]...)
		val = append(val, suffixes[i]...)
		result = append(result, val)
	}

	return result, nil
}

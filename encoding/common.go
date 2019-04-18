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

	"github.com/minio/parquet-go/common"
)

// Refer https://en.wikipedia.org/wiki/LEB128#Unsigned_LEB128
func varIntEncode(ui64 uint64) []byte {
	if ui64 == 0 {
		return []byte{0}
	}

	length := int(common.BitWidth(ui64)+6) / 7
	data := make([]byte, length)
	for i := 0; i < length; i++ {
		data[i] = byte(ui64&0x7F) | 0x80
		ui64 >>= 7
	}
	data[length-1] &= 0x7F

	return data
}

func varIntDecode(reader *bytes.Reader) (v uint64, err error) {
	var b byte
	var shift uint64

	for {
		if b, err = reader.ReadByte(); err != nil {
			return 0, err
		}

		if v |= ((uint64(b) & 0x7F) << shift); b&0x80 == 0 {
			break
		}

		shift += 7
	}

	return v, nil
}

func bytesToUint32(buf []byte) uint32 {
	return binary.LittleEndian.Uint32(buf)
}

func i64sToi32s(i64s []int64) (i32s []int32) {
	i32s = make([]int32, len(i64s))
	for i := range i64s {
		i32s[i] = int32(i64s[i])
	}

	return i32s
}

func bitPackedDecode(reader *bytes.Reader, header, bitWidth uint64) (result []int64, err error) {
	count := header * 8

	if count == 0 {
		return result, nil
	}

	if bitWidth == 0 {
		return make([]int64, count), nil
	}

	data := make([]byte, header*bitWidth)
	if _, err = reader.Read(data); err != nil {
		return nil, err
	}

	var val, used, left, b uint64

	valNeedBits := bitWidth
	i := -1
	for {
		if left <= 0 {
			i++
			if i >= len(data) {
				break
			}

			b = uint64(data[i])
			left = 8
			used = 0
		}

		if left >= valNeedBits {
			val |= ((b >> used) & ((1 << valNeedBits) - 1)) << (bitWidth - valNeedBits)
			result = append(result, int64(val))
			val = 0
			left -= valNeedBits
			used += valNeedBits
			valNeedBits = bitWidth
		} else {
			val |= (b >> used) << (bitWidth - valNeedBits)
			valNeedBits -= left
			left = 0
		}
	}

	return result, nil
}

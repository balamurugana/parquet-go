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

	"github.com/minio/parquet-go/gen-go/parquet"
)

type Values struct {
	values        interface{}
	valueType     *parquet.Type
	convertedType *parquet.ConvertedType
}

func (values *Values) BooleanValues() []*bool {
	if values.valueType == nil || *values.valueType != parquet.Type_BOOLEAN {
		panic(fmt.Errorf("values is not BOOLEAN type"))
	}

	return values.values.([]*bool)
}

func (values *Values) Int32Values() ([]*int32, *parquet.ConvertedType) {
	if values.valueType == nil || *values.valueType != parquet.Type_INT32 {
		panic(fmt.Errorf("values is not INT32 type"))
	}

	return values.values.([]*int32), values.convertedType
}

func (values *Values) Int64Values() ([]*int64, *parquet.ConvertedType) {
	if values.valueType == nil || *values.valueType != parquet.Type_INT64 {
		panic(fmt.Errorf("values is not INT64 type"))
	}

	return values.values.([]*int64), values.convertedType
}

func (values *Values) Int96Values() []*[]byte {
	if values.valueType == nil || *values.valueType != parquet.Type_INT96 {
		panic(fmt.Errorf("values is not INT96 type"))
	}

	return values.values.([]*[]byte)
}

func (values *Values) FloatValues() []*float32 {
	if values.valueType == nil || *values.valueType != parquet.Type_FLOAT {
		panic(fmt.Errorf("values is not FLOAT type"))
	}

	return values.values.([]*float32)
}

func (values *Values) DoubleValues() []*float64 {
	if values.valueType == nil || *values.valueType != parquet.Type_DOUBLE {
		panic(fmt.Errorf("values is not DOUBLE type"))
	}

	return values.values.([]*float64)
}

func (values *Values) ByteArrayValues() ([]*[]byte, *parquet.ConvertedType) {
	if values.valueType == nil || *values.valueType != parquet.Type_BYTE_ARRAY {
		panic(fmt.Errorf("values is not BYTE_ARRAY type"))
	}

	return values.values.([]*[]byte), values.convertedType
}

func (values *Values) FixedLenByteArrayValues() []*[]byte {
	if values.valueType == nil || *values.valueType != parquet.Type_FIXED_LEN_BYTE_ARRAY {
		panic(fmt.Errorf("values is not FIXED_LEN_BYTE_ARRAY type"))
	}

	return values.values.([]*[]byte)
}

func NewBooleanValues(bs []*bool) *Values {
	return &Values{
		values:    bs,
		valueType: parquet.TypePtr(parquet.Type_BOOLEAN),
	}
}

func NewInt32Values(i32s []*int32, convertedType *parquet.ConvertedType) *Values {
	return &Values{
		values:        i32s,
		valueType:     parquet.TypePtr(parquet.Type_INT32),
		convertedType: convertedType,
	}
}

func NewInt64Values(i64s []*int64, convertedType *parquet.ConvertedType) *Values {
	return &Values{
		values:        i64s,
		valueType:     parquet.TypePtr(parquet.Type_INT64),
		convertedType: convertedType,
	}
}

func NewInt96Values(arrays []*[]byte) *Values {
	for i := range arrays {
		if arrays[i] != nil && len(*arrays[i]) != 12 {
			panic(fmt.Errorf("length %v of arrays[%v] must be 12", len(*arrays[i]), i))
		}
	}

	return &Values{
		values:    arrays,
		valueType: parquet.TypePtr(parquet.Type_INT96),
	}
}

func NewFloatValues(f32s []*float32) *Values {
	return &Values{
		values:    f32s,
		valueType: parquet.TypePtr(parquet.Type_FLOAT),
	}
}

func NewDoubleValues(f64s []*float64) *Values {
	return &Values{
		values:    f64s,
		valueType: parquet.TypePtr(parquet.Type_DOUBLE),
	}
}

func NewByteArrayValues(arrays []*[]byte, convertedType *parquet.ConvertedType) *Values {
	return &Values{
		values:        arrays,
		valueType:     parquet.TypePtr(parquet.Type_BYTE_ARRAY),
		convertedType: convertedType,
	}
}

func NewFixedLenByteArrayValues(arrays []*[]byte) *Values {
	arrayLen := 0
	isArrayLenSet := false
	for i := range arrays {
		if arrays[i] == nil {
			continue
		}

		if !isArrayLenSet {
			arrayLen = len(*arrays[i])
			isArrayLenSet = true
		}

		if arrayLen != len(*arrays[i]) {
			panic(fmt.Errorf("length %v of arrays[%v] differs in length %v", len(*arrays[i]), i, arrayLen))
		}
	}

	return &Values{
		values:    arrays,
		valueType: parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
	}
}

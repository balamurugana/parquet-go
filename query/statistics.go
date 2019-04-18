package query

import (
	"bytes"
	"fmt"

	"github.com/minio/parquet-go/encoding"
	"github.com/minio/parquet-go/gen-go/parquet"
)

type Statistics struct {
	max       *Value
	min       *Value
	numValues uint64
	numNulls  uint64
	numRows   uint64
}

func decodeStatValue(data []byte, parquetType parquet.Type) (*Value, error) {
	if data == nil {
		return nil, nil
	}

	values, err := encoding.PlainDecode(bytes.NewReader(data), parquetType, 1, 0)
	if err != nil {
		return nil, err
	}

	switch parquetType {
	case parquet.Type_BOOLEAN:
		return NewBool(values.([]bool)[0]), nil
	case parquet.Type_INT32:
		return NewInt(int64(values.([]int32)[0])), nil
	case parquet.Type_INT64:
		return NewInt(values.([]int64)[0]), nil
	case parquet.Type_FLOAT:
		return NewFloat(float64(values.([]float32)[0])), nil
	case parquet.Type_DOUBLE:
		return NewFloat(values.([]float64)[0]), nil
	case parquet.Type_INT96, parquet.Type_BYTE_ARRAY, parquet.Type_FIXED_LEN_BYTE_ARRAY:
		return NewString(string(values.([][]byte)[0])), nil
	}

	panic(fmt.Errorf("invalid parquet type %v", parquetType))
}

func getMaxValue(statistics *parquet.Statistics, parquetType parquet.Type) (*Value, error) {
	data := statistics.MaxValue
	if data == nil {
		data = statistics.Max
	}

	return decodeStatValue(data, parquetType)
}

func getMinValue(statistics *parquet.Statistics, parquetType parquet.Type) (*Value, error) {
	data := statistics.MinValue
	if data == nil {
		data = statistics.Min
	}

	return decodeStatValue(data, parquetType)
}

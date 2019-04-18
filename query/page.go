package query

import (
	"bytes"
	"fmt"

	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/minio/parquet-go/common"
	"github.com/minio/parquet-go/encoding"
	"github.com/minio/parquet-go/gen-go/parquet"
	"github.com/minio/parquet-go/schema"
)

type DictPage struct {
	values      interface{}
	parquetType parquet.Type
}

// func (page *DictPage) Type() parquet.Type {
// 	return page.parquetType
// }
//
// func (page *DictPage) Bools() []bool {
// 	if page.parquetType != parquet.Type_BOOLEAN {
// 		panic(fmt.Errorf("not BOOLEAN values"))
// 	}
//
// 	return page.values.([]bool)
// }
//
// func (page *DictPage) Int32s() []int32 {
// 	if page.parquetType != parquet.Type_INT32 {
// 		panic(fmt.Errorf("not INT32 values"))
// 	}
//
// 	return page.values.([]int32)
// }
//
// func (page *DictPage) Int64s() []int64 {
// 	if page.parquetType != parquet.Type_INT64 {
// 		panic(fmt.Errorf("not INT64 values"))
// 	}
//
// 	return page.values.([]int64)
// }
//
// func (page *DictPage) Floats() []float32 {
// 	if page.parquetType != parquet.Type_FLOAT {
// 		panic("not FLOAT values")
// 	}
//
// 	return page.values.([]float32)
// }
//
// func (page *DictPage) Doubles() []float64 {
// 	if page.parquetType != parquet.Type_DOUBLE {
// 		panic("not DOUBLE values")
// 	}
//
// 	return page.values.([]float64)
// }
//
// func (page *DictPage) ByteArrays() [][]byte {
// 	switch page.parquetType {
// 	case parquet.Type_INT96, parquet.Type_BYTE_ARRAY, parquet.Type_FIXED_LEN_BYTE_ARRAY:
// 		return page.values.([][]byte)
// 	}
//
// 	panic(fmt.Errorf("not INT96, BYTE_ARRAY or FIXED_LEN_BYTE_ARRAY values"))
// }

type DataPage struct {
	element *schema.Element

	RLs []int64
	DLs []int64

	// Either values or dictPage/indices or rawData must be present along with RLs, DLs.
	values []*Value

	dictPage *DictPage
	indices  []int64

	rawData      []byte
	isCompressed bool

	numValues int64
	numNulls  int64
	numRows   int64
	maxValue  *Value
	minValue  *Value

	isFirstValueChunked bool
	skipInNextPage      int64
}

func (page *DataPage) IsEmpty() bool {
	return len(page.values) == 0 || len(page.indices) == 0
}

func (page *DataPage) decodeData(data []byte) error {
	typeLength := uint64(0)
	if page.element.TypeLength != nil {
		typeLength = uint64(*page.element.TypeLength)
	}

	values, err := encoding.Decode(bytes.NewReader(data), *page.element.Encoding, *page.element.Type,
		uint64(page.numValues-page.numNulls), typeLength)
	if err != nil {
		return err
	}

	page.values = make([]*Value, page.numValues-page.numNulls)

	switch *page.element.Type {
	case parquet.Type_BOOLEAN:
		bs := values.([]bool)
		for i, b := range bs {
			page.values[i] = NewBool(b)
		}

	case parquet.Type_INT32:
		i32s := values.([]int32)
		for i, i32 := range i32s {
			page.values[i] = NewInt(int64(i32))
		}

	case parquet.Type_INT64:
		i64s := values.([]int64)
		for i, i64 := range i64s {
			page.values[i] = NewInt(i64)
		}

	case parquet.Type_FLOAT:
		f32s := values.([]float32)
		for i, f32 := range f32s {
			page.values[i] = NewFloat(float64(f32))
		}

	case parquet.Type_DOUBLE:
		f64s := values.([]float64)
		for i, f64 := range f64s {
			page.values[i] = NewFloat(f64)
		}

	case parquet.Type_INT96, parquet.Type_BYTE_ARRAY, parquet.Type_FIXED_LEN_BYTE_ARRAY:
		byteSlices := values.([][]byte)
		for i, byteSlice := range byteSlices {
			page.values[i] = NewString(string(byteSlice))
		}

	default:
		panic(fmt.Errorf("invalid parquet type %v", page.element.Type))
	}

	return nil
}

func (page *DataPage) decodeIndices(data []byte) (err error) {
	page.indices, err = encoding.RLEBitPackedHybridDecode(bytes.NewReader(data[1:]), 0, uint64(data[0]))
	if err != nil {
		return err
	}

	var maxValue, minValue *Value
	switch page.dictPage.parquetType {
	case parquet.Type_BOOLEAN:
		var max, min bool
		bs := page.dictPage.values.([]bool)
		for i, index := range page.indices {
			value := bs[index]
			if i == 0 {
				max = value
				min = value
				continue
			}

			if value {
				max = value
			} else {
				min = value
			}
		}
		maxValue = NewBool(max)
		minValue = NewBool(min)

	case parquet.Type_INT32:
		var max, min int64
		i32s := page.dictPage.values.([]int32)
		for i, index := range page.indices {
			value := int64(i32s[index])
			if i == 0 {
				max = value
				min = value
				continue
			}

			if max < value {
				max = value
			}
			if min > value {
				min = value
			}
		}
		maxValue = NewInt(max)
		minValue = NewInt(min)

	case parquet.Type_INT64:
		var max, min int64
		i64s := page.dictPage.values.([]int64)
		for i, index := range page.indices {
			value := i64s[index]
			if i == 0 {
				max = value
				min = value
				continue
			}

			if max < value {
				max = value
			}
			if min > value {
				min = value
			}
		}
		maxValue = NewInt(max)
		minValue = NewInt(min)

	case parquet.Type_FLOAT:
		var max, min float64
		f32s := page.dictPage.values.([]float32)
		for i, index := range page.indices {
			value := float64(f32s[index])
			if i == 0 {
				max = value
				min = value
				continue
			}

			if max < value {
				max = value
			}
			if min > value {
				min = value
			}
		}
		maxValue = NewFloat(max)
		minValue = NewFloat(min)

	case parquet.Type_DOUBLE:
		var max, min float64
		f64s := page.dictPage.values.([]float64)
		for i, index := range page.indices {
			value := f64s[index]
			if i == 0 {
				max = value
				min = value
				continue
			}

			if max < value {
				max = value
			}
			if min > value {
				min = value
			}
		}
		maxValue = NewFloat(max)
		minValue = NewFloat(min)

	case parquet.Type_INT96, parquet.Type_BYTE_ARRAY, parquet.Type_FIXED_LEN_BYTE_ARRAY:
		var max, min []byte
		byteSlices := page.dictPage.values.([][]byte)
		for i, index := range page.indices {
			value := byteSlices[index]
			if i == 0 {
				max = value
				min = value
				continue
			}

			if bytes.Compare(max, value) < 0 {
				max = value
			}
			if bytes.Compare(min, value) > 0 {
				min = value
			}
		}
		maxValue = NewString(string(max))
		minValue = NewString(string(min))

	default:
		panic(fmt.Errorf("invalid parquet type %v", page.element.Type))
	}

	return nil
}

func (page *DataPage) Decode() error {
	if page.rawData == nil {
		return nil
	}

	data, err := common.Uncompress(*page.element.CompressionType, page.rawData)
	if err != nil {
		return err
	}

	if page.dictPage == nil {
		err = page.decodeData(data)
	} else {
		err = page.decodeIndices(data)
	}

	if err != nil {
		return err
	}

	page.rawData = nil
	page.isCompressed = false

	return nil
}

func (page *DataPage) getRowStartIndex(i int64) int64 {
	for ; i < int64(len(page.RLs)); i++ {
		if page.RLs[i] == 0 {
			return i
		}
	}

	return -1
}

func (page *DataPage) skipRows(n int64) (int64, error) {
	if page.IsEmpty() {
		return 0, fmt.Errorf("empty page")
	}

	if page.numRows < n {
		page.RLs = nil
		page.DLs = nil
		page.values = nil
		page.indices = nil
		n = page.numRows
		page.numRows = 0
		return n, nil
	}

	i := int64(0)
	for r := int64(0); r < n; r++ {
		if i = page.getRowStartIndex(i); i < 0 {
			panic(fmt.Errorf("insufficient row values"))
		}
	}

	currRowIndex := i
	if i = page.getRowStartIndex(i); i < 0 {
		i = currRowIndex
		n--
	}

	if page.indices != nil {
		j := 0
		for di := int64(0); di < i; di++ {
			if page.DLs[di] == page.element.MaxDefinitionLevel {
				j++
			}
		}
		page.indices = page.indices[j:]
	} else {
		page.values = page.values[i:]
	}

	page.RLs = page.RLs[i:]
	page.DLs = page.DLs[i:]
	page.numRows -= n
	return n, nil
}

func (page *DataPage) SkipRows(n int64) error {
	skipped, err := page.skipRows(n)
	if err != nil {
		return err
	}

	if n != skipped {
		page.skipInNextPage = n - skipped
	}

	return nil
}

func readDataPageV1(header *parquet.DataPageHeader, thriftReader *thrift.TBufferedTransport, compressedPageSize int64, element *schema.Element, dictPage *DictPage) (*DataPage, error) {
	var err error
	data := make([]byte, compressedPageSize)
	if _, err = thriftReader.Read(data); err != nil {
		return nil, err
	}
	if data, err = common.Uncompress(*element.CompressionType, data); err != nil {
		return nil, err
	}

	bytesReader := bytes.NewReader(data)

	var RLs, DLs []int64
	if element.MaxRepetitionLevel > 0 {
		values, err := encoding.Decode(bytesReader, parquet.Encoding_RLE, parquet.Type_INT64,
			uint64(header.NumValues), uint64(common.BitWidth(uint64(element.MaxRepetitionLevel))))
		if err != nil {
			return nil, err
		}

		if RLs = values.([]int64); len(RLs) > int(header.NumValues) {
			RLs = RLs[:header.NumValues]
		}
	}

	if element.MaxDefinitionLevel > 0 {
		values, err := encoding.Decode(bytesReader, parquet.Encoding_RLE, parquet.Type_INT64,
			uint64(header.NumValues), uint64(common.BitWidth(uint64(element.MaxDefinitionLevel))))
		if err != nil {
			return nil, err
		}

		if DLs = values.([]int64); len(DLs) > int(header.NumValues) {
			DLs = DLs[:header.NumValues]
		}
	}

	numNulls := int64(0)
	for i := 0; i < len(DLs); i++ {
		if DLs[i] != int64(element.MaxDefinitionLevel) {
			numNulls++
		}
	}

	numRows := int64(0)
	for i := 0; i < len(RLs); i++ {
		if RLs[i] == 0 {
			numRows++
		}
	}

	isFirstValueChunked := len(RLs) > 0 && RLs[0] != 0

	rawData := data[bytesReader.Len():]

	var maxValue, minValue *Value
	if header.Statistics != nil {
		if maxValue, err = getMaxValue(header.Statistics, *element.Type); err != nil {
			return nil, err
		}

		if minValue, err = getMinValue(header.Statistics, *element.Type); err != nil {
			return nil, err
		}
	}

	return &DataPage{
		element: element,

		RLs: RLs,
		DLs: DLs,

		dictPage: dictPage,

		rawData: rawData,

		numValues: int64(header.NumValues),
		numNulls:  numNulls,
		numRows:   numRows,
		maxValue:  maxValue,
		minValue:  minValue,

		isFirstValueChunked: isFirstValueChunked,
	}, nil
}

func readDataPageV2(header *parquet.DataPageHeaderV2, thriftReader *thrift.TBufferedTransport, compressedPageSize int64, element *schema.Element, dictPage *DictPage) (*DataPage, error) {
	var RLs []int64
	if header.RepetitionLevelsByteLength > 0 {
		RLData := make([]byte, header.RepetitionLevelsByteLength)
		if _, err := thriftReader.Read(RLData); err != nil {
			return nil, err
		}

		data := uint32ToBytes(uint32(header.RepetitionLevelsByteLength))
		data = append(data, RLData...)

		values, err := encoding.Decode(bytes.NewReader(data), parquet.Encoding_RLE, parquet.Type_INT64,
			uint64(header.NumValues), uint64(common.BitWidth(uint64(element.MaxRepetitionLevel))))
		if err != nil {
			return nil, err
		}

		if RLs = values.([]int64); len(RLs) > int(header.NumValues) {
			RLs = RLs[:header.NumValues]
		}
	}

	var DLs []int64
	if header.DefinitionLevelsByteLength > 0 {
		DLData := make([]byte, header.DefinitionLevelsByteLength)
		if _, err := thriftReader.Read(DLData); err != nil {
			return nil, err
		}

		data := uint32ToBytes(uint32(header.DefinitionLevelsByteLength))
		data = append(data, DLData...)

		values, err := encoding.Decode(bytes.NewReader(DLData), parquet.Encoding_RLE, parquet.Type_INT64,
			uint64(header.NumValues), uint64(common.BitWidth(uint64(element.MaxDefinitionLevel))))
		if err != nil {
			return nil, err
		}

		if DLs = values.([]int64); len(DLs) > int(header.NumValues) {
			DLs = DLs[:header.NumValues]
		}
	}

	size := compressedPageSize - int64(header.RepetitionLevelsByteLength-header.DefinitionLevelsByteLength)
	rawData := make([]byte, size)
	if _, err := thriftReader.Read(rawData); err != nil {
		return nil, err
	}

	isFirstValueChunked := len(RLs) > 0 && RLs[0] != 0

	var maxValue, minValue *Value
	var err error
	if header.Statistics != nil {
		if maxValue, err = getMaxValue(header.Statistics, *element.Type); err != nil {
			return nil, err
		}

		if minValue, err = getMinValue(header.Statistics, *element.Type); err != nil {
			return nil, err
		}
	}

	return &DataPage{
		element: element,

		RLs: RLs,
		DLs: DLs,

		dictPage: dictPage,

		rawData:      rawData,
		isCompressed: *element.CompressionType > 0,

		numValues: int64(header.NumValues),
		numNulls:  int64(header.NumNulls),
		numRows:   int64(header.NumRows),
		maxValue:  maxValue,
		minValue:  minValue,

		isFirstValueChunked: isFirstValueChunked,
	}, nil
}

func ReadDataPage(pageHeader *parquet.PageHeader, thriftReader *thrift.TBufferedTransport, compressedPageSize int64, element *schema.Element, skipFirstValueChunk bool, dictPage *DictPage) (*DataPage, error) {
	var dataPage *DataPage
	var err error

	switch pageHeader.Type {
	case parquet.PageType_DATA_PAGE:
		dataPage, err = readDataPageV1(pageHeader.DataPageHeader, thriftReader, compressedPageSize, element, dictPage)
	case parquet.PageType_DATA_PAGE_V2:
		dataPage, err = readDataPageV2(pageHeader.DataPageHeaderV2, thriftReader, compressedPageSize, element, dictPage)
	default:
		panic(fmt.Errorf("unknown data page type %v", pageHeader.Type))
	}

	if err != nil {
		return nil, err
	}

	if skipFirstValueChunk && dataPage.isFirstValueChunked && !dataPage.IsEmpty() {
		i := int64(0)
		if i = dataPage.getRowStartIndex(i); i < 0 {
			dataPage.RLs = nil
			dataPage.DLs = nil
			dataPage.values = nil
			dataPage.indices = nil
			dataPage.numRows = 0
		} else {
			if dataPage.indices != nil {
				j := 0
				for di := int64(0); di < i; di++ {
					if dataPage.DLs[di] == dataPage.element.MaxDefinitionLevel {
						j++
					}
				}
				dataPage.indices = dataPage.indices[j:]
			} else {
				dataPage.values = dataPage.values[i:]
			}

			dataPage.RLs = dataPage.RLs[i:]
			dataPage.DLs = dataPage.DLs[i:]
		}
	}

	return dataPage, nil
}

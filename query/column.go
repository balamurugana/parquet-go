package query

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/minio/parquet-go/common"
	"github.com/minio/parquet-go/encoding"
	"github.com/minio/parquet-go/gen-go/parquet"
)

func uint32ToBytes(v uint32) []byte {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, v)
	return buf
}

type column struct {
	chunk         *parquet.ColumnChunk
	getReaderFunc GetReaderFunc
	typeLength    uint64
	maxRL         int32
	maxDL         int32
	convertedType *parquet.ConvertedType
	statistics    *Statistics

	rc           io.ReadCloser
	thriftReader *thrift.TBufferedTransport

	dictValues interface{}

	numValues   uint64
	numNulls    uint64
	numRows     uint64
	RLs         []int64
	DLs         []int64
	indexValues []int64
	values      []*Value
	data        []byte

	dataPageHeader *parquet.PageHeader
}

func (c *column) readPageV2() (data []byte, err error) {
	header := c.dataPageHeader.DataPageHeaderV2

	c.numValues = uint64(header.NumValues)
	c.numNulls = uint64(header.NumNulls)
	c.numRows = uint64(header.NumRows)

	if header.RepetitionLevelsByteLength > 0 {
		data := uint32ToBytes(uint32(header.RepetitionLevelsByteLength))
		RLData := make([]byte, header.RepetitionLevelsByteLength)
		if _, err := c.thriftReader.Read(RLData); err != nil {
			return nil, err
		}
		data = append(data, RLData...)

		values, err := encoding.Decode(bytes.NewReader(data), parquet.Encoding_RLE, parquet.Type_INT64,
			c.numValues, uint64(common.BitWidth(uint64(c.maxRL))))
		if err != nil {
			return nil, err
		}

		if c.RLs = values.([]int64); uint64(len(c.RLs)) > c.numValues {
			c.RLs = c.RLs[:c.numValues]
		}
	}

	if header.DefinitionLevelsByteLength > 0 {
		data := uint32ToBytes(uint32(header.DefinitionLevelsByteLength))
		DLData := make([]byte, header.DefinitionLevelsByteLength)
		if _, err := c.thriftReader.Read(DLData); err != nil {
			return nil, err
		}
		data = append(data, DLData...)

		values, err := encoding.Decode(bytes.NewReader(DLData), parquet.Encoding_RLE, parquet.Type_INT64,
			c.numValues, uint64(common.BitWidth(uint64(c.maxDL))))
		if err != nil {
			return nil, err
		}

		if c.DLs = values.([]int64); uint64(len(c.DLs)) > c.numValues {
			c.DLs = c.DLs[:c.numValues]
		}
	}

	size := c.dataPageHeader.CompressedPageSize - header.RepetitionLevelsByteLength -
		header.DefinitionLevelsByteLength

	// Note: no need to use io.LimitReader because page size is usually 8KiB.
	data = make([]byte, size)
	if _, err := c.thriftReader.Read(data); err != nil {
		return nil, err
	}

	data, err = common.Uncompress(c.chunk.MetaData.Codec, data)
	return data, err
}

func (c *column) readPageV1() (data []byte, err error) {
	header := c.dataPageHeader.DataPageHeader

	c.numValues = uint64(header.NumValues)

	data = make([]byte, c.dataPageHeader.CompressedPageSize)
	if _, err = c.thriftReader.Read(data); err != nil {
		return nil, err
	}
	if data, err = common.Uncompress(c.chunk.MetaData.Codec, data); err != nil {
		return nil, err
	}

	bytesReader := bytes.NewReader(data)

	if c.maxRL > 0 {
		values, err := encoding.Decode(bytesReader, parquet.Encoding_RLE, parquet.Type_INT64,
			c.numValues, uint64(common.BitWidth(uint64(c.maxRL))))
		if err != nil {
			return nil, err
		}

		if c.RLs = values.([]int64); uint64(len(c.RLs)) > c.numValues {
			c.RLs = c.RLs[:c.numValues]
		}
	}

	if c.maxDL > 0 {
		values, err := encoding.Decode(bytesReader, parquet.Encoding_RLE, parquet.Type_INT64,
			c.numValues, uint64(common.BitWidth(uint64(c.maxDL))))
		if err != nil {
			return nil, err
		}

		if c.DLs = values.([]int64); uint64(len(c.DLs)) > c.numValues {
			c.DLs = c.DLs[:c.numValues]
		}
	}

	c.numNulls = 0
	for i := 0; i < len(c.DLs); i++ {
		if c.DLs[i] != int64(c.maxDL) {
			c.numNulls++
		}
	}

	c.numRows = 0
	for i := 0; i < len(c.RLs); i++ {
		if c.RLs[i] == 0 {
			c.numRows++
		}
	}

	return data[bytesReader.Len():], nil
}

func (c *column) loadIndices() (err error) {
	var data []byte
	if c.dataPageHeader.Type == parquet.PageType_DATA_PAGE_V2 {
		data, err = c.readPageV2()
	} else {
		data, err = c.readPageV1()
	}
	if err != nil {
		return err
	}

	c.indexValues, err = encoding.RLEBitPackedHybridDecode(bytes.NewReader(data[1:]), 0, uint64(data[0]))
	return err
}

func (c *column) setDataPageHeader(pageHeader *parquet.PageHeader) (err error) {
	c.dataPageHeader = pageHeader

	var maxValue, minValue *Value

	if c.dictValues != nil {
		if err = c.loadIndices(); err != nil {
			return err
		}

		switch c.chunk.MetaData.Type {
		case parquet.Type_BOOLEAN:
			bs := c.dictValues.([]bool)
			max := bs[c.indexValues[0]]
			min := bs[c.indexValues[0]]
			for _, i := range c.indexValues[1:] {
				if bs[i] {
					max = bs[i]
				} else {
					min = bs[i]
				}
			}
			maxValue = NewBool(max)
			minValue = NewBool(min)

		case parquet.Type_INT32:
			i32s := c.dictValues.([]int32)
			max := i32s[c.indexValues[0]]
			min := i32s[c.indexValues[0]]
			for _, i := range c.indexValues[1:] {
				if i32s[i] > max {
					max = i32s[i]
				}
				if i32s[i] < min {
					min = i32s[i]
				}
			}
			maxValue = NewInt(int64(max))
			minValue = NewInt(int64(min))

		case parquet.Type_INT64:
			i64s := c.dictValues.([]int64)
			max := i64s[c.indexValues[0]]
			min := i64s[c.indexValues[0]]
			for _, i := range c.indexValues[1:] {
				if i64s[i] > max {
					max = i64s[i]
				}
				if i64s[i] < min {
					min = i64s[i]
				}
			}
			maxValue = NewInt(max)
			minValue = NewInt(min)

		case parquet.Type_FLOAT:
			f32s := c.dictValues.([]float32)
			max := f32s[c.indexValues[0]]
			min := f32s[c.indexValues[0]]
			for _, i := range c.indexValues[1:] {
				if f32s[i] > max {
					max = f32s[i]
				}
				if f32s[i] < min {
					min = f32s[i]
				}
			}
			maxValue = NewFloat(float64(max))
			minValue = NewFloat(float64(min))

		case parquet.Type_DOUBLE:
			f64s := c.dictValues.([]float64)
			max := f64s[c.indexValues[0]]
			min := f64s[c.indexValues[0]]
			for _, i := range c.indexValues[1:] {
				if f64s[i] > max {
					max = f64s[i]
				}
				if f64s[i] < min {
					min = f64s[i]
				}
			}
			maxValue = NewFloat(max)
			minValue = NewFloat(min)

		case parquet.Type_INT96, parquet.Type_BYTE_ARRAY, parquet.Type_FIXED_LEN_BYTE_ARRAY:
			byteSlices := c.dictValues.([][]byte)
			max := byteSlices[c.indexValues[0]]
			min := byteSlices[c.indexValues[0]]
			for _, i := range c.indexValues[1:] {
				if bytes.Compare(byteSlices[i], max) > 0 {
					max = byteSlices[i]
				}
				if bytes.Compare(byteSlices[i], min) < 0 {
					min = byteSlices[i]
				}
			}
			maxValue = NewString(string(max))
			minValue = NewString(string(min))

		default:
			panic(fmt.Errorf("invalid parquet type %v", c.chunk.MetaData.Type))
		}
	} else {
		var statistics *parquet.Statistics
		switch c.dataPageHeader.Type {
		case parquet.PageType_DATA_PAGE:
			if c.data, err = c.readPageV1(); err != nil {
				return err
			}
			statistics = c.dataPageHeader.DataPageHeader.Statistics
		case parquet.PageType_DATA_PAGE_V2:
			c.numValues = uint64(c.dataPageHeader.DataPageHeaderV2.NumValues)
			c.numNulls = uint64(c.dataPageHeader.DataPageHeaderV2.NumNulls)
			c.numRows = uint64(c.dataPageHeader.DataPageHeaderV2.NumRows)
			statistics = c.dataPageHeader.DataPageHeaderV2.Statistics
		}

		if statistics != nil {
			if maxValue, err = getMaxValue(statistics, c.chunk.MetaData.Type); err != nil {
				return err
			}

			if minValue, err = getMinValue(statistics, c.chunk.MetaData.Type); err != nil {
				return err
			}
		}
	}

	c.statistics.max = maxValue
	c.statistics.min = minValue
	c.statistics.numValues = c.numValues
	c.statistics.numNulls = c.numNulls
	c.statistics.numRows = c.numRows

	return nil
}

func (c *column) readChunk() (err error) {
	offset := c.chunk.MetaData.DataPageOffset
	if c.chunk.MetaData.DictionaryPageOffset != nil {
		offset = *c.chunk.MetaData.DictionaryPageOffset
	}
	size := c.chunk.MetaData.TotalCompressedSize

	if c.rc, err = c.getReaderFunc(offset, size); err != nil {
		return err
	}

	c.thriftReader = thrift.NewTBufferedTransport(thrift.NewStreamTransportR(c.rc), int(size))
	return nil
}

func (c *column) readPageHeader() (*parquet.PageHeader, error) {
	pageHeader := parquet.NewPageHeader()
	if err := pageHeader.Read(thrift.NewTCompactProtocol(c.thriftReader)); err != nil {
		c.rc.Close()
		return nil, err
	}

	if pageHeader.Type == parquet.PageType_INDEX_PAGE {
		return nil, fmt.Errorf("unsupported page type %v", parquet.PageType_INDEX_PAGE)
	}

	return pageHeader, nil
}

func (c *column) Read() (err error) {
	if err = c.readChunk(); err != nil {
		return err
	}

	pageHeader, err := c.readPageHeader()
	if err != nil {
		return err
	}

	if c.chunk.MetaData.DictionaryPageOffset == nil {
		return c.setDataPageHeader(pageHeader)
	}

	// As we got dictionary page, its required to uncompress/decode to values to find max/min values pointed in data pages.
	data := make([]byte, pageHeader.CompressedPageSize)
	if _, err = c.thriftReader.Read(data); err != nil {
		return err
	}

	if data, err = common.Uncompress(c.chunk.MetaData.Codec, data); err != nil {
		return err
	}

	if c.dictValues, err = encoding.PlainDecode(bytes.NewReader(data), c.chunk.MetaData.Type,
		uint64(pageHeader.DictionaryPageHeader.NumValues), 0); err != nil {
		return err
	}

	// Here pageHeader must be DATA_PAGE or DATA_PAGE_V2.
	if pageHeader, err = c.readPageHeader(); err != nil {
		return err
	}

	return c.setDataPageHeader(pageHeader)
}

func (c *column) SkipPage() error {
	if c.dictValues == nil && c.dataPageHeader.Type == parquet.PageType_DATA_PAGE_V2 {
		RLData := make([]byte, c.dataPageHeader.DataPageHeaderV2.RepetitionLevelsByteLength)
		if _, err := c.thriftReader.Read(RLData); err != nil {
			return err
		}

		DLData := make([]byte, c.dataPageHeader.DataPageHeaderV2.DefinitionLevelsByteLength)
		if _, err := c.thriftReader.Read(DLData); err != nil {
			return err
		}

		size := c.dataPageHeader.CompressedPageSize - c.dataPageHeader.DataPageHeaderV2.RepetitionLevelsByteLength -
			c.dataPageHeader.DataPageHeaderV2.DefinitionLevelsByteLength

		data := make([]byte, size)
		if _, err := c.thriftReader.Read(data); err != nil {
			return err
		}
	}

	pageHeader, err := c.readPageHeader()
	if err != nil {
		return err
	}

	return c.setDataPageHeader(pageHeader)
}

func (c *column) loadValuesByIndices() {
	c.values = make([]*Value, len(c.indexValues))

	switch c.chunk.MetaData.Type {
	case parquet.Type_BOOLEAN:
		bs := c.dictValues.([]bool)
		for _, i := range c.indexValues {
			c.values[i] = NewBool(bs[i])
		}

	case parquet.Type_INT32:
		i32s := c.dictValues.([]int32)
		for _, i := range c.indexValues {
			c.values[i] = NewInt(int64(i32s[i]))
		}

	case parquet.Type_INT64:
		i64s := c.dictValues.([]int64)
		for _, i := range c.indexValues {
			c.values[i] = NewInt(i64s[i])
		}

	case parquet.Type_FLOAT:
		f32s := c.dictValues.([]float32)
		for _, i := range c.indexValues {
			c.values[i] = NewFloat(float64(f32s[i]))
		}

	case parquet.Type_DOUBLE:
		f64s := c.dictValues.([]float64)
		for _, i := range c.indexValues {
			c.values[i] = NewFloat(f64s[i])
		}

	case parquet.Type_INT96, parquet.Type_BYTE_ARRAY, parquet.Type_FIXED_LEN_BYTE_ARRAY:
		byteSlices := c.dictValues.([][]byte)
		for _, i := range c.indexValues {
			c.values[i] = NewString(string(byteSlices[i]))
		}

	default:
		panic(fmt.Errorf("invalid parquet type %v", c.chunk.MetaData.Type))
	}

	c.indexValues = nil
	c.dictValues = nil
}

func (c *column) loadValues() (err error) {
	var data []byte
	var pageEncoding parquet.Encoding
	if c.dataPageHeader.Type == parquet.PageType_DATA_PAGE_V2 {
		data, err = c.readPageV2()
		pageEncoding = c.dataPageHeader.DataPageHeaderV2.Encoding
	} else {
		data = c.data
		c.data = nil
		pageEncoding = c.dataPageHeader.DataPageHeader.Encoding
	}

	if err != nil {
		return err
	}

	values, err := encoding.Decode(bytes.NewReader(data), pageEncoding, c.chunk.MetaData.Type,
		c.numValues-c.numNulls, c.typeLength)
	if err != nil {
		return err
	}

	c.values = make([]*Value, c.numValues-c.numNulls)

	switch c.chunk.MetaData.Type {
	case parquet.Type_BOOLEAN:
		bs := values.([]bool)
		for i, b := range bs {
			c.values[i] = NewBool(b)
		}

	case parquet.Type_INT32:
		i32s := values.([]int32)
		for i, i32 := range i32s {
			c.values[i] = NewInt(int64(i32))
		}

	case parquet.Type_INT64:
		i64s := values.([]int64)
		for i, i64 := range i64s {
			c.values[i] = NewInt(i64)
		}

	case parquet.Type_FLOAT:
		f32s := values.([]float32)
		for i, f32 := range f32s {
			c.values[i] = NewFloat(float64(f32))
		}

	case parquet.Type_DOUBLE:
		f64s := values.([]float64)
		for i, f64 := range f64s {
			c.values[i] = NewFloat(f64)
		}

	case parquet.Type_INT96, parquet.Type_BYTE_ARRAY, parquet.Type_FIXED_LEN_BYTE_ARRAY:
		byteSlices := values.([][]byte)
		for i, byteSlice := range byteSlices {
			c.values[i] = NewString(string(byteSlice))
		}

	default:
		panic(fmt.Errorf("invalid parquet type %v", c.chunk.MetaData.Type))
	}

	return nil
}

func (c *column) DecodePage() (err error) {
	if c.dictValues == nil {
		if err = c.loadValues(); err != nil {
			return err
		}
	} else {
		c.loadValuesByIndices()
	}

	c.numRows = uint64(0)
	for _, RL := range c.RLs {
		if RL == 0 {
			c.numRows++
		}
	}

	return nil
}

func (c *column) Close() error {
	return c.rc.Close()
}

func (c *column) GetStatistics() *Statistics {
	return c.statistics
}

func (c *column) NumRows() uint64 {
	return c.numRows
}

func newColumn(chunk *parquet.ColumnChunk, getReaderFunc GetReaderFunc, typeLength uint64, maxRL, maxDL int32, convertedType *parquet.ConvertedType) *column {
	// FIXME: add support to read partitioned data set.
	if chunk.FilePath != nil {
		return nil
	}

	var err error
	statistics := &Statistics{
		numValues: uint64(chunk.MetaData.NumValues),
	}
	if statistics.max, err = getMaxValue(chunk.MetaData.Statistics, chunk.MetaData.Type); err == nil {
		statistics.min, _ = getMinValue(chunk.MetaData.Statistics, chunk.MetaData.Type)
	}

	return &column{
		chunk:         chunk,
		getReaderFunc: getReaderFunc,
		typeLength:    typeLength,
		maxRL:         maxRL,
		maxDL:         maxDL,
		convertedType: convertedType,
		statistics:    statistics,
	}
}

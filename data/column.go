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

package data

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/minio/parquet-go/common"
	"github.com/minio/parquet-go/encoding"
	"github.com/minio/parquet-go/gen-go/parquet"
	"github.com/minio/parquet-go/schema"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

func getDefaultEncoding(parquetType parquet.Type) parquet.Encoding {
	switch parquetType {
	case parquet.Type_BOOLEAN:
		return parquet.Encoding_PLAIN
	case parquet.Type_INT32, parquet.Type_INT64, parquet.Type_FLOAT, parquet.Type_DOUBLE:
		return parquet.Encoding_RLE_DICTIONARY
	case parquet.Type_BYTE_ARRAY:
		return parquet.Encoding_DELTA_LENGTH_BYTE_ARRAY
	}

	return parquet.Encoding_PLAIN
}

func getFirstValueElement(tree *schema.Tree) (valueElement *schema.Element) {
	tree.Range(func(name string, element *schema.Element) bool {
		if element.Children == nil {
			valueElement = element
		} else {
			valueElement = getFirstValueElement(element.Children)
		}

		return false
	})

	return valueElement
}

func populate(columnDataMap map[string]*Column, input *jsonValue, tree *schema.Tree, firstValueRL int64) (map[string]*Column, error) {
	var err error

	pos := 0
	handleElement := func(name string, element *schema.Element) bool {
		pos++

		dataPath := element.PathInTree

		if *element.RepetitionType == parquet.FieldRepetitionType_REPEATED {
			panic(fmt.Errorf("%v: repetition type must be REQUIRED or OPTIONAL type", dataPath))
		}

		inputValue := input.Get(name)
		if *element.RepetitionType == parquet.FieldRepetitionType_REQUIRED && inputValue.IsNull() {
			err = fmt.Errorf("%v: nil value for required field", dataPath)
			return false
		}

		add := func(element *schema.Element, value interface{}, DL, RL int64) {
			columnData := columnDataMap[element.PathInSchema]
			if columnData == nil {
				columnData = NewColumn(*element.Type)
			}
			columnData.add(value, DL, RL)
			columnDataMap[element.PathInSchema] = columnData
		}

		// Handle primitive type element.
		if element.Type != nil {
			var value interface{}
			if value, err = inputValue.GetValue(*element.Type, element.ConvertedType); err != nil {
				return false
			}

			DL := element.MaxDefinitionLevel
			if value == nil && DL > 0 {
				DL--
			}

			RL := element.MaxRepetitionLevel
			if pos == 1 {
				RL = firstValueRL
			}

			add(element, value, DL, RL)
			return true
		}

		addNull := func() {
			valueElement := getFirstValueElement(element.Children)

			DL := element.MaxDefinitionLevel
			if DL > 0 {
				DL--
			}

			RL := element.MaxRepetitionLevel
			if RL > 0 {
				RL--
			}

			add(valueElement, nil, DL, RL)
		}

		// Handle group type element.
		if element.ConvertedType == nil {
			if inputValue.IsNull() {
				addNull()
				return true
			}

			columnDataMap, err = populate(columnDataMap, inputValue, element.Children, firstValueRL)
			return (err == nil)
		}

		// Handle list type element.
		if *element.ConvertedType == parquet.ConvertedType_LIST {
			if inputValue.IsNull() {
				addNull()
				return true
			}

			var results []gjson.Result
			if results, err = inputValue.GetArray(); err != nil {
				return false
			}

			listElement, _ := element.Children.Get("list")
			valueElement, _ := listElement.Children.Get("element")
			for i := range results {
				rl := valueElement.MaxRepetitionLevel
				if i == 0 {
					rl = firstValueRL
				}

				var jsonData []byte
				if jsonData, err = sjson.SetBytes([]byte{}, "element", results[i].Value()); err != nil {
					return false
				}

				var jv *jsonValue
				if jv, err = bytesToJSONValue(jsonData); err != nil {
					return false
				}

				if columnDataMap, err = populate(columnDataMap, jv, listElement.Children, rl); err != nil {
					return false
				}
			}
			return true
		}

		if *element.ConvertedType == parquet.ConvertedType_MAP {
			if inputValue.IsNull() {
				addNull()
				return true
			}

			keyValueElement, _ := element.Children.Get("key_value")
			var rerr error
			err = inputValue.Range(func(key, value gjson.Result) bool {
				if !key.Exists() || key.Type == gjson.Null {
					rerr = fmt.Errorf("%v.key_value.key: not found or null", dataPath)
					return false
				}

				var jsonData []byte
				if jsonData, rerr = sjson.SetBytes([]byte{}, "key", key.Value()); err != nil {
					return false
				}

				if jsonData, rerr = sjson.SetBytes(jsonData, "value", value.Value()); err != nil {
					return false
				}

				var jv *jsonValue
				if jv, rerr = bytesToJSONValue(jsonData); rerr != nil {
					return false
				}

				if columnDataMap, rerr = populate(columnDataMap, jv, keyValueElement.Children, firstValueRL); err != nil {
					return false
				}

				return true
			})

			if err != nil {
				return false
			}

			err = rerr
			return (err == nil)
		}

		err = fmt.Errorf("%v: unsupported converted type %v in %v field type", dataPath, *element.ConvertedType, *element.RepetitionType)
		return false
	}

	tree.Range(handleElement)
	return columnDataMap, err
}

// Column - denotes values of a column.
type Column struct {
	parquetType      parquet.Type  // value type.
	values           []interface{} // must be a slice of parquet typed values.
	definitionLevels []int64       // exactly same length of values.
	repetitionLevels []int64       // exactly same length of values.
	rowCount         int
	nullCount        int
	maxBitWidth      int
	minValue         interface{}
	maxValue         interface{}
}

func (data *Column) updateMinMaxValue(value interface{}) {
	if data.minValue == nil && data.maxValue == nil {
		data.minValue = value
		data.maxValue = value
		return
	}

	switch data.parquetType {
	case parquet.Type_BOOLEAN:
		if data.minValue.(bool) && !value.(bool) {
			data.minValue = value
		}

		if !data.maxValue.(bool) && value.(bool) {
			data.maxValue = value
		}

	case parquet.Type_INT32:
		if data.minValue.(int32) > value.(int32) {
			data.minValue = value
		}

		if data.maxValue.(int32) < value.(int32) {
			data.maxValue = value
		}

	case parquet.Type_INT64:
		if data.minValue.(int64) > value.(int64) {
			data.minValue = value
		}

		if data.maxValue.(int64) < value.(int64) {
			data.maxValue = value
		}

	case parquet.Type_FLOAT:
		if data.minValue.(float32) > value.(float32) {
			data.minValue = value
		}

		if data.maxValue.(float32) < value.(float32) {
			data.maxValue = value
		}

	case parquet.Type_DOUBLE:
		if data.minValue.(float64) > value.(float64) {
			data.minValue = value
		}

		if data.maxValue.(float64) < value.(float64) {
			data.maxValue = value
		}

	case parquet.Type_BYTE_ARRAY:
		if bytes.Compare(data.minValue.([]byte), value.([]byte)) > 0 {
			data.minValue = value
		}

		if bytes.Compare(data.minValue.([]byte), value.([]byte)) < 0 {
			data.maxValue = value
		}
	}
}

func (data *Column) updateStats(value interface{}, DL, RL int64) {
	if RL == 0 {
		data.rowCount++
	}

	if value == nil {
		data.nullCount++
		return
	}

	var bitWidth int
	switch data.parquetType {
	case parquet.Type_BOOLEAN:
		bitWidth = 1
	case parquet.Type_INT32:
		bitWidth = common.BitWidth(uint64(value.(int32)))
	case parquet.Type_INT64:
		bitWidth = common.BitWidth(uint64(value.(int64)))
	case parquet.Type_FLOAT:
		bitWidth = 32
	case parquet.Type_DOUBLE:
		bitWidth = 64
	case parquet.Type_BYTE_ARRAY:
		bitWidth = len(value.([]byte))
	}
	if data.maxBitWidth < bitWidth {
		data.maxBitWidth = bitWidth
	}

	data.updateMinMaxValue(value)
}

func (data *Column) add(value interface{}, DL, RL int64) {
	data.values = append(data.values, value)
	data.definitionLevels = append(data.definitionLevels, DL)
	data.repetitionLevels = append(data.repetitionLevels, RL)
	data.updateStats(value, DL, RL)
}

// AddNull - adds nil value.
func (data *Column) AddNull(DL, RL int64) {
	data.add(nil, DL, RL)
}

// AddBoolean - adds boolean value.
func (data *Column) AddBoolean(value bool, DL, RL int64) {
	if data.parquetType != parquet.Type_BOOLEAN {
		panic(fmt.Errorf("expected %v value", data.parquetType))
	}

	data.add(value, DL, RL)
}

// AddInt32 - adds int32 value.
func (data *Column) AddInt32(value int32, DL, RL int64) {
	if data.parquetType != parquet.Type_INT32 {
		panic(fmt.Errorf("expected %v value", data.parquetType))
	}

	data.add(value, DL, RL)
}

// AddInt64 - adds int64 value.
func (data *Column) AddInt64(value int64, DL, RL int64) {
	if data.parquetType != parquet.Type_INT64 {
		panic(fmt.Errorf("expected %v value", data.parquetType))
	}

	data.add(value, DL, RL)
}

// AddFloat - adds float32 value.
func (data *Column) AddFloat(value float32, DL, RL int64) {
	if data.parquetType != parquet.Type_FLOAT {
		panic(fmt.Errorf("expected %v value", data.parquetType))
	}

	data.add(value, DL, RL)
}

// AddDouble - adds float64 value.
func (data *Column) AddDouble(value float64, DL, RL int64) {
	if data.parquetType != parquet.Type_DOUBLE {
		panic(fmt.Errorf("expected %v value", data.parquetType))
	}

	data.add(value, DL, RL)
}

// AddByteArray - adds byte array value.
func (data *Column) AddByteArray(value []byte, DL, RL int64) {
	if data.parquetType != parquet.Type_BYTE_ARRAY {
		panic(fmt.Errorf("expected %v value", data.parquetType))
	}

	data.add(value, DL, RL)
}

// Merge - merges column data.
func (data *Column) Merge(dataToMerge *Column) {
	if data.parquetType != dataToMerge.parquetType {
		panic(fmt.Errorf("merge differs in parquet type"))
	}

	data.values = append(data.values, dataToMerge.values...)
	data.definitionLevels = append(data.definitionLevels, dataToMerge.definitionLevels...)
	data.repetitionLevels = append(data.repetitionLevels, dataToMerge.repetitionLevels...)

	data.rowCount += dataToMerge.rowCount
	data.nullCount += dataToMerge.nullCount
	if data.maxBitWidth < dataToMerge.maxBitWidth {
		data.maxBitWidth = dataToMerge.maxBitWidth
	}

	data.updateMinMaxValue(dataToMerge.minValue)
	data.updateMinMaxValue(dataToMerge.maxValue)
}

func (data *Column) String() string {
	var strs []string
	strs = append(strs, fmt.Sprintf("parquetType: %v", data.parquetType))
	strs = append(strs, fmt.Sprintf("values: %v", data.values))
	strs = append(strs, fmt.Sprintf("definitionLevels: %v", data.definitionLevels))
	strs = append(strs, fmt.Sprintf("repetitionLevels: %v", data.repetitionLevels))
	strs = append(strs, fmt.Sprintf("rowCount: %v", data.rowCount))
	strs = append(strs, fmt.Sprintf("nullCount: %v", data.nullCount))
	strs = append(strs, fmt.Sprintf("maxBitWidth: %v", data.maxBitWidth))
	strs = append(strs, fmt.Sprintf("minValue: %v", data.minValue))
	strs = append(strs, fmt.Sprintf("maxValue: %v", data.maxValue))
	return "{" + strings.Join(strs, ", ") + "}"
}

func (data *Column) encodeValue(value interface{}, element *schema.Element) []byte {
	if value == nil {
		return nil
	}

	valueData := encoding.PlainEncode(common.ToSliceValue([]interface{}{value}, data.parquetType), data.parquetType)
	if data.parquetType == parquet.Type_BYTE_ARRAY && element.ConvertedType != nil {
		switch *element.ConvertedType {
		case parquet.ConvertedType_UTF8, parquet.ConvertedType_DECIMAL:
			valueData = valueData[4:]
		}
	}

	return valueData
}

func (data *Column) toDataPageV2(element *schema.Element, parquetEncoding parquet.Encoding) *ColumnChunk {
	var definedValues []interface{}
	for _, value := range data.values {
		if value != nil {
			definedValues = append(definedValues, value)
		}
	}

	var encodedData []byte
	switch parquetEncoding {
	case parquet.Encoding_PLAIN:
		encodedData = encoding.PlainEncode(common.ToSliceValue(definedValues, data.parquetType), data.parquetType)

	case parquet.Encoding_DELTA_LENGTH_BYTE_ARRAY:
		var bytesSlices [][]byte
		for _, value := range data.values {
			bytesSlices = append(bytesSlices, value.([]byte))
		}
		encodedData = encoding.DeltaLengthByteArrayEncode(bytesSlices)
	}

	compressionType := parquet.CompressionCodec_SNAPPY
	if element.CompressionType != nil {
		compressionType = *element.CompressionType
	}

	compressedData, err := common.Compress(compressionType, encodedData)
	if err != nil {
		panic(err)
	}

	DLData := encoding.RLEBitPackedHybridEncode(
		data.definitionLevels,
		int32(common.BitWidth(uint64(element.MaxDefinitionLevel))),
		parquet.Type_INT64,
	)

	RLData := encoding.RLEBitPackedHybridEncode(
		data.repetitionLevels,
		int32(common.BitWidth(uint64(element.MaxRepetitionLevel))),
		parquet.Type_INT64,
	)

	pageHeader := parquet.NewPageHeader()
	pageHeader.Type = parquet.PageType_DATA_PAGE_V2
	pageHeader.CompressedPageSize = int32(len(compressedData) + len(DLData) + len(RLData))
	pageHeader.UncompressedPageSize = int32(len(encodedData) + len(DLData) + len(RLData))
	pageHeader.DataPageHeaderV2 = parquet.NewDataPageHeaderV2()
	pageHeader.DataPageHeaderV2.NumValues = int32(len(data.values))
	pageHeader.DataPageHeaderV2.NumNulls = int32(len(data.values) - len(definedValues))
	pageHeader.DataPageHeaderV2.NumRows = int32(data.rowCount)
	pageHeader.DataPageHeaderV2.Encoding = parquetEncoding
	pageHeader.DataPageHeaderV2.DefinitionLevelsByteLength = int32(len(DLData))
	pageHeader.DataPageHeaderV2.RepetitionLevelsByteLength = int32(len(RLData))
	pageHeader.DataPageHeaderV2.IsCompressed = true
	pageHeader.DataPageHeaderV2.Statistics = parquet.NewStatistics()
	pageHeader.DataPageHeaderV2.Statistics.Min = data.encodeValue(data.minValue, element)
	pageHeader.DataPageHeaderV2.Statistics.Max = data.encodeValue(data.maxValue, element)

	ts := thrift.NewTSerializer()
	ts.Protocol = thrift.NewTCompactProtocolFactory().GetProtocol(ts.Transport)
	rawData, err := ts.Write(context.TODO(), pageHeader)
	if err != nil {
		panic(err)
	}
	rawData = append(rawData, RLData...)
	rawData = append(rawData, DLData...)
	rawData = append(rawData, compressedData...)

	metadata := parquet.NewColumnMetaData()
	metadata.Type = data.parquetType
	metadata.Encodings = []parquet.Encoding{
		parquet.Encoding_PLAIN,
		parquet.Encoding_RLE,
		parquet.Encoding_DELTA_LENGTH_BYTE_ARRAY,
	}
	metadata.Codec = compressionType
	metadata.NumValues = int64(pageHeader.DataPageHeaderV2.NumValues)
	metadata.TotalCompressedSize = int64(len(rawData))
	metadata.TotalUncompressedSize = int64(pageHeader.UncompressedPageSize) + int64(len(rawData)) - int64(pageHeader.CompressedPageSize)
	metadata.PathInSchema = strings.Split(element.PathInSchema, ".")
	metadata.Statistics = parquet.NewStatistics()
	metadata.Statistics.Min = pageHeader.DataPageHeaderV2.Statistics.Min
	metadata.Statistics.Max = pageHeader.DataPageHeaderV2.Statistics.Max

	chunk := new(ColumnChunk)
	chunk.ColumnChunk.MetaData = metadata
	chunk.dataPageLen = int64(len(rawData))
	chunk.dataLen = int64(len(rawData))
	chunk.data = rawData

	return chunk
}

func (data *Column) toRLEDictPage(element *schema.Element) *ColumnChunk {
	dictPageData, dataPageData, valueCount, _, indexBitWidth := encoding.RLEDictEncode(data.values, data.parquetType, int32(data.maxBitWidth))

	compressionType := parquet.CompressionCodec_SNAPPY
	if element.CompressionType != nil {
		compressionType = *element.CompressionType
	}

	compressedData, err := common.Compress(compressionType, dictPageData)
	if err != nil {
		panic(err)
	}

	dictPageHeader := parquet.NewPageHeader()
	dictPageHeader.Type = parquet.PageType_DICTIONARY_PAGE
	dictPageHeader.CompressedPageSize = int32(len(compressedData))
	dictPageHeader.UncompressedPageSize = int32(len(dictPageData))
	dictPageHeader.DictionaryPageHeader = parquet.NewDictionaryPageHeader()
	dictPageHeader.DictionaryPageHeader.NumValues = int32(valueCount)
	dictPageHeader.DictionaryPageHeader.Encoding = parquet.Encoding_PLAIN

	ts := thrift.NewTSerializer()
	ts.Protocol = thrift.NewTCompactProtocolFactory().GetProtocol(ts.Transport)
	dictPageRawData, err := ts.Write(context.TODO(), dictPageHeader)
	if err != nil {
		panic(err)
	}
	dictPageRawData = append(dictPageRawData, compressedData...)

	RLData := encoding.RLEBitPackedHybridEncode(
		data.repetitionLevels,
		int32(common.BitWidth(uint64(element.MaxRepetitionLevel))),
		parquet.Type_INT64,
	)
	encodedData := RLData

	DLData := encoding.RLEBitPackedHybridEncode(
		data.definitionLevels,
		int32(common.BitWidth(uint64(element.MaxDefinitionLevel))),
		parquet.Type_INT64,
	)
	encodedData = append(encodedData, DLData...)

	encodedData = append(encodedData, byte(indexBitWidth))
	encodedData = append(encodedData, dataPageData...)

	compressedData, err = common.Compress(compressionType, encodedData)
	if err != nil {
		panic(err)
	}

	dataPageHeader := parquet.NewPageHeader()
	dataPageHeader.Type = parquet.PageType_DATA_PAGE
	dataPageHeader.CompressedPageSize = int32(len(compressedData))
	dataPageHeader.UncompressedPageSize = int32(len(encodedData))
	dataPageHeader.DataPageHeader = parquet.NewDataPageHeader()
	dataPageHeader.DataPageHeader.NumValues = int32(len(data.values))
	dataPageHeader.DataPageHeader.DefinitionLevelEncoding = parquet.Encoding_RLE
	dataPageHeader.DataPageHeader.RepetitionLevelEncoding = parquet.Encoding_RLE
	dataPageHeader.DataPageHeader.Encoding = parquet.Encoding_RLE_DICTIONARY

	ts = thrift.NewTSerializer()
	ts.Protocol = thrift.NewTCompactProtocolFactory().GetProtocol(ts.Transport)
	dataPageRawData, err := ts.Write(context.TODO(), dataPageHeader)
	if err != nil {
		panic(err)
	}
	dataPageRawData = append(dataPageRawData, compressedData...)

	metadata := parquet.NewColumnMetaData()
	metadata.Type = data.parquetType
	metadata.Encodings = []parquet.Encoding{
		parquet.Encoding_PLAIN,
		parquet.Encoding_RLE,
		parquet.Encoding_DELTA_LENGTH_BYTE_ARRAY,
		parquet.Encoding_RLE_DICTIONARY,
	}
	metadata.Codec = compressionType
	metadata.NumValues = int64(dataPageHeader.DataPageHeader.NumValues)
	metadata.TotalCompressedSize = int64(len(dictPageRawData)) + int64(len(dataPageRawData))
	uncompressedSize := int64(dictPageHeader.UncompressedPageSize) + int64(len(dictPageData)) - int64(dictPageHeader.CompressedPageSize)
	uncompressedSize += int64(dataPageHeader.UncompressedPageSize) + int64(len(dataPageData)) - int64(dataPageHeader.CompressedPageSize)
	metadata.TotalUncompressedSize = uncompressedSize
	metadata.PathInSchema = strings.Split(element.PathInSchema, ".")
	metadata.Statistics = parquet.NewStatistics()
	metadata.Statistics.Min = data.encodeValue(data.minValue, element)
	metadata.Statistics.Max = data.encodeValue(data.maxValue, element)

	chunk := new(ColumnChunk)
	chunk.ColumnChunk.MetaData = metadata
	chunk.isDictPage = true
	chunk.dictPageLen = int64(len(dictPageRawData))
	chunk.dataPageLen = int64(len(dataPageRawData))
	chunk.dataLen = chunk.dictPageLen + chunk.dataPageLen
	chunk.data = append(dictPageRawData, dataPageRawData...)

	return chunk
}

func (data *Column) Encode(element *schema.Element) *ColumnChunk {
	parquetEncoding := getDefaultEncoding(data.parquetType)
	if element.Encoding != nil {
		parquetEncoding = *element.Encoding
	}

	switch parquetEncoding {
	case parquet.Encoding_PLAIN, parquet.Encoding_DELTA_LENGTH_BYTE_ARRAY:
		return data.toDataPageV2(element, parquetEncoding)
	}

	return data.toRLEDictPage(element)
}

// NewColumn - creates new column data
func NewColumn(parquetType parquet.Type) *Column {
	switch parquetType {
	case parquet.Type_BOOLEAN, parquet.Type_INT32, parquet.Type_INT64, parquet.Type_FLOAT, parquet.Type_DOUBLE, parquet.Type_BYTE_ARRAY:
	default:
		panic(fmt.Errorf("unsupported parquet type %v", parquetType))
	}

	return &Column{
		parquetType: parquetType,
	}
}

// UnmarshalJSON - decodes JSON data into map of ColumnData.
func UnmarshalJSON(data []byte, tree *schema.Tree) (map[string]*Column, error) {
	if !tree.ReadOnly() {
		return nil, fmt.Errorf("tree must be read only")
	}

	inputValue, err := bytesToJSONValue(data)
	if err != nil {
		return nil, err
	}

	columnDataMap := make(map[string]*Column)
	return populate(columnDataMap, inputValue, tree, 0)
}

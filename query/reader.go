package query

import (
	"encoding/binary"
	"io"
	"strings"

	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/minio/minio-go/pkg/set"
	"github.com/minio/parquet-go/gen-go/parquet"
	"github.com/minio/parquet-go/schema"
)

// GetReaderFunc - function type returning io.ReadCloser for requested offset/length.
type GetReaderFunc func(offset, length int64) (io.ReadCloser, error)

func getFooterSize(getReaderFunc GetReaderFunc) (size int64, err error) {
	rc, err := getReaderFunc(-8, 4)
	if err != nil {
		return 0, err
	}
	defer rc.Close()

	buf := make([]byte, 4)
	if _, err = io.ReadFull(rc, buf); err != nil {
		return 0, err
	}

	size = int64(binary.LittleEndian.Uint32(buf))

	return size, nil
}

func getFileMetadata(getReaderFunc GetReaderFunc) (metadata *parquet.FileMetaData, err error) {
	var size int64
	if size, err = getFooterSize(getReaderFunc); err != nil {
		return nil, err
	}

	var rc io.ReadCloser
	if rc, err = getReaderFunc(-(8 + size), size); err != nil {
		return nil, err
	}
	defer rc.Close()

	metadata = parquet.NewFileMetaData()
	pf := thrift.NewTCompactProtocolFactory()
	protocol := pf.GetProtocol(thrift.NewStreamTransportR(rc))
	if err = metadata.Read(protocol); err != nil {
		return nil, err
	}

	return metadata, nil
}

// Reader - denotes parquet file.
type Reader struct {
	getReaderFunc GetReaderFunc
	fileMetadata  *parquet.FileMetaData
	schemaTree    *schema.Tree

	rowGroupIndex int
	eof           bool
	columnNames   set.StringSet
	columnMap     map[string]*column
}

// GetElement - get element of given name.
func (reader *Reader) GetElement(name string) (*schema.Element, bool) {
	return reader.schemaTree.Get(name)
}

func (reader *Reader) setColumnMetadata() error {
	if reader.eof {
		return io.EOF
	}

	columnMap := make(map[string]*column)
	rowGroup := reader.fileMetadata.RowGroups[reader.rowGroupIndex]
	for _, columnChunk := range rowGroup.Columns {
		columnName := strings.Join(columnChunk.MetaData.PathInSchema, ".")
		if reader.columnNames != nil && !reader.columnNames.Contains(columnName) {
			continue
		}

		element, _ := reader.schemaTree.Get(columnName)
		typeLength := uint64(0)
		if element.TypeLength != nil {
			typeLength = uint64(*element.TypeLength)
		}

		columnMap[columnName] = newColumn(columnChunk, reader.getReaderFunc,
			typeLength, int32(element.MaxRepetitionLevel), int32(element.MaxDefinitionLevel),
			element.ConvertedType)
	}

	// FIXME: close all columns if reader.columnMap is not empty.
	reader.columnMap = columnMap
	return nil
}

func (reader *Reader) SetRequiredColumns(columnNames set.StringSet) {
	reader.columnNames = columnNames
	reader.setColumnMetadata()
}

func (reader *Reader) SkipRowGroup() error {
	if reader.eof {
		return io.EOF
	}

	if reader.rowGroupIndex == len(reader.fileMetadata.RowGroups) {
		reader.eof = true
		return io.EOF
	}

	reader.rowGroupIndex++
	return reader.setColumnMetadata()
}

func (reader *Reader) GetStatistics(columnName string) (*Statistics, error) {
	if reader.eof {
		return nil, io.EOF
	}

	column := reader.columnMap[columnName]
	if column == nil {
		return nil, nil
	}

	return column.GetStatistics(), nil
}

func (reader *Reader) ReadColumns() error {
	if reader.eof {
		return io.EOF
	}

	for _, column := range reader.columnMap {
		if err := column.Read(); err != nil {
			// FIXME: close previously created column readers.
			return err
		}
	}

	return nil
}

func (reader *Reader) DecodeColumns() error {
	if reader.eof {
		return io.EOF
	}

	for _, column := range reader.columnMap {
		if err := column.DecodePage(); err != nil {
			// FIXME: close previously created column readers.
			return err
		}
	}

	return nil
}

func (reader *Reader) NumRows() (uint64, error) {
	if reader.eof {
		return 0, io.EOF
	}

	minNumRows := uint64(0)
	isFirstEntry := false
	for _, column := range reader.columnMap {
		numRows := column.NumRows()
		if !isFirstEntry {
			isFirstEntry = true
			minNumRows = numRows
			continue
		}

		if numRows < minNumRows {
			minNumRows = numRows
		}
	}

	return minNumRows, nil
}

func (reader *Reader) SkipPage() error {
	if reader.eof {
		return io.EOF
	}

	for _, column := range reader.columnMap {
		if err := column.SkipPage(); err != nil {
			// FIXME: close previously created column readers.
			return err
		}
	}

	return nil
}

// NewReader - creates new parquet reader.
func NewReader(getReaderFunc GetReaderFunc) (*Reader, error) {
	fileMetadata, err := getFileMetadata(getReaderFunc)
	if err != nil {
		return nil, err
	}

	schemaTree := schema.NewTree()
	if err = schemaTree.AddAll(fileMetadata.GetSchema()); err != nil {
		return nil, err
	}

	return &Reader{
		getReaderFunc: getReaderFunc,
		fileMetadata:  fileMetadata,
		schemaTree:    schemaTree,
	}, nil
}

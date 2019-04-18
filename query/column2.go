package query

import (
	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/minio/parquet-go/gen-go/parquet"
	"github.com/minio/parquet-go/schema"
)

type Column2 struct {
	// No. of rows in this column.  This value is got from row group.
	numRows int64

	// This column information in a row group.
	chunk *parquet.ColumnChunk

	// Schema element information.
	element *schema.Element

	// Function used to read data for column values.
	getReaderFunc GetReaderFunc

	// thrift wrapped reader got from above getReaderFunc.
	thriftReader *thrift.TBufferedTransport

	// Row index points to current row value in this column.
	rowIndex int64

	// When a data page is skipped, there is a possibility that last value in the page may spanned into next page.
	// Hence we skip a data page except last row value of this column. This flag denotes whether we need to skip
	// first value chunk of next data page.
	skipFirstValueChunk bool

	// No. of rows to skip in next (upcoming) pages.
	rowsToSkip int64

	pages []*DataPage
}

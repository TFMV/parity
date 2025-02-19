package utils

import (
	"log"
	"strconv"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// SingleRecordReader is a custom RecordReader that wraps a single arrow.Record.
type SingleRecordReader struct {
	record arrow.Record
	done   bool
}

// NewSingleRecordReader creates a new SingleRecordReader.
func NewSingleRecordReader(record arrow.Record) *SingleRecordReader {
	return &SingleRecordReader{record: record, done: false}
}

// Schema returns the schema of the record.
func (r *SingleRecordReader) Schema() *arrow.Schema {
	return r.record.Schema()
}

// Next advances to the next record (in this case, only one record is available).
func (r *SingleRecordReader) Next() bool {
	if r.done {
		return false
	}
	r.done = true
	return true
}

// Record returns the current record.
func (r *SingleRecordReader) Record() arrow.Record {
	return r.record
}

// Err always returns nil as there is no error state in this simple reader.
func (r *SingleRecordReader) Err() error {
	return nil
}

// Release releases resources associated with the reader.
func (r *SingleRecordReader) Release() {
	r.record.Release()
}

// Retain increases the reference count of the record.
func (r *SingleRecordReader) Retain() {
	r.record.Retain()
}

// Close releases resources associated with the SingleRecordReader.
func (r *SingleRecordReader) Close() error {
	return nil
}

func ConvertNumericFields(record arrow.Record) arrow.Record {
	for i := 0; i < int(record.NumCols()); i++ {
		col := record.Column(i)
		switch record.ColumnName(i) {
		case "dep_time", "arr_time":
			strArray, ok := col.(*array.String)
			if !ok {
				continue
			}

			floatBuilder := array.NewFloat64Builder(memory.DefaultAllocator)
			for j := 0; j < int(strArray.Len()); j++ {
				if strArray.IsNull(j) {
					floatBuilder.AppendNull()
				} else {
					val, err := strconv.ParseFloat(strArray.Value(j), 64)
					if err != nil {
						log.Printf("Warning: failed to convert %s to float64: %v", strArray.Value(j), err)
						floatBuilder.AppendNull()
					} else {
						floatBuilder.Append(val)
					}
				}
			}

			newColumn := floatBuilder.NewArray()
			record.SetColumn(i, newColumn)
		}
	}
	return record
}

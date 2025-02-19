package utils

import (
	"github.com/apache/arrow-go/v18/arrow"
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

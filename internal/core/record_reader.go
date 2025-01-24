package core

import (
	"errors"
	"reflect"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

type Reader[T any] struct {
	records []arrow.Record
}

func NewReader[T any](records ...arrow.Record) *Reader[T] {
	var a T
	r := reflect.TypeOf(a)
	for r.Kind() == reflect.Ptr {
		r = r.Elem()
	}
	if r.Kind() != reflect.Struct {
		panic("parity/dynschema: " + r.String() + " is not supported")
	}

	return &Reader[T]{records: records}
}

func (r *Reader[T]) NumRows() int64 {
	var rows int64
	for _, record := range r.records {
		rows += record.NumRows()
	}
	return rows
}

func (r *Reader[T]) Value(i int) (T, error) {
	var row T
	rowType := reflect.TypeOf(row)

	// Locate the target record
	var record arrow.Record
	var previousRows int64
	for _, rec := range r.records {
		if i < int(previousRows+rec.NumRows()) {
			record = rec
			i -= int(previousRows)
			break
		}
		previousRows += rec.NumRows()
	}
	if record == nil {
		return row, errors.New("index out of range")
	}

	// Populate the struct fields
	rowVal := reflect.ValueOf(&row).Elem()
	for j := 0; j < rowType.NumField(); j++ {
		field := rowType.Field(j)

		// Get the field name from the `arrow` tag or default to the struct field name
		fieldName := field.Tag.Get("arrow")
		if fieldName == "" {
			fieldName = field.Name
		}

		// Get the corresponding column for the field
		indices := record.Schema().FieldIndices(fieldName)
		if len(indices) != 1 {
			return row, errors.New("field " + fieldName + " not found or ambiguous")
		}
		col := record.Column(indices[0])

		// Set the value if the field is not null
		if err := setValue(rowVal.Field(j), col, i); err != nil {
			return row, err
		}
	}

	return row, nil
}

func setValue(field reflect.Value, col arrow.Array, idx int) error {
	if col.IsNull(idx) {
		return nil
	}

	switch arr := col.(type) {
	case *array.Boolean:
		field.SetBool(arr.Value(idx))
	case *array.Float32:
		field.SetFloat(float64(arr.Value(idx)))
	case *array.Float64:
		field.SetFloat(arr.Value(idx))
	case *array.Int8:
		field.SetInt(int64(arr.Value(idx)))
	case *array.Int16:
		field.SetInt(int64(arr.Value(idx)))
	case *array.Int32:
		field.SetInt(int64(arr.Value(idx)))
	case *array.Int64:
		field.SetInt(arr.Value(idx))
	case *array.Uint8:
		field.SetUint(uint64(arr.Value(idx)))
	case *array.Uint16:
		field.SetUint(uint64(arr.Value(idx)))
	case *array.Uint32:
		field.SetUint(uint64(arr.Value(idx)))
	case *array.Uint64:
		field.SetUint(arr.Value(idx))
	case *array.String:
		field.SetString(arr.Value(idx))
	default:
		return errors.New("unsupported type " + reflect.TypeOf(col).String())
	}
	return nil
}

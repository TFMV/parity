// Package readers provides implementations of dataset readers for various data sources.
package readers

import (
	"fmt"

	"github.com/TFMV/parity/pkg/core"
)

// Factory creates a reader based on the given configuration.
type Factory struct {
	// registered readers by type
	readers map[string]Creator
}

// Creator is a function that creates a reader from a configuration.
type Creator func(config core.ReaderConfig) (core.DatasetReader, error)

// NewFactory creates a new reader factory.
func NewFactory() *Factory {
	return &Factory{
		readers: make(map[string]Creator),
	}
}

// Register registers a creator for a reader type.
func (f *Factory) Register(typ string, creator Creator) {
	f.readers[typ] = creator
}

// Create creates a reader based on the given configuration.
func (f *Factory) Create(config core.ReaderConfig) (core.DatasetReader, error) {
	creator, ok := f.readers[config.Type]
	if !ok {
		return nil, fmt.Errorf("unsupported reader type: %s", config.Type)
	}
	return creator(config)
}

// DefaultFactory is the default reader factory with built-in reader types.
var DefaultFactory = NewFactory()

// init registers built-in reader types.
func init() {
	DefaultFactory.Register("parquet", NewParquetReader)
	DefaultFactory.Register("arrow", NewArrowReader)
	DefaultFactory.Register("csv", NewCSVReader)
}

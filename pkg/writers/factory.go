// Package writers provides implementations of dataset writers for various data formats.
package writers

import (
	"fmt"

	"github.com/TFMV/parity/pkg/core"
)

// Factory creates a writer based on the given configuration.
type Factory struct {
	// registered writers by type
	writers map[string]Creator
}

// Creator is a function that creates a writer from a configuration.
type Creator func(config core.WriterConfig) (core.DatasetWriter, error)

// NewFactory creates a new writer factory.
func NewFactory() *Factory {
	return &Factory{
		writers: make(map[string]Creator),
	}
}

// Register registers a creator for a writer type.
func (f *Factory) Register(typ string, creator Creator) {
	f.writers[typ] = creator
}

// Create creates a writer based on the given configuration.
func (f *Factory) Create(config core.WriterConfig) (core.DatasetWriter, error) {
	creator, ok := f.writers[config.Type]
	if !ok {
		return nil, fmt.Errorf("unsupported writer type: %s", config.Type)
	}
	return creator(config)
}

// DefaultFactory is the default writer factory with built-in writer types.
var DefaultFactory = NewFactory()

// init registers built-in writer types.
func init() {
	DefaultFactory.Register("parquet", NewParquetWriter)
	DefaultFactory.Register("arrow", NewArrowWriter)
	DefaultFactory.Register("json", NewJSONWriter)
}

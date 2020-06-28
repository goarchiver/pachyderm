package chunk

import (
	"math"

	"github.com/chmduquesne/rollinghash/buzhash64"
	"github.com/pachyderm/pachyderm/src/server/pkg/obj"
	"github.com/pachyderm/pachyderm/src/server/pkg/serviceenv"
	"github.com/pachyderm/pachyderm/src/server/pkg/storage/gc"
)

// StorageOption configures a storage.
type StorageOption func(s *Storage)

// WithGarbageCollection sets the garbage collection client
// for the storage. The storage will use a mock client otherwise.
func WithGarbageCollection(gcClient gc.Client) StorageOption {
	return func(s *Storage) {
		s.gcClient = gcClient
	}
}

// WithMaxConcurrentObjects sets the maximum number of object writers (upload)
// and readers (download) that can be open at a time.
func WithMaxConcurrentObjects(maxDownload, maxUpload int) StorageOption {
	return func(s *Storage) {
		s.objClient = obj.NewLimitedClient(s.objClient, maxDownload, maxUpload)
	}
}

// ServiceEnvToOptions converts a service environment configuration (specifically
// the storage configuration) to a set of storage options.
func ServiceEnvToOptions(env *serviceenv.ServiceEnv) (options []StorageOption) {
	if env.StorageUploadConcurrencyLimit > 0 {
		options = append(options, WithMaxConcurrentObjects(0, env.StorageUploadConcurrencyLimit))
	}
	return options
}

// WriterOption configures a chunk writer.
type WriterOption func(w *Writer)

// WithRollingHashConfig sets up the rolling hash with the passed in configuration.
func WithRollingHashConfig(averageBits int, seed int64) WriterOption {
	return func(w *Writer) {
		w.chunkSize.avg = int(math.Pow(2, float64(averageBits)))
		w.splitMask = (1 << uint64(averageBits)) - 1
		w.hash = buzhash64.NewFromUint64Array(buzhash64.GenerateHashes(seed))
	}
}

// WithMinMax sets the minimum and maximum chunk size.
func WithMinMax(min, max int) WriterOption {
	return func(w *Writer) {
		w.chunkSize.min = min
		w.chunkSize.max = max
	}
}

// WithNoUpload sets the writer to no upload (will not upload chunks).
func WithNoUpload() WriterOption {
	return func(w *Writer) {
		w.noUpload = true
	}
}

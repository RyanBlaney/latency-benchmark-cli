package latency

import (
	"fmt"
	"time"

	"github.com/RyanBlaney/latency-benchmark-common/logging"
	"github.com/RyanBlaney/latency-benchmark-common/stream/common"
	"github.com/RyanBlaney/sonido-sonar/fingerprint"
	"github.com/RyanBlaney/sonido-sonar/fingerprint/extractors"
)

// Config extends the base config with benchmark-specific settings
type Config struct {
	// Benchmark-specific settings
	Benchmark Settings `json:"benchmark" yaml:"benchmark"`
}

// Settings contains benchmark execution settings
type Settings struct {
	// Global timeouts
	OperationTimeout time.Duration `json:"operation_timeout" yaml:"operation_timeout"`

	// Audio analysis settings
	SampleDuration           time.Duration `json:"audio_segment_duration" yaml:"audio_segment_duration"`
	MinAlignmentConfidence   float64       `json:"min_alignment_confidence" yaml:"min_alignment_confidence"`
	MinFingerprintSimilarity float64       `json:"min_fingerprint_similarity" yaml:"min_fingerprint_similarity"`
	MaxAlignmentOffset       float64       `json:"max_alignment_offset" yaml:"max_alignment_offset"`

	// Analysis flags
	Verbose                   bool `json:"enable_detailed_analysis" yaml:"enable_detailed_analysis"`
	SkipFingerprintComparison bool `json:"skip_fingerprint_comparison" yaml:"skip_fingerprint_comparison"`
	ValidateHLS               bool `json:"validate_hls" yaml:"validate_hls"`
	ValidateICEcast           bool `json:"validate_icecast" yaml:"validate_icecast"`

	// Output settings
	OutputFormat            string         `json:"output_format" yaml:"output_format"`
	OutputToFile            bool           `json:"output_to_file" yaml:"output_to_file"`
	IncludeFeatureBreakdown bool           `json:"include_feature_breakdown" yaml:"include_feature_breakdown"`
	PrettyPrint             bool           `json:"pretty_print" yaml:"pretty_print"`
	Logger                  logging.Logger `json:"-"`
}

func DefaultSettings() *Settings {
	sampleDuration := time.Duration(200)

	settings := &Settings{
		SampleDuration:            sampleDuration * time.Second,
		OperationTimeout:          (sampleDuration * time.Second) - ((sampleDuration * time.Second) / 5),
		MinAlignmentConfidence:    0.55,
		MinFingerprintSimilarity:  0.75,
		Verbose:                   false,
		SkipFingerprintComparison: false,
		ValidateHLS:               true,
		ValidateICEcast:           true,
		OutputFormat:              "json",
		IncludeFeatureBreakdown:   false,
		PrettyPrint:               true,
	}

	return settings
}

// StreamType represents the type of stream being measured
type StreamType string

const (
	StreamTypeHLS     StreamType = "hls"
	StreamTypeICEcast StreamType = "icecast"
)

// StreamRole represents the role of the stream in the CDN architecture
type StreamRole string

const (
	StreamRoleSource StreamRole = "source"
	StreamRoleCDN    StreamRole = "cdn"
)

// StreamEndpoint represents a single stream endpoint
type StreamEndpoint struct {
	URL         string     `json:"url" yaml:"url"`
	Type        StreamType `json:"type,omitempty" yaml:"type,omitempty"`
	ContentType string     `json:"content_type,omitempty" yaml:"content_type,omitempty"`
	Enabled     *bool      `json:"enabled,omitempty" yaml:"enabled,omitempty"`
}

// StreamMeasurement represents the measurement results for a single stream
type StreamMeasurement struct {
	Endpoint            *StreamEndpoint               `json:"endpoint"`
	AudioData           *common.AudioData             `json:"-"` // Don't serialize raw audio
	Fingerprint         *fingerprint.AudioFingerprint `json:"-"` // Don't serialize fingerprint
	TimeToFirstByte     time.Duration                 `json:"time_to_first_byte_ms"`
	AudioExtractionTime time.Duration                 `json:"audio_extraction_time_seconds"`
	FingerprintTime     time.Duration                 `json:"fingerprint_time"`
	TotalProcessingTime time.Duration                 `json:"total_processing_time_seconds"`
	StreamValidation    *StreamValidation             `json:"stream_validation"`
	Error               error                         `json:"error,omitempty"`
	Timestamp           time.Time                     `json:"timestamp"`
}

// StreamValidation represents the validation results for a stream
type StreamValidation struct {
	IsValid           bool     `json:"is_valid"`
	ValidationErrors  []string `json:"validation_errors,omitempty"`
	PlaylistStructure bool     `json:"playlist_structure,omitempty"` // For HLS
	HTTPHeaders       bool     `json:"http_headers,omitempty"`       // For ICEcast
	AudioFormat       bool     `json:"audio_format"`
	BitrateConsistent bool     `json:"bitrate_consistent"`
}

// AlignmentMeasurement represents alignment comparison between two streams
type AlignmentMeasurement struct {
	Stream1          *StreamMeasurement            `json:"stream1"`
	Stream2          *StreamMeasurement            `json:"stream2"`
	AlignmentResult  *extractors.AlignmentFeatures `json:"alignment_result"`
	LatencySeconds   float64                       `json:"latency_seconds"`
	IsValidAlignment bool                          `json:"is_valid_alignment"`
	ComparisonTime   time.Duration                 `json:"comparison_time_ms"`
	Error            error                         `json:"error,omitempty"`
	Timestamp        time.Time                     `json:"timestamp"`
}

// FingerprintComparison represents fingerprint similarity comparison
type FingerprintComparison struct {
	Stream1           *StreamMeasurement            `json:"stream1"`
	Stream2           *StreamMeasurement            `json:"stream2"`
	SimilarityResult  *fingerprint.SimilarityResult `json:"similarity_result"`
	AlignmentFeatures *extractors.AlignmentFeatures `json:"alignment_features,omitempty"`
	IsValidMatch      bool                          `json:"is_valid_match"`
	ComparisonTime    time.Duration                 `json:"comparison_time_ms"`
	Error             error                         `json:"error,omitempty"`
	Timestamp         time.Time                     `json:"timestamp"`
}

// Liveness wraps the lag (in seconds) and the status of a stream.
// The status is "MATCH" for both aligned and fingerprint valid,
// "NO_ALIGNMENT" for alignment invalid, and "NO_MATCH" a fingerprint
// failure implies that alignment failed.
type Liveness struct {
	LagSeconds float64 `json:"lag_seconds"`
	Status     string  `json:"status"`
}

// OverallValidation represents the overall health of all streams
type OverallValidation struct {
	StreamsValid            bool `json:"all_streams_valid"`
	FingerprintMatchesValid bool `json:"fingerprint_matches_valid"`
}

// BenchmarkSummary represents a summary of multiple broadcast measurements
type BenchmarkSummary struct {
	StartTime     time.Time     `json:"start_time"`
	EndTime       time.Time     `json:"end_time"`
	TotalDuration time.Duration `json:"total_duration"`
}

// Validate validates the benchmark configuration
func (s *Settings) Validate() error {
	// Validate global timeouts
	if s.OperationTimeout <= 0 {
		return fmt.Errorf("operation timeout must be positive")
	}

	// Validate audio analysis settings
	if s.SampleDuration <= 0 {
		return fmt.Errorf("audio segment duration must be positive")
	}

	if s.MinAlignmentConfidence < 0 || s.MinAlignmentConfidence > 1 {
		return fmt.Errorf("min alignment confidence must be between 0 and 1")
	}

	if s.MinFingerprintSimilarity < 0 || s.MinFingerprintSimilarity > 1 {
		return fmt.Errorf("min fingerprint similarity must be between 0 and 1")
	}

	if s.MaxAlignmentOffset < 0 {
		return fmt.Errorf("max alignment offset must be non-negative")
	}

	// Validate output settings
	validFormats := map[string]bool{
		"json": true,
		"xml":  true,
		"csv":  true,
		"txt":  true,
	}

	if s.OutputFormat != "" {
		if !validFormats[s.OutputFormat] {
			return fmt.Errorf("output format must be one of: json, xml, csv, txt")
		}
	}

	return nil
}

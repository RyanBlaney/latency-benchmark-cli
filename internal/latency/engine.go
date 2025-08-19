package latency

import (
	"context"
	"encoding/binary"
	"fmt"
	"maps"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/RyanBlaney/latency-benchmark-common/logging"
	"github.com/RyanBlaney/latency-benchmark-common/stream"
	"github.com/RyanBlaney/latency-benchmark-common/stream/common"
	"github.com/RyanBlaney/sonido-sonar/fingerprint"
	"github.com/RyanBlaney/sonido-sonar/fingerprint/config"
	"github.com/RyanBlaney/sonido-sonar/fingerprint/extractors"
	"github.com/RyanBlaney/sonido-sonar/transcode"
)

// MeasurementEngine handles latency measurements for streams
type MeasurementEngine struct {
	logger             logging.Logger
	operationTimeout   time.Duration
	sampleDuration     time.Duration
	minAlignmentConf   float64
	maxAlignmentOffset float64
	minSimilarity      float64
	verbose            bool
	outputToFile       bool
}

// NewMeasurementEngine creates a new measurement engine
func NewMeasurementEngine(config *Settings) *MeasurementEngine {
	logger := logging.NewDefaultLogger()

	return &MeasurementEngine{
		logger:             logger,
		operationTimeout:   config.OperationTimeout,
		sampleDuration:     config.SampleDuration,
		minAlignmentConf:   config.MinAlignmentConfidence,
		maxAlignmentOffset: config.MaxAlignmentOffset,
		minSimilarity:      config.MinFingerprintSimilarity,
		verbose:            config.Verbose,
		outputToFile:       config.OutputToFile,
	}
}

// DownloadStream downloads a single stream endpoint
func (e *MeasurementEngine) DownloadStream(ctx context.Context, streamURL, streamType, contentType string) (*StreamEndpoint, *common.AudioData, error) {
	stream := &StreamEndpoint{
		URL:         streamURL,
		Type:        StreamType(streamType),
		ContentType: contentType,
	}

	e.logger.Info("Starting stream download", map[string]any{
		"url":          stream.URL,
		"type":         stream.Type,
		"content_type": stream.ContentType,
	})

	audioData, _, err := e.loadAudio(ctx, stream)
	if err != nil {
		e.logger.Error(err, "failed to load audio")
		return nil, nil, err
	}

	if e.outputToFile {
		outputToFile(stream.URL, audioData)
	}

	return stream, audioData, nil
}

func (e *MeasurementEngine) DetectContentType(ctx context.Context, audioData *common.AudioData) config.ContentType {
	cd := fingerprint.NewContentDetector(&config.ContentAwareConfig{
		EnableContentDetection: true,
		DefaultContentType:     "news",
		AutoDetectThreshold:    2.0,
	})

	ad := convertToTranscodeAudioData(audioData)

	return cd.DetectContentType(ad)
}

func (e *MeasurementEngine) ExtractFeatures(ctx context.Context, audioData *common.AudioData, endpoint *StreamEndpoint) (*fingerprint.AudioFingerprint, time.Duration, error) {
	fingerprintStart := time.Now()
	fingerprint, err := e.generateFingerprint(audioData, endpoint)
	if err != nil {
		e.logger.Error(err, "failed to generate fingerprint")
		return nil, time.Since(fingerprintStart), err
	}
	fingerprintTime := time.Since(fingerprintStart)

	return fingerprint, fingerprintTime, nil
}

func convertToTranscodeAudioData(src *common.AudioData) *transcode.AudioData {
	if src == nil {
		return nil
	}

	dst := &transcode.AudioData{
		SampleRate: src.SampleRate,
		Channels:   src.Channels,
		Duration:   src.Duration,
		Timestamp:  src.Timestamp,
	}

	// Efficient PCM deep copy
	if src.PCM != nil {
		dst.PCM = make([]float64, len(src.PCM))
		copy(dst.PCM, src.PCM)
	}

	// Deep copy metadata
	if src.Metadata != nil {
		dst.Metadata = copyStreamMetadata(src.Metadata)
	}

	return dst
}

func copyStreamMetadata(src *common.StreamMetadata) *transcode.StreamMetadata {
	if src == nil {
		return nil
	}

	dst := &transcode.StreamMetadata{
		URL:         src.URL,
		Type:        string(src.Type),
		Format:      src.Format,
		Bitrate:     src.Bitrate,
		SampleRate:  src.SampleRate,
		Channels:    src.Channels,
		Codec:       src.Codec,
		ContentType: src.ContentType,
		Title:       src.Title,
		Artist:      src.Artist,
		Genre:       src.Genre,
		Station:     src.Station,
		Timestamp:   src.Timestamp,
	}

	// Deep copy the Headers map
	if src.Headers != nil {
		dst.Headers = make(map[string]string, len(src.Headers))
		maps.Copy(dst.Headers, src.Headers)
	}

	return dst
}

// MeasureStream measures a single stream endpoint
func (e *MeasurementEngine) MeasureStream(ctx context.Context, endpoint *StreamEndpoint) *StreamMeasurement {
	measurement := &StreamMeasurement{
		Endpoint:  endpoint,
		Timestamp: time.Now(),
	}

	e.logger.Info("Starting stream measurement", map[string]any{
		"url":          endpoint.URL,
		"type":         endpoint.Type,
		"content_type": endpoint.ContentType,
	})

	totalStart := time.Now()

	// Step 1: Load audio and measure TTFB
	audioData, ttfb, err := e.loadAudio(ctx, endpoint)
	if err != nil {
		measurement.Error = fmt.Errorf("failed to load audio: %w", err)
		return measurement
	}
	measurement.TimeToFirstByte = ttfb
	measurement.AudioData = audioData

	// Output to file (for testing)
	outputToFile(endpoint.URL, audioData)

	// Step 2: Validate stream
	measurement.StreamValidation = e.ValidateStream(endpoint, audioData)

	// Step 3: Generate fingerprint
	fingerprintStart := time.Now()
	fingerprint, err := e.generateFingerprint(audioData, endpoint)
	if err != nil {
		measurement.Error = fmt.Errorf("failed to generate fingerprint: %w", err)
		return measurement
	}
	measurement.FingerprintTime = time.Since(fingerprintStart)
	measurement.Fingerprint = fingerprint

	measurement.TotalProcessingTime = time.Since(totalStart)

	e.logger.Debug("Stream measurement completed", map[string]any{
		"url":                 endpoint.URL,
		"ttfb_ms":             measurement.TimeToFirstByte.Milliseconds(),
		"fingerprint_time_ms": measurement.FingerprintTime.Milliseconds(),
		"total_processing_ms": measurement.TotalProcessingTime.Milliseconds(),
		"audio_duration_s":    audioData.Duration.Seconds(),
		"is_valid":            measurement.StreamValidation.IsValid,
	})

	return measurement
}

// outputToFile is a santity check to ensure the downloads are actually starting and downloading properly
func outputToFile(urlStr string, audioData *common.AudioData) error {
	// Create live-test directory if it doesn't exist
	if err := os.MkdirAll("./live-test", 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}
	// Clean URL to make it filesystem-safe
	parsedURL, err := url.Parse(urlStr)
	if err != nil {
		return fmt.Errorf("failed to parse URL: %w", err)
	}
	// Create filename from URL (remove protocol, replace invalid chars)
	filename := strings.ReplaceAll(parsedURL.Host+parsedURL.Path, "/", "_")
	filename = strings.ReplaceAll(filename, ":", "_")
	filename = strings.ReplaceAll(filename, "?", "_")
	filename = strings.ReplaceAll(filename, "&", "_")
	filename = strings.ReplaceAll(filename, "=", "_")
	if filename == "" {
		filename = "audio"
	}
	// Create temporary raw PCM file
	tempPCMFile := filepath.Join("./live-test", filename+"_temp.pcm")
	mp3File := filepath.Join("./live-test", filename+".mp3")
	// Convert float64 to 16-bit PCM and write to temp file
	file, err := os.Create(tempPCMFile)
	if err != nil {
		return fmt.Errorf("failed to create temp PCM file: %w", err)
	}
	defer file.Close()
	defer os.Remove(tempPCMFile) // Clean up temp file
	for _, sample := range audioData.PCM {
		// Clamp to [-1.0, 1.0] and convert to 16-bit signed integer
		if sample > 1.0 {
			sample = 1.0
		} else if sample < -1.0 {
			sample = -1.0
		}
		pcm16 := int16(sample * 32767)
		if err := binary.Write(file, binary.LittleEndian, pcm16); err != nil {
			return fmt.Errorf("failed to write PCM data: %w", err)
		}
	}
	file.Close()
	// Use ffmpeg to convert raw PCM to MP3
	cmd := exec.Command("ffmpeg",
		"-f", "s16le", // 16-bit signed little-endian
		"-ar", fmt.Sprintf("%d", audioData.SampleRate),
		"-ac", fmt.Sprintf("%d", audioData.Channels),
		"-i", tempPCMFile, // input file
		"-codec:a", "libmp3lame", // MP3 encoder
		"-b:a", "128k", // bitrate (adjust as needed: 64k, 96k, 128k, 192k, 256k, 320k)
		"-y",    // overwrite output file
		mp3File, // output file
	)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("ffmpeg failed: %w\nOutput: %s", err, string(output))
	}
	fmt.Printf("Successfully created MP3 file: %s\n", mp3File)
	return nil
}

// MeasureAlignment measures temporal alignment between two streams
func (e *MeasurementEngine) MeasureAlignment(ctx context.Context, stream1, stream2 *StreamMeasurement) *AlignmentMeasurement {
	measurement := &AlignmentMeasurement{
		Stream1:   stream1,
		Stream2:   stream2,
		Timestamp: time.Now(),
	}

	if stream1.Error != nil || stream2.Error != nil {
		measurement.Error = fmt.Errorf("cannot measure alignment: one or both streams have errors")
		return measurement
	}

	e.logger.Debug("Starting alignment measurement", map[string]any{
		"stream1_url": stream1.Endpoint.URL,
		"stream2_url": stream2.Endpoint.URL,
	})

	compStart := time.Now()

	// Extract alignment features
	contentType := e.parseContentType(stream1.Endpoint.ContentType)
	featureConfig := e.createFeatureConfig(stream1.AudioData.SampleRate)
	alignmentConfig := config.AlignmentConfigForContent(contentType)

	alignmentExtractor := extractors.NewAlignmentExtractorWithMaxLag(
		featureConfig, alignmentConfig, e.maxAlignmentOffset)

	alignmentFeatures, err := alignmentExtractor.ExtractAlignmentFeatures(
		stream1.Fingerprint.Features, stream2.Fingerprint.Features,
		stream1.AudioData.PCM, stream2.AudioData.PCM,
		stream1.AudioData.SampleRate)

	measurement.ComparisonTime = time.Since(compStart)

	if err != nil {
		measurement.Error = fmt.Errorf("alignment extraction failed: %w", err)
		return measurement
	}

	measurement.AlignmentResult = alignmentFeatures
	measurement.IsValidAlignment = alignmentFeatures.OffsetConfidence >= e.minAlignmentConf

	if measurement.IsValidAlignment {
		measurement.LatencySeconds = alignmentFeatures.TemporalOffset
	}

	measurement = e.TruncateToAlignment(ctx, measurement, alignmentExtractor)

	e.logger.Debug("Alignment measurement completed", map[string]any{
		"stream1_url":        stream1.Endpoint.URL,
		"stream2_url":        stream2.Endpoint.URL,
		"offset_seconds":     alignmentFeatures.TemporalOffset,
		"confidence":         alignmentFeatures.OffsetConfidence,
		"is_valid":           measurement.IsValidAlignment,
		"comparison_time_ms": measurement.ComparisonTime.Milliseconds(),
	})

	return measurement
}

// TruncateToAlignment truncates the audio data in both streams to the aligned portions
func (e *MeasurementEngine) TruncateToAlignment(ctx context.Context, alignment *AlignmentMeasurement, extractor *extractors.AlignmentExtractor) *AlignmentMeasurement {
	if alignment.Error != nil {
		return alignment
	}

	if !alignment.IsValidAlignment {
		alignment.Error = fmt.Errorf("cannot truncate: alignment is not valid (confidence %.3f < %.3f)",
			alignment.AlignmentResult.OffsetConfidence, e.minAlignmentConf)
		return alignment
	}

	e.logger.Debug("Truncating streams to aligned segments", map[string]any{
		"stream1_url":    alignment.Stream1.Endpoint.URL,
		"stream2_url":    alignment.Stream2.Endpoint.URL,
		"offset_seconds": alignment.AlignmentResult.TemporalOffset,
		"original_len1":  len(alignment.Stream1.AudioData.PCM),
		"original_len2":  len(alignment.Stream2.AudioData.PCM),
	})

	// Truncate PCM data using the alignment
	alignedPCM1, alignedPCM2, err := extractor.TruncateToAlignmentPCM(
		alignment.Stream1.AudioData.PCM,
		alignment.Stream2.AudioData.PCM,
		alignment.Stream1.AudioData.SampleRate,
		alignment.AlignmentResult,
	)
	if err != nil {
		alignment.Error = fmt.Errorf("truncation failed: %w", err)
		return alignment
	}

	// Update AudioData with truncated PCM
	sampleRate := float64(alignment.Stream1.AudioData.SampleRate)

	// Clone Streams
	stream1Copy := &StreamMeasurement{
		Endpoint:            alignment.Stream1.Endpoint,
		Fingerprint:         alignment.Stream1.Fingerprint,
		StreamValidation:    alignment.Stream1.StreamValidation,
		TimeToFirstByte:     alignment.Stream1.TimeToFirstByte,
		AudioExtractionTime: alignment.Stream1.AudioExtractionTime,
		FingerprintTime:     alignment.Stream1.FingerprintTime,
		Error:               alignment.Stream1.Error,
		Timestamp:           alignment.Stream1.Timestamp,
		AudioData: &common.AudioData{
			PCM:        alignedPCM1,
			SampleRate: alignment.Stream1.AudioData.SampleRate,
			Channels:   alignment.Stream1.AudioData.Channels,
			Duration:   time.Duration(float64(len(alignedPCM1)) / sampleRate * float64(time.Second)),
		},
	}

	stream2Copy := &StreamMeasurement{
		Endpoint:            alignment.Stream2.Endpoint,
		Fingerprint:         alignment.Stream2.Fingerprint,
		StreamValidation:    alignment.Stream2.StreamValidation,
		TimeToFirstByte:     alignment.Stream2.TimeToFirstByte,
		AudioExtractionTime: alignment.Stream2.AudioExtractionTime,
		FingerprintTime:     alignment.Stream2.FingerprintTime,
		Error:               alignment.Stream2.Error,
		Timestamp:           alignment.Stream2.Timestamp,
		AudioData: &common.AudioData{
			PCM:        alignedPCM2,
			SampleRate: alignment.Stream2.AudioData.SampleRate,
			Channels:   alignment.Stream2.AudioData.Channels,
			Duration:   time.Duration(float64(len(alignedPCM2)) / sampleRate * float64(time.Second)),
		},
	}

	alignment.Stream1 = stream1Copy
	alignment.Stream2 = stream2Copy

	e.logger.Debug("Stream truncation completed", map[string]any{
		"stream1_url":      alignment.Stream1.Endpoint.URL,
		"stream2_url":      alignment.Stream2.Endpoint.URL,
		"aligned_len1":     len(alignedPCM1),
		"aligned_len2":     len(alignedPCM2),
		"aligned_duration": alignment.Stream1.AudioData.Duration.Seconds(),
		"samples_saved1":   len(alignment.Stream1.AudioData.PCM) - len(alignedPCM1),
		"samples_saved2":   len(alignment.Stream2.AudioData.PCM) - len(alignedPCM2),
	})

	return alignment
}

// CompareFingerprintSimilarity compares fingerprint similarity between two streams
func (e *MeasurementEngine) CompareFingerprintSimilarity(ctx context.Context, stream1, stream2 *StreamMeasurement, alignmentFeatures *extractors.AlignmentFeatures) *FingerprintComparison {
	comparison := &FingerprintComparison{
		Stream1:           stream1,
		Stream2:           stream2,
		AlignmentFeatures: alignmentFeatures,
		Timestamp:         time.Now(),
	}

	if stream1.Error != nil || stream2.Error != nil {
		comparison.Error = fmt.Errorf("cannot compare fingerprints: one or both streams have errors")
		return comparison
	}

	e.logger.Debug("Starting fingerprint comparison", map[string]any{
		"stream1_url":   stream1.Endpoint.URL,
		"stream2_url":   stream2.Endpoint.URL,
		"has_alignment": alignmentFeatures != nil,
	})

	compStart := time.Now()

	// Create comparator
	contentType := e.parseContentType(stream1.Endpoint.ContentType)
	comparisonConfig := fingerprint.ContentOptimizedComparisonConfig(contentType)
	comparator := fingerprint.NewFingerprintComparator(comparisonConfig)

	var result *fingerprint.SimilarityResult
	var err error

	// Compare with or without alignment
	if alignmentFeatures != nil && alignmentFeatures.OffsetConfidence >= e.minAlignmentConf {
		result, err = comparator.Compare(
			stream1.Fingerprint, stream2.Fingerprint)
	} else {
		result, err = comparator.Compare(stream1.Fingerprint, stream2.Fingerprint)
	}

	comparison.ComparisonTime = time.Since(compStart)

	if err != nil {
		comparison.Error = fmt.Errorf("fingerprint comparison failed: %w", err)
		return comparison
	}

	comparison.SimilarityResult = result
	comparison.IsValidMatch = result.OverallSimilarity >= e.minSimilarity

	e.logger.Debug("Fingerprint comparison completed", map[string]any{
		"stream1_url":        stream1.Endpoint.URL,
		"stream2_url":        stream2.Endpoint.URL,
		"overall_similarity": result.OverallSimilarity,
		"confidence":         result.Confidence,
		"is_valid_match":     comparison.IsValidMatch,
		"comparison_time_ms": comparison.ComparisonTime.Milliseconds(),
	})

	return comparison
}

// loadAudio loads audio from a stream endpoint. Also returns TTFB (time to first byte)
func (e *MeasurementEngine) loadAudio(ctx context.Context, endpoint *StreamEndpoint) (*common.AudioData, time.Duration, error) {
	if e.isLocalFile(endpoint.URL) {
		return e.loadLocalFile(endpoint.URL, endpoint.ContentType)
	}
	return e.loadStreamURL(ctx, endpoint.URL)
}

// isLocalFile checks if the input is a local file
func (e *MeasurementEngine) isLocalFile(input string) bool {
	return strings.HasPrefix(input, "file://") ||
		strings.HasPrefix(input, "/") ||
		strings.HasPrefix(input, "./") ||
		strings.HasPrefix(input, "../") ||
		(!strings.HasPrefix(input, "http://") && !strings.HasPrefix(input, "https://"))
}

// loadLocalFile loads audio from a local file
func (e *MeasurementEngine) loadLocalFile(filePath, contentType string) (*common.AudioData, time.Duration, error) {
	// Implementation similar to your working fingerprint-test.go
	cleanPath := strings.TrimPrefix(filePath, "file://")

	decoder := transcode.NewNormalizingDecoder(contentType)
	anyData, err := decoder.DecodeFile(cleanPath)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to decode audio file: %w", err)
	}

	audioData := common.ConvertToAudioData(anyData)
	if audioData == nil {
		return nil, 0, fmt.Errorf("decoder returned unexpected type: %T", anyData)
	}

	// Truncate if needed
	if e.sampleDuration > 0 {
		maxSamples := int(float64(e.sampleDuration) * float64(audioData.SampleRate))
		if len(audioData.PCM) > maxSamples {
			audioData.PCM = audioData.PCM[:maxSamples]
			audioData.Duration = time.Duration(float64(len(audioData.PCM))/float64(audioData.SampleRate)) * time.Second
		}
	}

	return audioData, 0, nil
}

// loadStreamURL loads audio from a stream URL
func (e *MeasurementEngine) loadStreamURL(ctx context.Context, url string) (*common.AudioData, time.Duration, error) {
	managerConfig := &stream.ManagerConfig{
		StreamTimeout:        e.operationTimeout,
		OverallTimeout:       e.operationTimeout + ((e.operationTimeout / 10) * time.Second),
		MaxConcurrentStreams: 1,
		ResultBufferSize:     1,
	}
	manager := stream.NewManagerWithConfig(managerConfig)

	results, err := manager.ExtractAudioSequential(ctx, []string{url}, e.sampleDuration)
	if err != nil {
		return nil, 0, err
	}

	if len(results.Results) == 0 || results.Results[0].Error != nil {
		if len(results.Results) > 0 {
			return nil, 0, results.Results[0].Error
		}
		return nil, 0, fmt.Errorf("no results from stream extraction")
	}

	return results.Results[0].AudioData, results.Results[0].TimeToFirstByte, nil
}

// generateFingerprint generates a fingerprint for audio data
func (e *MeasurementEngine) generateFingerprint(audioData *common.AudioData, endpoint *StreamEndpoint) (*fingerprint.AudioFingerprint, error) {
	contentType := e.parseContentType(endpoint.ContentType)
	fingerprintConfig := fingerprint.ContentOptimizedFingerprintConfig(contentType)

	// Apply the working parameters
	fingerprintConfig.WindowSize = 1024
	fingerprintConfig.HopSize = 256
	fingerprintConfig.FeatureConfig.WindowSize = 1024
	fingerprintConfig.FeatureConfig.HopSize = 256
	fingerprintConfig.FeatureConfig.SampleRate = audioData.SampleRate

	// Content-specific optimizations
	if endpoint.ContentType == "news" || endpoint.ContentType == "talk" {
		fingerprintConfig.FeatureConfig.EnableSpeechFeatures = true
		fingerprintConfig.FeatureConfig.EnableHarmonicFeatures = false
		fingerprintConfig.FeatureConfig.EnableChroma = false
		fingerprintConfig.FeatureConfig.MFCCCoefficients = 12
		fingerprintConfig.FeatureConfig.FreqRange = [2]float64{80.0, 8000.0}
	}

	fingerprintGenerator := fingerprint.NewFingerprintGenerator(fingerprintConfig)

	// Convert to transcode.AudioData
	transcodeAudioData := &transcode.AudioData{
		PCM:        audioData.PCM,
		SampleRate: audioData.SampleRate,
		Channels:   audioData.Channels,
		Metadata: &transcode.StreamMetadata{
			URL:         endpoint.URL,
			Type:        string(endpoint.Type),
			ContentType: endpoint.ContentType,
			SampleRate:  audioData.SampleRate,
			Channels:    audioData.Channels,
			Timestamp:   time.Now(),
		},
	}

	return fingerprintGenerator.GenerateFingerprint(transcodeAudioData)
}

// ValidateStream validates a stream's health and structure
func (e *MeasurementEngine) ValidateStream(endpoint *StreamEndpoint, audioData *common.AudioData) *StreamValidation {
	validation := &StreamValidation{
		IsValid:           true,
		ValidationErrors:  []string{},
		AudioFormat:       true,
		BitrateConsistent: true,
	}

	// Basic audio format validation
	if audioData.SampleRate <= 0 {
		validation.IsValid = false
		validation.AudioFormat = false
		validation.ValidationErrors = append(validation.ValidationErrors, "invalid sample rate")
	}

	if audioData.Channels <= 0 {
		validation.IsValid = false
		validation.AudioFormat = false
		validation.ValidationErrors = append(validation.ValidationErrors, "invalid channel count")
	}

	if len(audioData.PCM) == 0 {
		validation.IsValid = false
		validation.AudioFormat = false
		validation.ValidationErrors = append(validation.ValidationErrors, "no audio data")
	}

	// Duration validation
	expectedDuration := e.sampleDuration
	actualDuration := audioData.Duration
	durationDiff := actualDuration - expectedDuration
	if durationDiff < -time.Second || durationDiff > time.Second*5 {
		validation.ValidationErrors = append(validation.ValidationErrors,
			fmt.Sprintf("unexpected audio duration: got %.1fs, expected %.1fs",
				actualDuration.Seconds(), expectedDuration.Seconds()))
	}

	// TODO: Add HLS playlist structure validation
	if endpoint.Type == StreamTypeHLS {
		validation.PlaylistStructure = true // Placeholder - implement HLS validation
	}

	// TODO: Add ICEcast metadata validation
	if endpoint.Type == StreamTypeICEcast {
		validation.HTTPHeaders = true // Placeholder - implement ICEcast validation
	}

	return validation
}

// parseContentType converts string to config.ContentType
func (e *MeasurementEngine) parseContentType(contentTypeStr string) config.ContentType {
	switch strings.ToLower(contentTypeStr) {
	case "music":
		return config.ContentMusic
	case "news":
		return config.ContentNews
	case "talk":
		return config.ContentTalk
	case "sports":
		return config.ContentSports
	case "mixed":
		return config.ContentMixed
	default:
		return config.ContentNews
	}
}

// createFeatureConfig creates feature configuration for alignment
func (e *MeasurementEngine) createFeatureConfig(sampleRate int) *config.FeatureConfig {
	return &config.FeatureConfig{
		WindowSize: 1024,
		HopSize:    256,
		SampleRate: sampleRate,
		FreqRange:  [2]float64{80.0, 8000.0},

		EnableChroma:           false,
		EnableMFCC:             true,
		EnableSpectralContrast: false,
		EnableTemporalFeatures: true,
		EnableSpeechFeatures:   true,
		EnableHarmonicFeatures: false,

		MFCCCoefficients: 12,
		ChromaBins:       12,

		SimilarityWeights: map[string]float64{
			"mfcc":     0.50,
			"spectral": 0.25,
			"temporal": 0.15,
			"speech":   0.10,
		},
		MatchThreshold: 0.70,
	}
}

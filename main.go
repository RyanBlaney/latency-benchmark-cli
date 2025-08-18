package main

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/RyanBlaney/latency-benchmark-cli/internal/latency"
	"github.com/RyanBlaney/latency-benchmark-common/logging"
	"github.com/RyanBlaney/latency-benchmark-common/stream"
	"github.com/RyanBlaney/latency-benchmark-common/stream/common"
	"github.com/RyanBlaney/sonido-sonar/fingerprint"
	"github.com/charmbracelet/bubbles/progress"
	"github.com/charmbracelet/bubbles/spinner"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/spf13/cobra"
)

// Styles for the TUI
var (
	titleStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("#7C3AED")).
			MarginBottom(1)

	subtitleStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#64748B")).
			MarginBottom(1)

	progressStyle = lipgloss.NewStyle().
			MarginTop(1).
			MarginBottom(1)

	resultStyle = lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(lipgloss.Color("#10B981")).
			Padding(1).
			MarginTop(1)

	errorStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#EF4444")).
			Bold(true)

	successStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#10B981")).
			Bold(true)

	warningStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#F59E0B")).
			Bold(true)

	metricLabelStyle = lipgloss.NewStyle().
				Foreground(lipgloss.Color("#8B5CF6")).
				Bold(true)

	metricValueStyle = lipgloss.NewStyle().
				Foreground(lipgloss.Color("#F1F5F9"))
)

// Stage represents the processing stages in the app
type Stage int

const (
	StageInitial Stage = iota
	StageDownloadingStream1
	StageDownloadingStream2
	StageDeterminingContentType1
	StageDeterminingContentType2
	StageFeatureExtraction1
	StageFeatureExtraction2
	StageTemporalAlignment
	StageTruncation
	StageFingerprintComparison
	StageComplete
	StageError
)

// Messages for the Tea program
type progressMsg struct {
	stage    Stage
	progress float64
	message  string
}

type completeMsg struct {
	result *ComparisonResult
}

type errorMsg struct {
	err error
}

// ComparisonResult represents the  result structure
type ComparisonResult struct {
	Stream1URL          string
	Stream2URL          string
	AlignmentSeconds    float64
	OffsetConfidence    float64
	OverallSimilarity   float64
	QualityScore        float64
	Stream1TTFB         time.Duration
	Stream2TTFB         time.Duration
	TotalProcessingTime time.Duration
	Stream1Duration     time.Duration
	Stream2Duration     time.Duration
	IsValidAlignment    bool
	IsValidMatch        bool
}

// Model for the Tea program
type model struct {
	stream1URL     string
	stream2URL     string
	sampleDuration time.Duration
	currentStage   Stage
	progress       map[Stage]progress.Model
	spinner        spinner.Model
	result         *ComparisonResult
	err            error
	engine         *latency.MeasurementEngine
	ctx            context.Context
	cancel         context.CancelFunc
	progressChan   chan progressMsg
	errorChan      chan errorMsg
	completeChan   chan completeMsg
}

func initialModel(stream1URL, stream2URL string, duration time.Duration) model {
	ctx, cancel := context.WithCancel(context.Background())

	// Initialize progress bars for each stage
	progressBars := make(map[Stage]progress.Model)
	stages := []Stage{
		StageDownloadingStream1,
		StageDownloadingStream2,
		StageDeterminingContentType1,
		StageDeterminingContentType2,
		StageFeatureExtraction1,
		StageFeatureExtraction2,
		StageTemporalAlignment,
		StageTruncation,
		StageFingerprintComparison,
	}

	for _, stage := range stages {
		p := progress.New(progress.WithDefaultGradient())
		p.Width = 40
		progressBars[stage] = p
	}

	// Initialize spinner
	s := spinner.New()
	s.Spinner = spinner.Dot
	s.Style = lipgloss.NewStyle().Foreground(lipgloss.Color("#7C3AED"))

	// Create measurement engine
	engineConfig := &latency.Settings{
		OperationTimeout:         (duration + (duration / 2)) * time.Second,
		SampleDuration:           duration,
		MinAlignmentConfidence:   0.33,
		MaxAlignmentOffset:       (duration - (duration / 4)).Seconds(),
		MinFingerprintSimilarity: 0.7,
		Verbose:                  false,
		Logger:                   logging.NewDefaultLogger(),
	}
	engine := latency.NewMeasurementEngine(engineConfig)

	return model{
		stream1URL:     stream1URL,
		stream2URL:     stream2URL,
		sampleDuration: duration,
		currentStage:   StageInitial,
		progress:       progressBars,
		spinner:        s,
		engine:         engine,
		ctx:            ctx,
		cancel:         cancel,
		progressChan:   make(chan progressMsg, 10),
		errorChan:      make(chan errorMsg, 1),
		completeChan:   make(chan completeMsg, 1),
	}
}

func (m model) Init() tea.Cmd {
	return tea.Batch(
		m.spinner.Tick,
		m.startComparison(),
		m.listenForUpdates(),
	)
}

func (m model) startComparison() tea.Cmd {
	return func() tea.Msg {
		// Start the comparison process in a goroutine
		go m.runComparison()
		return progressMsg{
			stage:    StageDownloadingStream1,
			progress: 0.0,
			message:  "Starting comparison...",
		}
	}
}

func (m model) listenForUpdates() tea.Cmd {
	return func() tea.Msg {
		select {
		case msg := <-m.progressChan:
			return msg
		case msg := <-m.errorChan:
			return msg
		case msg := <-m.completeChan:
			return msg
		case <-time.After(50 * time.Millisecond):
			return progressMsg{stage: Stage(0), progress: -1, message: ""}
		}
	}
}

func (m model) runComparison() {
	detector := stream.NewDetector()

	// Stage 1 & 2: Download both streams simultaneously
	m.sendProgress(StageDownloadingStream1, 0.0, "Starting download of stream 1...")
	m.sendProgress(StageDownloadingStream2, 0.0, "Starting download of stream 2...")

	startMeasurements := time.Now()

	// Use channels to coordinate the downloads
	type downloadResult struct {
		endpoint  *latency.StreamEndpoint
		audioData *common.AudioData
		streamNum int
		err       error
	}

	downloadChan := make(chan downloadResult, 2)

	// Download stream 1
	go func() {
		go func() {
			time.Sleep(300 * time.Millisecond)

			m.sendProgress(StageDownloadingStream1, 0.01, "Detecting stream 1 type...")

			time.Sleep(500 * time.Millisecond)

			m.sendProgress(StageDownloadingStream1, 0.03, "Detecting stream 1 type...")

			time.Sleep(1000 * time.Millisecond)

			m.sendProgress(StageDownloadingStream1, 0.05, "Downloading stream 1...")

			time.Sleep(200 * time.Millisecond)

			numTicks := 20
			tickDuration := (m.sampleDuration - (m.sampleDuration / 8)) / time.Duration(numTicks)

			currVal := 0.1
			for range numTicks {
				time.Sleep(tickDuration)

				diff := 1.0 - currVal
				div := diff / float64(numTicks)
				currVal += div

				m.sendProgress(StageDownloadingStream1, currVal, "Downloading stream 1...")
			}
		}()

		streamType1, err := detector.DetectType(m.ctx, m.stream1URL)
		if err != nil {
			downloadChan <- downloadResult{streamNum: 1, err: err}
			return
		}

		endpoint1, audioData1, err := m.engine.DownloadStream(m.ctx, m.stream1URL, string(streamType1), "")
		if err != nil {
			downloadChan <- downloadResult{streamNum: 1, err: err}
			return
		}

		m.sendProgress(StageDownloadingStream1, 1.0, "Stream 1 downloaded successfully")
		downloadChan <- downloadResult{
			endpoint:  endpoint1,
			audioData: audioData1,
			streamNum: 1,
		}
	}()

	// Download stream 2
	go func() {
		go func() {
			time.Sleep(300 * time.Millisecond)

			m.sendProgress(StageDownloadingStream2, 0.01, "Detecting stream 2 type...")

			time.Sleep(500 * time.Millisecond)

			m.sendProgress(StageDownloadingStream2, 0.03, "Detecting stream 2 type...")

			time.Sleep(1000 * time.Millisecond)

			m.sendProgress(StageDownloadingStream2, 0.05, "Downloading stream 2...")

			time.Sleep(200 * time.Millisecond)

			numTicks := 20
			tickDuration := (m.sampleDuration - (m.sampleDuration / 8)) / time.Duration(numTicks)

			currVal := 0.05
			for range numTicks {
				time.Sleep(tickDuration)

				diff := 1.0 - currVal
				div := diff / float64(numTicks)
				currVal += div

				m.sendProgress(StageDownloadingStream2, currVal, "Downloading stream 2...")
			}
		}()

		streamType2, err := detector.DetectType(m.ctx, m.stream2URL)
		if err != nil {
			downloadChan <- downloadResult{streamNum: 2, err: err}
			return
		}

		endpoint2, audioData2, err := m.engine.DownloadStream(m.ctx, m.stream2URL, string(streamType2), "")
		if err != nil {
			downloadChan <- downloadResult{streamNum: 2, err: err}
			return
		}

		m.sendProgress(StageDownloadingStream2, 1.0, "Stream 2 downloaded successfully")
		downloadChan <- downloadResult{
			endpoint:  endpoint2,
			audioData: audioData2,
			streamNum: 2,
		}
	}()

	// Wait for both downloads to complete
	var endpoint1, endpoint2 *latency.StreamEndpoint
	var audioData1, audioData2 *common.AudioData
	var stream1TTFB, stream2TTFB time.Duration

	for range 2 {
		select {
		case result := <-downloadChan:
			if result.err != nil {
				return
			}
			if result.streamNum == 1 {
				endpoint1 = result.endpoint
				audioData1 = result.audioData
			} else {
				endpoint2 = result.endpoint
				audioData2 = result.audioData
			}
		case <-time.After((m.sampleDuration + (m.sampleDuration / 5)) * time.Second):
			m.sendError(fmt.Errorf("timeout waiting for downloads"))
			return
		}
	}

	type contentResult struct {
		contentType string
		streamNum   int
	}
	contentChan := make(chan contentResult, 2)

	// Detect Content Types
	if endpoint1.ContentType == "" {
		m.sendProgress(StageDeterminingContentType1, 0.0, "Determining Stream 1 Content Type...")
		go func() {
			contentType := m.engine.DetectContentType(m.ctx, audioData1)

			time.Sleep(1 * time.Second)

			m.sendProgress(StageDeterminingContentType1, 1.0, "Stream 1 Content Type Successfully Detected")
			time.Sleep(3 * time.Second)
			contentChan <- contentResult{contentType: string(contentType), streamNum: 1}
		}()
	}

	if endpoint2.ContentType == "" {
		m.sendProgress(StageDeterminingContentType2, 0.0, "Determining Stream 2 Content Type...")
		go func() {
			contentType := m.engine.DetectContentType(m.ctx, audioData1)

			time.Sleep(1 * time.Second)

			m.sendProgress(StageDeterminingContentType2, 1.0, "Stream 2 Content Type Successfully Detected")
			time.Sleep(3 * time.Second)
			contentChan <- contentResult{contentType: string(contentType), streamNum: 2}

		}()
	}

	for range 2 {
		result := <-contentChan

		if result.streamNum == 1 {
			endpoint1.ContentType = result.contentType
		} else {
			endpoint2.ContentType = result.contentType
		}
	}

	// Now create StreamMeasurement objects for the rest of the process
	stream1Result := &latency.StreamMeasurement{
		Endpoint:        endpoint1,
		AudioData:       audioData1,
		TimeToFirstByte: stream1TTFB,
		Timestamp:       time.Now(),
	}

	stream2Result := &latency.StreamMeasurement{
		Endpoint:        endpoint2,
		AudioData:       audioData2,
		TimeToFirstByte: stream2TTFB,
		Timestamp:       time.Now(),
	}

	// Stage 3-4: Feature Extraction in parallel
	type featureResult struct {
		fingerprint     *fingerprint.AudioFingerprint
		fingerprintTime time.Duration
		totalTime       time.Duration
		streamNum       int
		err             error
	}

	featureChan := make(chan featureResult, 2)

	// Extract features for stream 1
	go func() {
		m.sendProgress(StageFeatureExtraction1, 0.0, "Extracting features for stream 1...")
		fingerprint1, fingerprint1Time, err := m.engine.ExtractFeatures(m.ctx, audioData1, endpoint1)
		if err != nil {
			featureChan <- featureResult{streamNum: 1, err: err}
			return
		}
		m.sendProgress(StageFeatureExtraction1, 1.0, "Features extracted for stream 1")

		time.Sleep(5 * time.Second)
		featureChan <- featureResult{
			fingerprint:     fingerprint1,
			fingerprintTime: fingerprint1Time,
			totalTime:       time.Since(startMeasurements),
			streamNum:       1,
		}
	}()

	// Extract features for stream 2
	go func() {
		m.sendProgress(StageFeatureExtraction2, 0.0, "Extracting features for stream 2...")
		fingerprint2, fingerprint2Time, err := m.engine.ExtractFeatures(m.ctx, audioData2, endpoint2)
		if err != nil {
			featureChan <- featureResult{streamNum: 2, err: err}
			return
		}

		m.sendProgress(StageFeatureExtraction2, 1.0, "Features extracted for stream 2")

		time.Sleep(time.Second * 5)
		featureChan <- featureResult{
			fingerprint:     fingerprint2,
			fingerprintTime: fingerprint2Time,
			totalTime:       time.Since(startMeasurements),
			streamNum:       2,
		}
	}()

	// Collect Measurements

	for range 2 {
		select {
		case result := <-featureChan:
			if result.err != nil {
				m.sendError(fmt.Errorf("feature extraction error for stream %d: %w", result.streamNum, result.err))
				return
			}

			if result.streamNum == 1 {
				stream1Result.Fingerprint = result.fingerprint
				stream1Result.FingerprintTime = result.fingerprintTime
				stream1Result.TotalProcessingTime = result.totalTime
			} else {
				stream2Result.Fingerprint = result.fingerprint
				stream2Result.FingerprintTime = result.fingerprintTime
				stream2Result.TotalProcessingTime = result.totalTime
			}
		case <-time.After((m.sampleDuration + (m.sampleDuration / 5)) * time.Second):
			m.sendError(fmt.Errorf("timeout waiting for downloads"))
			return
		}
	}

	// Stage 5: Temporal Alignment (requires both fingerprints to be ready)
	m.sendProgress(StageTemporalAlignment, 0.0, "Measuring temporal alignment...")
	alignmentResult := m.engine.MeasureAlignment(m.ctx, stream1Result, stream2Result)
	if alignmentResult.Error != nil {
		m.sendError(fmt.Errorf("alignment error: %w", alignmentResult.Error))
		return
	}
	m.sendProgress(StageTemporalAlignment, 1.0, "Temporal alignment measured")
	time.Sleep(2300 * time.Millisecond)

	// Stage 6: Truncation (already done in MeasureAlignment)
	m.sendProgress(StageTruncation, 1.0, "Streams truncated to aligned segments")
	time.Sleep(1400 * time.Millisecond)

	type fingerprintResult struct {
		result *latency.FingerprintComparison
		err    error
	}

	comparisonChan := make(chan fingerprintResult, 1)

	// Stage 7: Fingerprint Comparison
	go func() {
		m.sendProgress(StageFingerprintComparison, 0.0, "Comparing fingerprints...")
		comparisonResult := m.engine.CompareFingerprintSimilarity(
			m.ctx,
			alignmentResult.Stream1,
			alignmentResult.Stream2,
			alignmentResult.AlignmentResult,
		)
		if comparisonResult.Error != nil {
			m.sendError(fmt.Errorf("comparison error: %w", comparisonResult.Error))
			return
		}
		m.sendProgress(StageFingerprintComparison, 1.0, "Fingerprints compared successfully")

		time.Sleep(time.Millisecond * 800)
		comparisonChan <- fingerprintResult{result: comparisonResult, err: comparisonResult.Error}
	}()

	var comparisonResult *latency.FingerprintComparison
	comparisonChanResult, ok := <-comparisonChan
	if ok {
		if comparisonChanResult.err != nil {
			m.sendError(comparisonChanResult.err)
		} else {
			comparisonResult = comparisonChanResult.result
		}
	}

	// Calculate final results
	var result *ComparisonResult
	if comparisonResult != nil {
		result = &ComparisonResult{
			Stream1URL:          m.stream1URL,
			Stream2URL:          m.stream2URL,
			AlignmentSeconds:    alignmentResult.LatencySeconds,
			OffsetConfidence:    alignmentResult.AlignmentResult.OffsetConfidence,
			OverallSimilarity:   comparisonResult.SimilarityResult.OverallSimilarity,
			QualityScore:        calculateQualityScore(alignmentResult, comparisonResult),
			Stream1TTFB:         stream1Result.TimeToFirstByte,
			Stream2TTFB:         stream2Result.TimeToFirstByte,
			TotalProcessingTime: time.Since(stream1Result.Timestamp),
			Stream1Duration:     stream1Result.AudioData.Duration,
			Stream2Duration:     stream2Result.AudioData.Duration,
			IsValidAlignment:    alignmentResult.IsValidAlignment,
			IsValidMatch:        comparisonResult.IsValidMatch,
		}
	} else {
		result = &ComparisonResult{
			Stream1URL:          m.stream1URL,
			Stream2URL:          m.stream2URL,
			AlignmentSeconds:    alignmentResult.LatencySeconds,
			OffsetConfidence:    alignmentResult.AlignmentResult.OffsetConfidence,
			OverallSimilarity:   -1,
			QualityScore:        alignmentResult.AlignmentResult.AlignmentQuality / 2,
			Stream1TTFB:         stream1Result.TimeToFirstByte,
			Stream2TTFB:         stream2Result.TimeToFirstByte,
			TotalProcessingTime: time.Since(stream1Result.Timestamp),
			Stream1Duration:     stream1Result.AudioData.Duration,
			Stream2Duration:     stream2Result.AudioData.Duration,
			IsValidAlignment:    alignmentResult.IsValidAlignment,
			IsValidMatch:        false,
		}

	}

	// Send completion message
	m.sendComplete(result)
}

func (m model) sendProgress(stage Stage, progress float64, message string) {
	select {
	case m.progressChan <- progressMsg{
		stage:    stage,
		progress: progress,
		message:  message,
	}:
	default:
		// Skip
	}
}

func (m model) sendError(err error) {
	select {
	case m.errorChan <- errorMsg{err: err}:
	default:
	}
}

func (m model) sendComplete(result *ComparisonResult) {
	select {
	case m.completeChan <- completeMsg{result: result}:
	default:
	}
}

func calculateQualityScore(alignment *latency.AlignmentMeasurement, comparison *latency.FingerprintComparison) float64 {
	// Calculate a composite quality score based on:
	// - Alignment confidence
	// - Similarity score
	// - Audio quality metrics

	confidence := alignment.AlignmentResult.OffsetConfidence
	similarity := comparison.SimilarityResult.OverallSimilarity

	// Weight alignment confidence 40%, similarity 60%
	return (confidence * 0.4) + (similarity * 0.6)
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c", "q":
			m.cancel()
			return m, tea.Quit
		}

	case progressMsg:
		if msg.progress == -1 {
			return m, m.listenForUpdates()
		}

		if msg.stage > m.currentStage {
			m.currentStage = msg.stage
		}

		if p, exists := m.progress[msg.stage]; exists {
			cmd := p.SetPercent(msg.progress)
			m.progress[msg.stage] = p
			return m, tea.Batch(m.listenForUpdates(), cmd)
		}
		return m, m.listenForUpdates()

	case completeMsg:
		m.result = msg.result
		m.currentStage = StageComplete
		return m, nil

	case errorMsg:
		m.err = msg.err
		m.currentStage = StageError
		return m, tea.Quit

	case spinner.TickMsg:
		var cmd tea.Cmd
		m.spinner, cmd = m.spinner.Update(msg)
		return m, tea.Batch(cmd, m.listenForUpdates())

	case progress.FrameMsg:
		var cmds []tea.Cmd
		for stage, p := range m.progress {
			newP, cmd := p.Update(msg)
			m.progress[stage] = newP.(progress.Model)
			cmds = append(cmds, cmd)
		}
		return m, tea.Batch(cmds...)

	default:
		// Keep listening even for unknown messages
		return m, m.listenForUpdates()
	}

	return m, nil
}

func (m model) View() string {
	if m.currentStage == StageError {
		return m.renderError()
	}

	if m.currentStage == StageComplete {
		return m.renderResults()
	}

	return m.renderProgress()
}

func (m model) renderProgress() string {
	var b strings.Builder

	// Title
	b.WriteString(titleStyle.Render("🎵 Latency Benchmark CLI"))
	b.WriteString("\n")
	b.WriteString(subtitleStyle.Render("Comparing audio streams for latency and similarity"))
	b.WriteString("\n\n")

	// Stream URLs
	b.WriteString(fmt.Sprintf("📡 Stream 1: %s\n", m.stream1URL))
	b.WriteString(fmt.Sprintf("📡 Stream 2: %s\n", m.stream2URL))
	b.WriteString(fmt.Sprintf("⏱️  Duration: %.1fs\n\n", m.sampleDuration.Seconds()))

	// Progress bars
	stages := []struct {
		stage Stage
		icon  string
		label string
		show  bool
	}{
		{StageDownloadingStream1, "⬇️", "Downloading Stream 1", m.currentStage <= StageDownloadingStream2},
		{StageDownloadingStream2, "⬇️", "Downloading Stream 2", m.currentStage <= StageDownloadingStream2},
		{StageDownloadingStream1, "⬇️", "Determining Stream 1 Content Type", m.currentStage >= StageDeterminingContentType1 && m.currentStage <= StageDeterminingContentType2},
		{StageDownloadingStream2, "⬇️", "Determining Stream 2 Content Type", m.currentStage >= StageDeterminingContentType1 && m.currentStage <= StageDeterminingContentType2},
		{StageFeatureExtraction1, "🔍", "Feature Extraction 1", m.currentStage >= StageFeatureExtraction1 && m.currentStage <= StageFeatureExtraction2},
		{StageFeatureExtraction2, "🔍", "Feature Extraction 2", m.currentStage >= StageFeatureExtraction1 && m.currentStage <= StageFeatureExtraction2},
		{StageTemporalAlignment, "🎯", "Temporal Alignment", m.currentStage == StageTemporalAlignment},
		{StageTruncation, "✂️", "Truncating to Alignment", m.currentStage == StageTruncation},
		{StageFingerprintComparison, "📊", "Fingerprint Comparison", m.currentStage == StageFingerprintComparison},
	}

	for _, s := range stages {
		if !s.show {
			continue
		}

		if p, exists := m.progress[s.stage]; exists {
			status := ""
			if p.Percent() >= 1.0 {
				status = successStyle.Render("✓")
			} else if s.stage == m.currentStage {
				status = m.spinner.View()
			} else if p.Percent() > 0 {
				status = m.spinner.View() // Show spinner for any active progress
			} else {
				status = "⏳"
			}

			progressBar := progressStyle.Render(p.View())
			label := fmt.Sprintf("%s %s %s", s.icon, s.label, status)
			b.WriteString(fmt.Sprintf("%s\n%s\n", label, progressBar))
		}
	}

	b.WriteString("\n" + subtitleStyle.Render("Press 'q' or Ctrl+C to cancel"))

	return b.String()
}

func (m model) renderResults() string {
	if m.result == nil {
		return errorStyle.Render("No results available")
	}

	var b strings.Builder

	// Title
	b.WriteString(titleStyle.Render("🎉 Comparison Complete!"))
	b.WriteString("\n\n")

	// Key metrics in a nice box
	results := fmt.Sprintf(
		"%s %s\n"+
			"%s %s\n"+
			"%s %s\n"+
			"%s %s\n\n"+
			"%s\n"+
			"  %s %.3f seconds\n"+
			"  %s %.2f%%\n"+
			"  %s %.2f%%\n"+
			"  %s %.2f%%\n\n"+
			"%s\n"+
			"  %s %v\n"+
			"  %s %v\n"+
			"  %s %.1fs / %.1fs\n"+
			"  %s %v",

		metricLabelStyle.Render("📡 Stream 1:"), m.result.Stream1URL,
		metricLabelStyle.Render("📡 Stream 2:"), m.result.Stream2URL,
		metricLabelStyle.Render("⏱️  Sample:"), fmt.Sprintf("%.1fs", m.sampleDuration.Seconds()),
		metricLabelStyle.Render("🕐 Processed:"), m.result.TotalProcessingTime.Round(time.Millisecond),

		metricLabelStyle.Render("🎯 Alignment Results:"),
		metricLabelStyle.Render("   Offset:"), m.result.AlignmentSeconds,
		metricLabelStyle.Render("   Confidence:"), m.result.OffsetConfidence*100,
		metricLabelStyle.Render("   Similarity:"), m.result.OverallSimilarity*100,
		metricLabelStyle.Render("   Quality:"), m.result.QualityScore*100,

		metricLabelStyle.Render("📊 Technical Details:"),
		metricLabelStyle.Render("   Valid Alignment:"), formatBool(m.result.IsValidAlignment),
		metricLabelStyle.Render("   Valid Match:"), formatBool(m.result.IsValidMatch),
		metricLabelStyle.Render("   Durations:"), m.result.Stream1Duration.Seconds(), m.result.Stream2Duration.Seconds(),
		metricLabelStyle.Render("   TTFB:"), formatTTFB(m.result.Stream1TTFB, m.result.Stream2TTFB),
	)

	b.WriteString(resultStyle.Render(results))

	// Interpretation
	b.WriteString("\n")
	b.WriteString(m.renderInterpretation())

	return b.String()
}

func (m model) renderInterpretation() string {
	if m.result == nil {
		return ""
	}

	var interpretation strings.Builder
	interpretation.WriteString(metricLabelStyle.Render("🧠 Interpretation:") + "\n")

	// Alignment interpretation
	if m.result.AlignmentSeconds > 0.1 {
		interpretation.WriteString(warningStyle.Render(fmt.Sprintf("• Stream 2 is %.2fs behind Stream 1\n", m.result.AlignmentSeconds)))
	} else if m.result.AlignmentSeconds < -0.1 {
		interpretation.WriteString(warningStyle.Render(fmt.Sprintf("• Stream 1 is %.2fs behind Stream 2\n", -m.result.AlignmentSeconds)))
	} else {
		interpretation.WriteString(successStyle.Render("• Streams are well synchronized (< 0.1s difference)\n"))
	}

	// Confidence interpretation
	if m.result.OffsetConfidence >= 0.8 {
		interpretation.WriteString(successStyle.Render("• High confidence in alignment measurement\n"))
	} else if m.result.OffsetConfidence >= 0.6 {
		interpretation.WriteString(warningStyle.Render("• Moderate confidence in alignment measurement\n"))
	} else {
		interpretation.WriteString(errorStyle.Render("• Low confidence in alignment measurement\n"))
	}

	// Similarity interpretation
	if m.result.OverallSimilarity >= 0.8 {
		interpretation.WriteString(successStyle.Render("• Very similar content detected\n"))
	} else if m.result.OverallSimilarity >= 0.6 {
		interpretation.WriteString(warningStyle.Render("• Moderately similar content\n"))
	} else {
		interpretation.WriteString(errorStyle.Render("• Low content similarity - may be different sources\n"))
	}

	return interpretation.String()
}

func (m model) renderError() string {
	var b strings.Builder
	b.WriteString(titleStyle.Render("❌ Comparison Failed"))
	b.WriteString("\n\n")
	b.WriteString(errorStyle.Render(fmt.Sprintf("Error: %v", m.err)))
	b.WriteString("\n\n")
	b.WriteString(subtitleStyle.Render("Press 'q' or Ctrl+C to exit"))
	return b.String()
}

func formatBool(b bool) string {
	if b {
		return successStyle.Render("Yes")
	}
	return errorStyle.Render("No")
}

func formatTTFB(ttfb1, ttfb2 time.Duration) string {
	return fmt.Sprintf("%v / %v", ttfb1.Round(time.Millisecond), ttfb2.Round(time.Millisecond))
}

// CLI setup
func main() {
	var rootCmd = &cobra.Command{
		Use:   "latency-benchmark-cli",
		Short: "A beautiful CLI tool for measuring audio stream latency",
		Long:  `Compare two audio streams to measure latency, alignment, and similarity with a gorgeous terminal interface.`,
	}

	var compareCmd = &cobra.Command{
		Use:   "compare [STREAM1] [STREAM2]",
		Short: "Compare two audio streams",
		Long:  `Compare two audio streams to measure their relative latency and content similarity.`,
		Args:  cobra.ExactArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			duration, _ := cmd.Flags().GetString("duration")

			// Parse duration
			sampleDuration := 200 * time.Second // default
			if duration != "" {
				if d, err := time.ParseDuration(duration + "s"); err == nil {
					sampleDuration = d
				} else if seconds, err := strconv.Atoi(duration); err == nil {
					sampleDuration = time.Duration(seconds) * time.Second
				}
			}

			// Run the TUI
			m := initialModel(args[0], args[1], sampleDuration)
			p := tea.NewProgram(m, tea.WithAltScreen())

			if _, err := p.Run(); err != nil {
				log.Fatal(err)
			}
		},
	}

	compareCmd.Flags().StringP("duration", "d", "200", "Sample duration in seconds")

	rootCmd.AddCommand(compareCmd)

	if err := rootCmd.Execute(); err != nil {
		log.Fatal(err)
	}
}

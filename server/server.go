package server

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"net"
	"net/http"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/google/uuid"
	grpcrecovery "github.com/grpc-ecosystem/go-grpc-middleware/recovery"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/pkg/errors"
	"github.com/soheilhy/cmux"
	"go.opentelemetry.io/contrib/instrumentation/github.com/labstack/echo/otelecho"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"google.golang.org/grpc"

	"github.com/usememos/memos/internal/profile"
	storepb "github.com/usememos/memos/proto/gen/store"
	"github.com/usememos/memos/server/profiler"
	apiv1 "github.com/usememos/memos/server/router/api/v1"
	"github.com/usememos/memos/server/router/frontend"
	"github.com/usememos/memos/server/router/rss"
	"github.com/usememos/memos/server/runner/memopayload"
	"github.com/usememos/memos/server/runner/s3presign"
	"github.com/usememos/memos/store"
)

type Server struct {
	Secret  string
	Profile *profile.Profile
	Store   *store.Store

	echoServer        *echo.Echo
	grpcServer        *grpc.Server
	profiler          *profiler.Profiler
	tracerProvider    *trace.TracerProvider
	runnerCancelFuncs []context.CancelFunc
}

func InitOtel() (*trace.TracerProvider, error) {
	// Check if OpenTelemetry is enabled
	if os.Getenv("OTEL_SDK_DISABLED") == "true" {
		slog.Info("OpenTelemetry is disabled")
		return nil, nil
	}

	// Read configuration from environment variables
	endpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if endpoint == "" {
		endpoint = "http://localhost:4318" // Default OTLP HTTP endpoint
	}

	serviceName := os.Getenv("OTEL_SERVICE_NAME")
	if serviceName == "" {
		serviceName = "memos-server"
	}

	serviceVersion := os.Getenv("OTEL_SERVICE_VERSION")
	if serviceVersion == "" {
		serviceVersion = "unknown"
	}

	// Parse sampling ratio
	samplingRatio := 1.0 // Default to 100% sampling
	if ratioStr := os.Getenv("OTEL_TRACES_SAMPLER_ARG"); ratioStr != "" {
		if ratio, err := parseFloat64(ratioStr); err == nil {
			samplingRatio = ratio
		}
	}

	// Create resource with service information
	res, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName(serviceName),
			semconv.ServiceVersion(serviceVersion),
			attribute.String("environment", os.Getenv("OTEL_ENVIRONMENT")),
			// attribute.String("deployment.environment", os.Getenv("DEPLOYMENT_ENVIRONMENT")),
		),
	)
	if err != nil {
		slog.Error("failed to create otel resource", "error", err)
		return nil, errors.Wrap(err, "failed to create otel resource")
	}

	// Create OTLP HTTP exporter
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	exporterOptions := []otlptracehttp.Option{
		otlptracehttp.WithEndpoint(endpoint),
	}

	// Use insecure connection if explicitly set or if using localhost
	if os.Getenv("OTEL_EXPORTER_OTLP_INSECURE") == "true" || strings.Contains(endpoint, "localhost") {
		exporterOptions = append(exporterOptions, otlptracehttp.WithInsecure())
	}

	// Add headers if provided
	if headers := os.Getenv("OTEL_EXPORTER_OTLP_HEADERS"); headers != "" {
		headerMap := make(map[string]string)
		for _, header := range strings.Split(headers, ",") {
			parts := strings.SplitN(header, "=", 2)
			if len(parts) == 2 {
				headerMap[strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
			}
		}
		exporterOptions = append(exporterOptions, otlptracehttp.WithHeaders(headerMap))
	}

	exporter, err := otlptracehttp.New(ctx, exporterOptions...)
	if err != nil {
		slog.Error("failed to create otlp exporter", "error", err)
		return nil, errors.Wrap(err, "failed to create otlp exporter")
	}

	// Create tracer provider
	tp := trace.NewTracerProvider(
		trace.WithBatcher(exporter),
		trace.WithResource(res),
		trace.WithSampler(trace.TraceIDRatioBased(samplingRatio)),
	)

	// Set global tracer provider
	otel.SetTracerProvider(tp)

	// Set up propagators
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	slog.Info("OpenTelemetry initialized",
		"endpoint", endpoint,
		"service_name", serviceName,
		"service_version", serviceVersion,
		"sampling_ratio", samplingRatio,
	)

	return tp, nil
}

func parseFloat64(s string) (float64, error) {
	if f, err := fmt.Sscanf(s, "%f", new(float64)); err == nil && f == 1 {
		var result float64
		fmt.Sscanf(s, "%f", &result)
		return result, nil
	}
	return 0, fmt.Errorf("invalid float64: %s", s)
}

func NewServer(ctx context.Context, profile *profile.Profile, store *store.Store) (*Server, error) {
	// Initialize OpenTelemetry
	tracerProvider, err := InitOtel()
	if err != nil {
		slog.Warn("failed to initialize OpenTelemetry", "error", err)
	}

	s := &Server{
		Store:          store,
		Profile:        profile,
		tracerProvider: tracerProvider,
	}

	echoServer := echo.New()
	echoServer.Debug = true
	echoServer.HideBanner = true
	echoServer.HidePort = true

	echoServer.Use(middleware.Recover())

	// Add OpenTelemetry middleware
	echoServer.Use(otelecho.Middleware("memos-server",
		otelecho.WithTracerProvider(otel.GetTracerProvider()),
		otelecho.WithSkipper(func(c echo.Context) bool {
			// Skip tracing for health check and static assets
			path := c.Request().URL.Path
			return path == "/healthz" ||
				path == "/favicon.ico" ||
				strings.HasPrefix(path, "/assets/") ||
				strings.HasPrefix(path, "/static/")
		}),
	))

	// Add request logging middleware
	echoServer.Use(middleware.LoggerWithConfig(middleware.LoggerConfig{
		Format: "time=${time_rfc3339} method=${method} uri=${uri} status=${status} " +
			"latency=${latency_human} bytes_in=${bytes_in} bytes_out=${bytes_out} " +
			"remote_ip=${remote_ip} user_agent=${user_agent}\n",
		CustomTimeFormat: "2006-01-02 15:04:05",
	}))
	s.echoServer = echoServer

	// Initialize profiler
	s.profiler = profiler.NewProfiler()
	s.profiler.RegisterRoutes(echoServer)
	s.profiler.StartMemoryMonitor(ctx)

	workspaceBasicSetting, err := s.getOrUpsertWorkspaceBasicSetting(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get workspace basic setting")
	}

	secret := "usememos"
	if profile.Mode == "prod" {
		secret = workspaceBasicSetting.SecretKey
	}
	s.Secret = secret

	// Register healthz endpoint.
	echoServer.GET("/healthz", func(c echo.Context) error {
		return c.String(http.StatusOK, "Service ready.")
	})

	// Serve frontend resources.
	frontend.NewFrontendService(profile, store).Serve(ctx, echoServer)

	rootGroup := echoServer.Group("")

	// Create and register RSS routes.
	rss.NewRSSService(s.Profile, s.Store).RegisterRoutes(rootGroup)

	grpcServer := grpc.NewServer(
		// Override the maximum receiving message size to math.MaxInt32 for uploading large resources.
		grpc.MaxRecvMsgSize(math.MaxInt32),
		grpc.ChainUnaryInterceptor(
			apiv1.NewLoggerInterceptor().LoggerInterceptor,
			grpcrecovery.UnaryServerInterceptor(),
			apiv1.NewGRPCAuthInterceptor(store, secret).AuthenticationInterceptor,
		))
	s.grpcServer = grpcServer

	apiV1Service := apiv1.NewAPIV1Service(s.Secret, profile, store, grpcServer)
	// Register gRPC gateway as api v1.
	if err := apiV1Service.RegisterGateway(ctx, echoServer); err != nil {
		return nil, errors.Wrap(err, "failed to register gRPC gateway")
	}

	return s, nil
}

func (s *Server) Start(ctx context.Context) error {
	var address, network string
	if len(s.Profile.UNIXSock) == 0 {
		address = fmt.Sprintf("%s:%d", s.Profile.Addr, s.Profile.Port)
		network = "tcp"
	} else {
		address = s.Profile.UNIXSock
		network = "unix"
	}
	listener, err := net.Listen(network, address)
	if err != nil {
		return errors.Wrap(err, "failed to listen")
	}

	muxServer := cmux.New(listener)
	go func() {
		grpcListener := muxServer.MatchWithWriters(cmux.HTTP2MatchHeaderFieldSendSettings("content-type", "application/grpc"))
		if err := s.grpcServer.Serve(grpcListener); err != nil {
			slog.Error("failed to serve gRPC", "error", err)
		}
	}()
	go func() {
		httpListener := muxServer.Match(cmux.HTTP1Fast(http.MethodPatch))
		s.echoServer.Listener = httpListener
		if err := s.echoServer.Start(address); err != nil {
			slog.Error("failed to start echo server", "error", err)
		}
	}()
	go func() {
		if err := muxServer.Serve(); err != nil {
			slog.Error("mux server listen error", "error", err)
		}
	}()
	s.StartBackgroundRunners(ctx)

	return nil
}

func (s *Server) Shutdown(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	slog.Info("server shutting down")

	// Cancel all background runners
	for _, cancelFunc := range s.runnerCancelFuncs {
		if cancelFunc != nil {
			cancelFunc()
		}
	}

	// Shutdown echo server.
	if err := s.echoServer.Shutdown(ctx); err != nil {
		slog.Error("failed to shutdown server", slog.String("error", err.Error()))
	}

	// Shutdown gRPC server.
	s.grpcServer.GracefulStop()

	// Shutdown OpenTelemetry tracer provider
	if s.tracerProvider != nil {
		if err := s.tracerProvider.Shutdown(ctx); err != nil {
			slog.Error("failed to shutdown tracer provider", "error", err)
		} else {
			slog.Info("tracer provider shutdown successfully")
		}
	}

	// Stop the profiler
	if s.profiler != nil {
		slog.Info("stopping profiler")
		// Log final memory stats
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		slog.Info("final memory stats before exit",
			"heapAlloc", m.Alloc,
			"heapSys", m.Sys,
			"heapObjects", m.HeapObjects,
			"numGoroutine", runtime.NumGoroutine(),
		)
	}

	// Close database connection.
	if err := s.Store.Close(); err != nil {
		slog.Error("failed to close database", slog.String("error", err.Error()))
	}

	slog.Info("memos stopped properly")
}

func (s *Server) StartBackgroundRunners(ctx context.Context) {
	// Create a separate context for each background runner
	// This allows us to control cancellation for each runner independently
	s3Context, s3Cancel := context.WithCancel(ctx)

	// Store the cancel function so we can properly shut down runners
	s.runnerCancelFuncs = append(s.runnerCancelFuncs, s3Cancel)

	// Create and start S3 presign runner
	s3presignRunner := s3presign.NewRunner(s.Store)
	s3presignRunner.RunOnce(ctx)

	// Create and start memo payload runner just once
	memopayloadRunner := memopayload.NewRunner(s.Store)
	// Rebuild all memos' payload after server starts.
	memopayloadRunner.RunOnce(ctx)

	// Start continuous S3 presign runner
	go func() {
		s3presignRunner.Run(s3Context)
		slog.Info("s3presign runner stopped")
	}()

	// Log the number of goroutines running
	slog.Info("background runners started", "goroutines", runtime.NumGoroutine())
}

func (s *Server) getOrUpsertWorkspaceBasicSetting(ctx context.Context) (*storepb.WorkspaceBasicSetting, error) {
	workspaceBasicSetting, err := s.Store.GetWorkspaceBasicSetting(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get workspace basic setting")
	}
	modified := false
	if workspaceBasicSetting.SecretKey == "" {
		workspaceBasicSetting.SecretKey = uuid.NewString()
		modified = true
	}
	if modified {
		workspaceSetting, err := s.Store.UpsertWorkspaceSetting(ctx, &storepb.WorkspaceSetting{
			Key:   storepb.WorkspaceSettingKey_BASIC,
			Value: &storepb.WorkspaceSetting_BasicSetting{BasicSetting: workspaceBasicSetting},
		})
		if err != nil {
			return nil, errors.Wrap(err, "failed to upsert workspace setting")
		}
		workspaceBasicSetting = workspaceSetting.GetBasicSetting()
	}
	return workspaceBasicSetting, nil
}

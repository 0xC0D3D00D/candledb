package server

import (
	"context"
	"net"
	"net/http"
	"strings"
	"time"

	"connectrpc.com/connect"
	"connectrpc.com/grpcreflect"
	"connectrpc.com/otelconnect"
	"connectrpc.com/validate"
	"connectrpc.com/vanguard"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/sdk/metric"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

type Server struct {
	srv *http.Server
}

type (
	HandlerFunc    func(opts ...connect.HandlerOption) (string, http.Handler)
	HandlerFuncOpt func(*http.ServeMux, ...connect.HandlerOption) (string, *vanguard.Service)
)

func WithHandlerFunc(fn HandlerFunc) HandlerFuncOpt {
	return func(mux *http.ServeMux, opts ...connect.HandlerOption) (string, *vanguard.Service) {
		srvName, handler := fn(opts...)
		return srvName, vanguard.NewService(srvName, handler)
	}
}

func New(
	ctx context.Context,
	address string,
	handlers ...HandlerFuncOpt,
) (*Server, error) {
	mux := http.NewServeMux()

	// OpenTelemetry and prometheus metrics
	otelPrometheusExporter, err := prometheus.New()
	metricsProvider := metric.NewMeterProvider(metric.WithReader(otelPrometheusExporter))
	mux.Handle("/metrics", promhttp.Handler())

	otelInterceptor, err := otelconnect.NewInterceptor(otelconnect.WithMeterProvider(metricsProvider))
	if err != nil {
		return nil, err
	}

	// Validation set up
	validateInterceptor, err := validate.NewInterceptor()
	if err != nil {
		return nil, err
	}

	// Service registration and HTTP transcoding
	serviceNames := []string{}
	services := []*vanguard.Service{}
	for _, handler := range handlers {
		name, srv := handler(mux, connect.WithInterceptors(otelInterceptor, validateInterceptor))
		serviceNames = append(serviceNames, strings.ReplaceAll(name, "/", ""))
		services = append(services, srv)
	}

	// Customize json codec to use snake case proto names instead of camel case json names
	codec := func(res vanguard.TypeResolver) vanguard.Codec {
		jsonCodec := vanguard.NewJSONCodec(res)
		jsonCodec.MarshalOptions.UseProtoNames = true
		jsonCodec.MarshalOptions.EmitUnpopulated = false
		return jsonCodec
	}

	transcoder, err := vanguard.NewTranscoder(services, vanguard.WithCodec(codec))
	if err != nil {
		return nil, err
	}
	mux.Handle("/", transcoder)

	// gRPC reflection services
	reflector := grpcreflect.NewStaticReflector(serviceNames...)
	mux.Handle(grpcreflect.NewHandlerV1(reflector))
	mux.Handle(grpcreflect.NewHandlerV1Alpha(reflector))

	// Liveliness and readiness probes
	mux.HandleFunc("/healthz", healthZHandleFunc())
	mux.HandleFunc("/readyz", readyZHandleFunc(ctx))

	srv := &http.Server{
		Addr: address,
		// Use h2c, so we can serve HTTP/2 without TLS.
		Handler: h2c.NewHandler(
			mux,
			&http2.Server{},
		),
		ReadHeaderTimeout: time.Second,
		ReadTimeout:       1 * time.Minute,
		WriteTimeout:      1 * time.Minute,
		MaxHeaderBytes:    16 * 1024, // 16KiB
		BaseContext: func(listener net.Listener) context.Context {
			return ctx
		},
	}

	return &Server{
		srv: srv,
	}, nil
}

func (s *Server) Serve(l net.Listener) error {
	return s.srv.Serve(l)
}

func (s *Server) Shutdown(ctx context.Context) error {
	if s.srv == nil {
		return nil
	}
	return s.srv.Shutdown(ctx)
}

var (
	statusHealthy    = []byte(`{"status":"HEALTHY"}`)
	statusNotServing = []byte(`{"status":"NOT_SERVING"}`)
	statusServing    = []byte(`{"status":"SERVING"}`)
)

func readyZHandleFunc(ctx context.Context) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("Content-Type", "application/json")
		if ctx.Err() != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write(statusNotServing)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write(statusServing)
	}
}

func healthZHandleFunc() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write(statusHealthy)
	}
}

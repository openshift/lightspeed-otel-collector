package httpsmetrics

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension"
	"go.uber.org/zap"
)

type httpsMetrics struct {
	config   *Config
	logger   *zap.Logger
	server   *http.Server
	listener net.Listener
}

var _ extension.Extension = (*httpsMetrics)(nil)

func newHTTPSMetrics(set extension.Settings, cfg *Config) *httpsMetrics {
	return &httpsMetrics{
		config: cfg,
		logger: set.Logger,
	}
}

func (h *httpsMetrics) Start(_ context.Context, _ component.Host) error {
	upstream, err := url.Parse(h.config.Upstream)
	if err != nil {
		return err
	}

	proxy := &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.Out.URL.Scheme = upstream.Scheme
			pr.Out.URL.Host = upstream.Host
			pr.Out.URL.Path = upstream.Path
			pr.Out.Host = upstream.Host
		},
		ErrorLog: zap.NewStdLog(h.logger),
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", proxy)

	h.server = &http.Server{
		Addr:              h.config.Endpoint,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	listener, err := net.Listen("tcp", h.config.Endpoint)
	if err != nil {
		return err
	}
	h.listener = listener

	if h.config.TLSCertFile != "" {
		if _, err := os.Stat(h.config.TLSCertFile); err != nil {
			_ = listener.Close()
			return fmt.Errorf("tls_cert_file: %w", err)
		}
		if _, err := os.Stat(h.config.TLSKeyFile); err != nil {
			_ = listener.Close()
			return fmt.Errorf("tls_key_file: %w", err)
		}
		h.logger.Info("https_metrics: serving HTTPS",
			zap.String("endpoint", h.config.Endpoint),
			zap.String("upstream", h.config.Upstream))
		go func() {
			if err := h.server.ServeTLS(listener, h.config.TLSCertFile, h.config.TLSKeyFile); err != nil && err != http.ErrServerClosed {
				h.logger.Error("https_metrics: ServeTLS failed", zap.Error(err))
			}
		}()
	} else {
		h.logger.Info("https_metrics: serving plain HTTP (no TLS configured)",
			zap.String("endpoint", h.config.Endpoint),
			zap.String("upstream", h.config.Upstream))
		go func() {
			if err := h.server.Serve(listener); err != nil && err != http.ErrServerClosed {
				h.logger.Error("https_metrics: Serve failed", zap.Error(err))
			}
		}()
	}

	return nil
}

func (h *httpsMetrics) Shutdown(ctx context.Context) error {
	if h.server == nil {
		return nil
	}
	return h.server.Shutdown(ctx)
}

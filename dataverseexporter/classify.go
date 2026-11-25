package dataverseexporter

import (
	"errors"
	"fmt"
	"net/http"

	"go.opentelemetry.io/collector/consumer/consumererror"
)

// permanentUploadError marks an upload failure that can never succeed on
// retry, so the exporter drops the batch and counts it instead of feeding
// it back to retry_on_failure. reason is the drop-counter label value
// (e.g. "http_403", "credentials").
type permanentUploadError struct {
	reason string
	err    error
}

func (e *permanentUploadError) Error() string {
	return fmt.Sprintf("permanent upload failure (%s): %v", e.reason, e.err)
}

func (e *permanentUploadError) Unwrap() error { return e.err }

// dropReasonFor returns the drop-counter reason for a permanent error,
// defaulting to "permanent" when the error is not one of ours.
func dropReasonFor(err error) string {
	var perr *permanentUploadError
	if errors.As(err, &perr) {
		return perr.reason
	}
	return "permanent"
}

// classifyUploadError wraps retryable-vs-permanent policy around upload
// errors:
//
//   - credential shape errors (missing cloud.openshift.com entry,
//     malformed .dockerconfigjson) are permanent, reason "credentials";
//   - 4xx statuses except 401/408/429 are permanent, reason "http_<status>";
//   - everything else (network/timeout, 408, 429, 5xx, 401 even after a
//     failed refresh) stays retryable.
//
// Availability errors from the Kubernetes API server are not wrapped:
// they stay retryable via the plain error path.
func classifyUploadError(err error) error {
	if err == nil {
		return nil
	}
	var perr *permanentUploadError
	if errors.As(err, &perr) {
		return err // already classified
	}
	var shape *errCredentialShape
	if errors.As(err, &shape) {
		return consumererror.NewPermanent(&permanentUploadError{reason: dropReasonCredentials, err: err})
	}
	var statusErr *httpStatusError
	if errors.As(err, &statusErr) && isPermanentHTTPStatus(statusErr.code) {
		return consumererror.NewPermanent(&permanentUploadError{
			reason: fmt.Sprintf("http_%d", statusErr.code),
			err:    err,
		})
	}
	return err
}

// dropReasonCredentials is the drop-counter reason for credential shape
// errors.
const dropReasonCredentials = "credentials"

// isPermanentHTTPStatus reports whether an ingress status is a permanent
// failure: client errors that are not auth/rate-limit/timeout (401, 408,
// 429) and not a server error. Retrying identical bytes against a 403 or
// 413 cannot succeed.
func isPermanentHTTPStatus(code int) bool {
	if code < 400 || code >= 500 {
		return false
	}
	switch code {
	case http.StatusUnauthorized, http.StatusRequestTimeout, http.StatusTooManyRequests:
		return false
	}
	return true
}

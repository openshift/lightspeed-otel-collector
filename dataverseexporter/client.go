package dataverseexporter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/textproto"

	"go.uber.org/zap"
)

const (
	uploadFieldName   = "file"
	uploadFilename    = "lightspeed-assistant.tgz"
	userAgentFormat   = "openshift-lightspeed-operator/user-data-collection cluster/%s"
	contentTypeFormat = "application/vnd.redhat.%s.periodic+tar"
	maxResponseRead   = 1 << 20
	maxErrorBodyBytes = 1 << 10
)

// httpStatusError retains the status for retry classification and a bounded body excerpt for diagnostics.
type httpStatusError struct {
	code int
	body string
}

func (e *httpStatusError) Error() string {
	return fmt.Sprintf("ingress returned status %d: %s", e.code, e.body)
}

// truncate bounds b to at most n bytes.
func truncate(b []byte, n int) []byte {
	if len(b) <= n {
		return b
	}
	return b[:n]
}

// ingressClient uploads packaged payloads to the Dataverse ingress API.
type ingressClient struct {
	// client is injected and owned by the factory (confighttp-based):
	// timeout, TLS, and redirect policy are configured there, not here.
	// We deliberately do not clone or mutate it.
	client    *http.Client
	endpoint  string
	serviceID string
	auth      *clusterAuth
	logger    *zap.Logger
}

// upload returns the request ID on acceptance. After a 401 it may refresh the
// credential and resend the same multipart body, subject to the refresh interval.
func (c *ingressClient) upload(ctx context.Context, payload *bytes.Reader) (string, error) {
	body, contentType, err := c.buildMultipartBody(payload)
	if err != nil {
		return "", err
	}
	bodyReader := bytes.NewReader(body)

	requestID, err := c.doUpload(ctx, bodyReader, contentType)
	if err == nil {
		return requestID, nil
	}
	var statusErr *httpStatusError
	if errors.As(err, &statusErr) && statusErr.code == http.StatusUnauthorized {
		if !c.auth.ShouldRefresh() {
			// Leave the next attempt to exporterhelper rather than immediately
			// resending the credential that ingress rejected.
			return "", err
		}
		c.logger.Warn("ingress rejected the upload token; refreshing and retrying once")
		if _, rerr := c.auth.UploadToken(ctx, true); rerr != nil {
			return "", classifyUploadError(fmt.Errorf("refreshing upload token after 401: %w", rerr))
		}
		if _, serr := bodyReader.Seek(0, io.SeekStart); serr != nil {
			return "", fmt.Errorf("seeking body for retry: %w", serr)
		}
		retryID, err := c.doUpload(ctx, bodyReader, contentType)
		if err != nil {
			return "", classifyUploadError(err)
		}
		return retryID, nil
	}
	return "", classifyUploadError(err)
}

// buildMultipartBody assembles the single-file multipart upload body.
func (c *ingressClient) buildMultipartBody(payload *bytes.Reader) ([]byte, string, error) {
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	part, err := writer.CreatePart(textproto.MIMEHeader{
		"Content-Disposition": {fmt.Sprintf(`form-data; name="%s"; filename="%s"`, uploadFieldName, uploadFilename)},
		"Content-Type":        {fmt.Sprintf(contentTypeFormat, c.serviceID)},
	})
	if err != nil {
		return nil, "", err
	}
	if _, err := io.Copy(part, payload); err != nil {
		return nil, "", err
	}
	if err := writer.Close(); err != nil {
		return nil, "", err
	}
	return body.Bytes(), writer.FormDataContentType(), nil
}

// doUpload performs one HTTP attempt. Non-202 statuses, credential shape
// errors, and other failures are classified as permanent or retryable by
// classifyUploadError (called at upload's error returns).
func (c *ingressClient) doUpload(ctx context.Context, body io.Reader, contentType string) (string, error) {
	token, err := c.auth.UploadToken(ctx, false)
	if err != nil {
		return "", classifyUploadError(err)
	}
	clusterID, err := c.auth.ClusterID(ctx, false)
	if err != nil {
		return "", err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, body)
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("User-Agent", fmt.Sprintf(userAgentFormat, clusterID))

	resp, err := c.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	respBody, readErr := io.ReadAll(io.LimitReader(resp.Body, maxResponseRead))
	if resp.StatusCode != http.StatusAccepted {
		return "", &httpStatusError{
			code: resp.StatusCode,
			body: string(truncate(respBody, maxErrorBodyBytes)),
		}
	}
	if readErr != nil {
		return "", fmt.Errorf("reading ingress 202 response body: %w", readErr)
	}
	var accepted struct {
		RequestID string `json:"request_id"`
	}
	if err := json.Unmarshal(respBody, &accepted); err != nil || accepted.RequestID == "" {
		return "", fmt.Errorf("ingress 202 response missing request_id: %q", truncate(respBody, maxErrorBodyBytes))
	}
	return accepted.RequestID, nil
}

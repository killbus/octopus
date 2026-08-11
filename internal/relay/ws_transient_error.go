package relay

import (
	"errors"
	"strings"

	"github.com/bestruirui/octopus/internal/relay/stream"
)

// isTransientUpstreamTransportError reports failures that describe a temporary
// interruption of the upstream stream rather than invalid conversation state.
// These errors are safe to retry only on the already selected channel/key/URL.
func isTransientUpstreamTransportError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, stream.ErrEmptyUpstreamStream) || isUpstreamWSConnectionBroken(err) {
		return true
	}

	message := relayErrorMessage(err)
	var wsErr *wsUpstreamEventError
	if errors.As(err, &wsErr) && wsErr != nil {
		message = strings.Join([]string{
			message,
			strings.ToLower(wsErr.Code),
			strings.ToLower(wsErr.Type),
			strings.ToLower(wsErr.Message),
		}, " ")
	}
	return strings.Contains(message, "upstream continuation transport temporarily unavailable") ||
		strings.Contains(message, "ws stream ended before first event") ||
		strings.Contains(message, "stream disconnected") ||
		strings.Contains(message, "stream_disconnected")
}

package zerobus

import "fmt"

// ZerobusError represents an error from the Zerobus SDK
type ZerobusError struct {
	Message     string
	IsRetryable bool
}

func (e *ZerobusError) Error() string {
	if e.IsRetryable {
		return fmt.Sprintf("ZerobusError (retryable): %s", e.Message)
	}
	return fmt.Sprintf("ZerobusError: %s", e.Message)
}

// Retryable returns whether this error can be retried. After a Flush or WaitForOffset
// timeout, retrying means creating a new stream and resending the unacknowledged
// records, because the timed-out stream does not send them again.
func (e *ZerobusError) Retryable() bool {
	return e.IsRetryable
}

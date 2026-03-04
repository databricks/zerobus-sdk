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

// Retryable returns whether this error can be retried
func (e *ZerobusError) Retryable() bool {
	return e.IsRetryable
}

package utils

import (
	"log"
	"time"
)

// DoWithRetry executes an operation and automatically retries it if it fails.
// It uses an "Exponential Backoff" strategy: the waiting time between retries
// will double until it reaches a maximum limit (maxDelay).
func DoWithRetry(maxRetries int, baseDelay time.Duration, maxDelay time.Duration, operation func() error) error {
	var err error
	backoff := baseDelay

	// The loop includes the first run (so maxRetries + 1)
	for i := 1; i <= maxRetries+1; i++ {
		err = operation()
		if err == nil {
			return nil // Success
		}

		if i <= maxRetries {
			log.Printf("RETRY: Operation failed (attempt %d/%d). Retrying after %v. Error: %v", i, maxRetries, backoff, err)
			time.Sleep(backoff)

			// Double the waiting time for the next retry.
			backoff *= 2
			if backoff > maxDelay {
				backoff = maxDelay // Do not allow the waiting time to exceed the limit.
			}
		}
	}

	log.Printf("RETRY: Giving up after %d attempts. Last error: %v", maxRetries, err)
	return err
}

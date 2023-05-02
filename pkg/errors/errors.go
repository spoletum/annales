package errors

import "fmt"

func InvalidStreamVersionError() error {
	return fmt.Errorf("invalid stream version")
}

package errors_test

import (
	"testing"

	"github.com/spoletum/annales/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestInvalidStreamVersionError(t *testing.T) {
	assert.Error(t, errors.InvalidStreamVersionError())
}

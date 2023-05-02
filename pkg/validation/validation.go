/*
Copyright (c) 2023 Alessandro Santini

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package validation

import (
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
)

type Rule func(interface{}) error

type Validation struct {
	fieldName string
	value     any
	rules     []Rule
}

type Validator struct {
	validations []*Validation
	errors      []error
}

func NewValidator() *Validator {
	return &Validator{}
}

func (v *Validator) Add(fieldName string, value any, rules ...Rule) *Validator {
	validation := &Validation{
		fieldName: fieldName,
		value:     value,
		rules:     rules,
	}
	v.validations = append(v.validations, validation)
	return v
}

func (v *Validator) Validate() *Validator {
	for _, validation := range v.validations {
		for _, rule := range validation.rules {
			if err := rule(validation.value); err != nil {
				v.errors = append(v.errors, fmt.Errorf("%s: %v", validation.fieldName, err))
			}
		}
	}
	return v
}

func (v *Validator) Error() error {
	if len(v.errors) == 0 {
		return nil
	}
	messages := make([]string, len(v.errors))
	for i, err := range v.errors {
		messages[i] = err.Error()
	}
	return errors.New(strings.Join(messages, "; "))
}

func NotEmpty() Rule {
	return func(value interface{}) error {
		if str, ok := value.(*string); ok && *str == "" {
			return errors.New("value cannot be empty")
		}
		return nil
	}
}

func MaxLength(max int) Rule {
	return func(value interface{}) error {
		if str, ok := value.(*string); ok && len(*str) > max {
			return fmt.Errorf("value exceeds max length of %d", max)
		}
		return nil
	}
}

func MinLength(min int) Rule {
	return func(value interface{}) error {
		if str, ok := value.(*string); ok && len(*str) < min {
			return fmt.Errorf("value must have a minimum length of %d", min)
		}
		return nil
	}
}

func IntGreaterThan(threshold int) Rule {
	return func(value interface{}) error {
		if intValue, ok := value.(*int); ok && *intValue <= threshold {
			return fmt.Errorf("value must be greater than %d", threshold)
		}
		return nil
	}
}

func IntGreaterThanOrEqual(threshold int) Rule {
	return func(value interface{}) error {
		if intValue, ok := value.(*int); ok && *intValue < threshold {
			return fmt.Errorf("value must be greater than or equal to %d", threshold)
		}
		return nil
	}
}

func IntLessThan(threshold int) Rule {
	return func(value interface{}) error {
		if intValue, ok := value.(*int); ok && *intValue >= threshold {
			return fmt.Errorf("value must be less than %d", threshold)
		}
		return nil
	}
}

func IntLessThanOrEqual(threshold int) Rule {
	return func(value interface{}) error {
		if intValue, ok := value.(*int); ok && *intValue > threshold {
			return fmt.Errorf("value must be less than or equal to %d", threshold)
		}
		return nil
	}
}

func FloatGreaterThan(threshold float64) Rule {
	return func(value interface{}) error {
		if floatValue, ok := value.(*float64); ok && *floatValue <= threshold {
			return fmt.Errorf("value must be greater than %f", threshold)
		}
		return nil
	}
}

func FloatGreaterThanOrEqual(threshold float64) Rule {
	return func(value interface{}) error {
		if floatValue, ok := value.(*float64); ok && *floatValue < threshold {
			return fmt.Errorf("value must be greater than or equal to %f", threshold)
		}
		return nil
	}
}

func FloatLessThan(threshold float64) Rule {
	return func(value interface{}) error {
		if floatValue, ok := value.(*float64); ok && *floatValue >= threshold {
			return fmt.Errorf("value must be less than %f", threshold)
		}
		return nil
	}
}

func FloatLessThanOrEqual(threshold float64) Rule {
	return func(value interface{}) error {
		if floatValue, ok := value.(*float64); ok && *floatValue > threshold {
			return fmt.Errorf("value must be less than or equal to %f", threshold)
		}
		return nil
	}
}

func Int64GreaterThan(threshold int64) Rule {
	return func(value interface{}) error {
		if intValue, ok := value.(*int64); ok && *intValue <= threshold {
			return fmt.Errorf("value must be greater than %d", threshold)
		}
		return nil
	}
}

func Int64GreaterThanOrEqual(threshold int64) Rule {
	return func(value interface{}) error {
		if intValue, ok := value.(*int64); ok && *intValue < threshold {
			return fmt.Errorf("value must be greater than or equal to %d", threshold)
		}
		return nil
	}
}

func Int64LessThan(threshold int64) Rule {
	return func(value interface{}) error {
		if intValue, ok := value.(*int64); ok && *intValue >= threshold {
			return fmt.Errorf("value must be less than %d", threshold)
		}
		return nil
	}
}

func Int64LessThanOrEqual(threshold int64) Rule {
	return func(value interface{}) error {
		if intValue, ok := value.(*int64); ok && *intValue > threshold {
			return fmt.Errorf("value must be less than or equal to %d", threshold)
		}
		return nil
	}
}

func NotNil() Rule {
	return func(value interface{}) error {
		if value == nil {
			return fmt.Errorf("value cannot be nil")
		}
		return nil
	}
}

func IsUUID() Rule {
	return func(value interface{}) error {
		if strValue, ok := value.(*string); ok {
			_, err := uuid.Parse(*strValue)
			if err != nil {
				return fmt.Errorf("value is not a valid UUID")
			}
		}
		return nil
	}
}

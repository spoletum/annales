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
package validation_test

import (
	"testing"

	"github.com/spoletum/annales/pkg/validation"
	"github.com/stretchr/testify/assert"
)

func TestValidator(t *testing.T) {
	testCases := []struct {
		name          string
		validator     *validation.Validator
		expectedError string
	}{
		{
			name: "All rules pass",
			validator: validation.NewValidator().
				Add("stringVar1", newString("hello"), validation.NotEmpty(), validation.MaxLength(30), validation.MinLength(5)).
				Add("stringVar2", newString("world"), validation.NotEmpty(), validation.MaxLength(10)),
		},
		{
			name: "Multiple rules fail",
			validator: validation.NewValidator().
				Add("stringVar1", newString(""), validation.NotEmpty(), validation.MaxLength(30), validation.MinLength(5)).
				Add("stringVar2", newString("toolongstring"), validation.NotEmpty(), validation.MaxLength(10)),
			expectedError: "stringVar1: string cannot be empty; stringVar1: value must have a minimum length of 5; stringVar2: value exceeds max length of 10",
		},
		{
			name: "Single rule fails",
			validator: validation.NewValidator().
				Add("stringVar1", newString("hello"), validation.NotEmpty(), validation.MaxLength(30), validation.MinLength(5)).
				Add("stringVar2", newString(""), validation.NotEmpty(), validation.MaxLength(10)),
			expectedError: "stringVar2: string cannot be empty",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			validator := tc.validator.Validate()
			if tc.expectedError != "" {
				assert.Equal(t, tc.expectedError, validator.Error().Error())
			} else {
				assert.Nil(t, validator.Error())
			}
		})
	}
}

func TestNumericRules(t *testing.T) {
	testCases := []struct {
		name          string
		validator     *validation.Validator
		expectedError string
	}{
		{
			name: "All rules pass",
			validator: validation.NewValidator().
				Add("intVar1", newInt(10), validation.IntGreaterThan(5), validation.IntLessThan(15)).
				Add("floatVar1", newFloat(5.5), validation.FloatGreaterThan(5.0), validation.FloatLessThan(6.0)),
		},
		{
			name: "Greater/Less rules fail",
			validator: validation.NewValidator().
				Add("intVar1", newInt(5), validation.IntGreaterThan(5), validation.IntLessThan(5)).
				Add("floatVar1", newFloat(5.0), validation.FloatGreaterThan(5.0), validation.FloatLessThan(5.0)),
			expectedError: "intVar1: value must be greater than 5; intVar1: value must be less than 5; floatVar1: value must be greater than 5.000000; floatVar1: value must be less than 5.000000",
		},
		{
			name: "GreaterThanOrEqual/LessThanOrEqual rules fail",
			validator: validation.NewValidator().
				Add("intVar1", newInt(5), validation.IntGreaterThanOrEqual(6), validation.IntLessThanOrEqual(4)).
				Add("floatVar1", newFloat(5.0), validation.FloatGreaterThanOrEqual(5.5), validation.FloatLessThanOrEqual(4.5)),
			expectedError: "intVar1: value must be greater than or equal to 6; intVar1: value must be less than or equal to 4; floatVar1: value must be greater than or equal to 5.500000; floatVar1: value must be less than or equal to 4.500000",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			validator := tc.validator.Validate()
			if tc.expectedError != "" {
				assert.Equal(t, tc.expectedError, validator.Error().Error())
			} else {
				assert.Nil(t, validator.Error())
			}
		})
	}
}

func TestInt64Rules(t *testing.T) {
	testCases := []struct {
		name          string
		validator     *validation.Validator
		expectedError string
	}{
		{
			name: "All rules pass",
			validator: validation.NewValidator().
				Add("int64Var1", newInt64(10), validation.Int64GreaterThan(5), validation.Int64LessThan(15)),
		},
		{
			name: "Greater/Less rules fail",
			validator: validation.NewValidator().
				Add("int64Var1", newInt64(5), validation.Int64GreaterThan(5), validation.Int64LessThan(5)),
			expectedError: "int64Var1: value must be greater than 5; int64Var1: value must be less than 5",
		},
		{
			name: "GreaterThanOrEqual/LessThanOrEqual rules fail",
			validator: validation.NewValidator().
				Add("int64Var1", newInt64(5), validation.Int64GreaterThanOrEqual(6), validation.Int64LessThanOrEqual(4)),
			expectedError: "int64Var1: value must be greater than or equal to 6; int64Var1: value must be less than or equal to 4",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			validator := tc.validator.Validate()
			if tc.expectedError != "" {
				assert.Equal(t, tc.expectedError, validator.Error().Error())
			} else {
				assert.Nil(t, validator.Error())
			}
		})
	}
}

func TestNotNilRule(t *testing.T) {
	testCases := []struct {
		name          string
		validator     *validation.Validator
		expectedError string
	}{
		{
			name: "NotNil rule passes",
			validator: validation.NewValidator().
				Add("stringVar1", newString("not nil"), validation.NotNil()),
		},
		{
			name: "NotNil rule fails",
			validator: validation.NewValidator().
				Add("stringVar1", nil, validation.NotNil()),
			expectedError: "stringVar1: value cannot be nil",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			validator := tc.validator.Validate()
			if tc.expectedError != "" {
				assert.Equal(t, tc.expectedError, validator.Error().Error())
			} else {
				assert.Nil(t, validator.Error())
			}
		})
	}
}

func TestIsUUIDRule(t *testing.T) {
	testCases := []struct {
		name          string
		validator     *validation.Validator
		expectedError string
	}{
		{
			name: "IsUUID rule passes",
			validator: validation.NewValidator().
				Add("stringVar1", newString("123e4567-e89b-12d3-a456-426614174000"), validation.IsUUID()),
		},
		{
			name: "IsUUID rule fails",
			validator: validation.NewValidator().
				Add("stringVar1", newString("invalid uuid"), validation.IsUUID()),
			expectedError: "stringVar1: value is not a valid UUID",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			validator := tc.validator.Validate()
			if tc.expectedError != "" {
				assert.Equal(t, tc.expectedError, validator.Error().Error())
			} else {
				assert.Nil(t, validator.Error())
			}
		})
	}
}

func newInt(i int) *int {
	return &i
}

func newFloat(f float64) *float64 {
	return &f
}

func newString(s string) *string {
	return &s
}

func newInt64(i int64) *int64 {
	return &i
}

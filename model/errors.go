package model

import "fmt"

// バリデーションエラー
type ValidationError struct {
	parameterName string
	cause         string
}

// バリデーションエラーを作成
func NewValidationError(parameterName string, cause string) *ValidationError {
	return &ValidationError{
		parameterName: parameterName,
		cause:         cause,
	}
}

func (v ValidationError) Error() string {
	return fmt.Sprintf("%s is invalid. caused by %s", v.parameterName, v.cause)
}

type ValidationErrors []*ValidationError

func (v ValidationErrors) Error() string {
	result := ""
	for _, valiadtionError := range v {
		result = result + fmt.Sprintf("%s,", valiadtionError.Error())
	}
	return result
}

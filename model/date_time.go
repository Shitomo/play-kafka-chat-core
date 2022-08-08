package model

import (
	"fmt"
	"log"
	"time"
)

const (
	Location       = "Asia/Tokyo"
	DateTimeFormat = "2006-01-02 15:04:05"
)

var location *time.Location

func LoadLocation() {
	var err error
	location, err = time.LoadLocation(Location)
	if err != nil {
		log.Fatalf("error loading location %v", err)
	}
}

// 日付
type Datetime time.Time

// 文字列から日付を作成
func NewDatetime(datetimeStr string) (Datetime, *ValidationError) {
	t, err := time.ParseInLocation("2006-01-02 15:04:05", datetimeStr, location)
	if err != nil {
		return Datetime{}, NewValidationError("datetime", fmt.Sprintf("invalid format %s", datetimeStr))
	}
	return Datetime(t), nil
}

func Now() Datetime {
	return Datetime(time.Now())
}

func (d Datetime) Format(format string) string {
	return time.Time(d).Format(format)
}

func (d Datetime) UnixMilli() int64 {
	return time.Time(d).UnixMilli()
}

func (d Datetime) Value() time.Time {
	return time.Time(d)
}

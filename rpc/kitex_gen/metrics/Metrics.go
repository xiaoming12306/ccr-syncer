// Code generated by thriftgo (0.2.12). DO NOT EDIT.

package metrics

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
)

type TUnit int64

const (
	TUnit_UNIT             TUnit = 0
	TUnit_UNIT_PER_SECOND  TUnit = 1
	TUnit_CPU_TICKS        TUnit = 2
	TUnit_BYTES            TUnit = 3
	TUnit_BYTES_PER_SECOND TUnit = 4
	TUnit_TIME_NS          TUnit = 5
	TUnit_DOUBLE_VALUE     TUnit = 6
	TUnit_NONE             TUnit = 7
	TUnit_TIME_MS          TUnit = 8
	TUnit_TIME_S           TUnit = 9
)

func (p TUnit) String() string {
	switch p {
	case TUnit_UNIT:
		return "UNIT"
	case TUnit_UNIT_PER_SECOND:
		return "UNIT_PER_SECOND"
	case TUnit_CPU_TICKS:
		return "CPU_TICKS"
	case TUnit_BYTES:
		return "BYTES"
	case TUnit_BYTES_PER_SECOND:
		return "BYTES_PER_SECOND"
	case TUnit_TIME_NS:
		return "TIME_NS"
	case TUnit_DOUBLE_VALUE:
		return "DOUBLE_VALUE"
	case TUnit_NONE:
		return "NONE"
	case TUnit_TIME_MS:
		return "TIME_MS"
	case TUnit_TIME_S:
		return "TIME_S"
	}
	return "<UNSET>"
}

func TUnitFromString(s string) (TUnit, error) {
	switch s {
	case "UNIT":
		return TUnit_UNIT, nil
	case "UNIT_PER_SECOND":
		return TUnit_UNIT_PER_SECOND, nil
	case "CPU_TICKS":
		return TUnit_CPU_TICKS, nil
	case "BYTES":
		return TUnit_BYTES, nil
	case "BYTES_PER_SECOND":
		return TUnit_BYTES_PER_SECOND, nil
	case "TIME_NS":
		return TUnit_TIME_NS, nil
	case "DOUBLE_VALUE":
		return TUnit_DOUBLE_VALUE, nil
	case "NONE":
		return TUnit_NONE, nil
	case "TIME_MS":
		return TUnit_TIME_MS, nil
	case "TIME_S":
		return TUnit_TIME_S, nil
	}
	return TUnit(0), fmt.Errorf("not a valid TUnit string")
}

func TUnitPtr(v TUnit) *TUnit { return &v }
func (p *TUnit) Scan(value interface{}) (err error) {
	var result sql.NullInt64
	err = result.Scan(value)
	*p = TUnit(result.Int64)
	return
}

func (p *TUnit) Value() (driver.Value, error) {
	if p == nil {
		return nil, nil
	}
	return int64(*p), nil
}

type TMetricKind int64

const (
	TMetricKind_GAUGE     TMetricKind = 0
	TMetricKind_COUNTER   TMetricKind = 1
	TMetricKind_PROPERTY  TMetricKind = 2
	TMetricKind_STATS     TMetricKind = 3
	TMetricKind_SET       TMetricKind = 4
	TMetricKind_HISTOGRAM TMetricKind = 5
)

func (p TMetricKind) String() string {
	switch p {
	case TMetricKind_GAUGE:
		return "GAUGE"
	case TMetricKind_COUNTER:
		return "COUNTER"
	case TMetricKind_PROPERTY:
		return "PROPERTY"
	case TMetricKind_STATS:
		return "STATS"
	case TMetricKind_SET:
		return "SET"
	case TMetricKind_HISTOGRAM:
		return "HISTOGRAM"
	}
	return "<UNSET>"
}

func TMetricKindFromString(s string) (TMetricKind, error) {
	switch s {
	case "GAUGE":
		return TMetricKind_GAUGE, nil
	case "COUNTER":
		return TMetricKind_COUNTER, nil
	case "PROPERTY":
		return TMetricKind_PROPERTY, nil
	case "STATS":
		return TMetricKind_STATS, nil
	case "SET":
		return TMetricKind_SET, nil
	case "HISTOGRAM":
		return TMetricKind_HISTOGRAM, nil
	}
	return TMetricKind(0), fmt.Errorf("not a valid TMetricKind string")
}

func TMetricKindPtr(v TMetricKind) *TMetricKind { return &v }
func (p *TMetricKind) Scan(value interface{}) (err error) {
	var result sql.NullInt64
	err = result.Scan(value)
	*p = TMetricKind(result.Int64)
	return
}

func (p *TMetricKind) Value() (driver.Value, error) {
	if p == nil {
		return nil, nil
	}
	return int64(*p), nil
}

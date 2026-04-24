package raft

import (
	"fmt"
	"io"
	"time"
)

type Marshaler interface {
	MarshalCBOR(w io.Writer) error
}

type Unmarshaler interface {
	UnmarshalCBOR(r io.Reader) error
}

type Er interface {
	Marshaler
	Unmarshaler
}
type TimeBytes struct {
	Bytes []byte `cborgen:"maxlen=16"`
}

type String struct {
	Value string
}

func TimeMarshalCBOR(t *time.Time, w io.Writer) error {
	binary, err := t.MarshalBinary()
	if err != nil {
		return fmt.Errorf("failed to marshal time: %w", err)
	}
	tb := TimeBytes{Bytes: binary}
	err = tb.MarshalCBOR(w)
	if err != nil {
		return fmt.Errorf("failed to marshal time bytes: %w", err)
	}
	return nil
}

func TimeUnmarshalCBOR(t *time.Time, r io.Reader) (err error) {
	var tb TimeBytes
	err = tb.UnmarshalCBOR(r)
	if err != nil {
		return fmt.Errorf("failed to unmarshal time: %w", err)
	}
	err = t.UnmarshalBinary(tb.Bytes)
	if err != nil {
		return fmt.Errorf("failed to unmarshal time: %w", err)
	}
	return nil
}

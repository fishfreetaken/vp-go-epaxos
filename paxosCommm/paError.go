package paxoscommm

import "errors"

type paerror struct {
	err   error
	icode int32
}

func NewPaError(s string, code int32) *paerror {
	var m paerror
	m.err = errors.New(s)
	m.icode = code
	return &m

}

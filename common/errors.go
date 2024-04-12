package common

import "errors"

var ErrKeyDoesNotExist = errors.New("key does not exist")
var ErrValDoesNotExist = errors.New("val does not exist")
var ErrIdxOutOfBounds = errors.New("index out of bounds")
var ErrInvalidRange = errors.New("range is invalid")

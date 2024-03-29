package common

import "errors"

var ErrKeyDoesNotExist = errors.New("this key does not exist")
var ErrIdxOutOfBounds = errors.New("index out of bounds")
var ErrInvalidRange = errors.New("range is invalid")

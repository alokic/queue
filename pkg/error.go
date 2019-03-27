package pkg

//HTTP

//ErrBadRequest is 400.
type ErrBadRequest struct {
	Err error
}

func (e ErrBadRequest) Error() string {
	return e.Err.Error()
}

//ErrNotFound is 404.
type ErrNotFound struct {
	Err error
}

func (e ErrNotFound) Error() string {
	return e.Err.Error()
}

//ErrInternalServerError is 500.
type ErrInternalServerError struct {
	Err error
}

func (e ErrInternalServerError) Error() string {
	return e.Err.Error()
}

//ErrMethodNotImplemented is not implemneted.
type ErrMethodNotImplemented struct {
	Err error
}

func (e ErrMethodNotImplemented) Error() string {
	return e.Err.Error()
}

//ErrUserEndpointNotFound is not found.
type ErrUserEndpointNotFound struct {
	Err error
}

func (e ErrUserEndpointNotFound) Error() string {
	return e.Err.Error()
}

//ErrServiceUnavailable is 503.
type ErrServiceUnavailable struct {
	Err error
}

func (e ErrServiceUnavailable) Error() string {
	return e.Err.Error()
}

//APP

//ErrTimedout is timeout.
type ErrTimedout struct {
	Err error
}

func (e ErrTimedout) Error() string {
	return e.Err.Error()
}

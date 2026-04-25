package gosocket

// recoverMiddleware is the concrete type returned by Recover(); detected via type assertion (no reflect).
type recoverMiddleware struct{}

func (recoverMiddleware) RunMiddleware(*Context) error { return nil }

func isRecoverMiddleware(mw Middleware) bool {
	if mw == nil {
		return false
	}
	_, ok := mw.(recoverMiddleware)
	return ok
}

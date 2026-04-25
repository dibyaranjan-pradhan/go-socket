package internal

// WireEvent is the application event name on an EmitJob (logging and correlation).
type WireEvent string

// OrDash returns the event string for logs, or "-" when empty.
func (e WireEvent) OrDash() string {
	if e == "" {
		return "-"
	}
	return string(e)
}

// WireRoom is the room identifier on an EmitJob (fan-out key and logging).
type WireRoom string

// OrDash returns the room id for logs, or "-" when empty.
func (r WireRoom) OrDash() string {
	if r == "" {
		return "-"
	}
	return string(r)
}

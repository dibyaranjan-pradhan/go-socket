package engineio

import "bytes"

// ProbePayload is the Engine.IO upgrade probe string exchanged before transport switch.
const ProbePayload = "probe"

// IsProbe reports whether packet data is the upgrade probe payload.
func IsProbe(data []byte) bool {
	return bytes.Equal(data, []byte(ProbePayload))
}

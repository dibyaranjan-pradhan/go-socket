package engineio_test

import (
	"fmt"

	"github.com/dibyaranjan-pradhan/go-socket/internal/engineio"
)

// ExampleEncode shows how a Message packet becomes wire bytes.
func ExampleEncode() {
	wire, _ := engineio.Encode(engineio.Packet{
		Type: engineio.Message,
		Data: []byte("hello"),
	})
	fmt.Printf("%s\n", wire)
	// Output: 4hello
}

// ExampleDecode shows how to read a packet back off the wire.
func ExampleDecode() {
	pkt, _ := engineio.Decode([]byte("2probe"))
	fmt.Printf("type=%s data=%s\n", pkt.Type, pkt.Data)
	// Output: type=ping data=probe
}

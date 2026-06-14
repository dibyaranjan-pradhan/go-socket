package socketio_test

import (
	"fmt"

	"github.com/dibyaranjan-pradhan/go-socket/internal/socketio"
)

// ExampleEncode builds an EVENT on the default namespace that asks for an ack.
func ExampleEncode() {
	ack := 7
	wire, _ := socketio.Encode(socketio.Packet{
		Type:      socketio.Event,
		Namespace: "/",
		AckID:     &ack,
		Data:      []byte(`["chat","hi"]`),
	})
	fmt.Printf("%s\n", wire)
	// Output: 27["chat","hi"]
}

// ExampleDecode reads a namespaced EVENT back off the wire.
func ExampleDecode() {
	pkt, _ := socketio.Decode([]byte(`2/admin,["kick","bob"]`))
	fmt.Printf("type=%s ns=%s data=%s\n", pkt.Type, pkt.Namespace, pkt.Data)
	// Output: type=event ns=/admin data=["kick","bob"]
}

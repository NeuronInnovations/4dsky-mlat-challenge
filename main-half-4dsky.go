// main2.go
// This file demonstrates how to parse the structured byte stream from sellers
// and extract raw ModeS messages, timestamps, and sensor positions.
//
// INSTRUCTIONS: To use this file, you must comment out or rename main.go first.
// Go does not allow two main() functions in the same package.
//
// After commenting out main.go, run:
//
//	go run main.go --port=6653 --mode=peer --buyer-or-seller=buyer --list-of-sellers-source=env --envFile=.buyer-env
package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"math"
	"time"

	neuronsdk "github.com/NeuronInnovations/neuron-go-hedera-sdk" // Import neuronFactory from neuron-go-sdk
	commonlib "github.com/NeuronInnovations/neuron-go-hedera-sdk/common-lib"
	keylib "github.com/NeuronInnovations/neuron-go-hedera-sdk/keylib"

	"github.com/hashgraph/hedera-sdk-go/v2"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

const (
	sensorIDSize             = 8 // int64 is 8 bytes
	sensorLongitudeSize      = 8 // float64 is 8 bytes
	sensorLatitudeSize       = 8 // float64 is 8 bytes
	sensorAltitudeSize       = 8 // float64 is 8 bytes
	secondsSinceMidnightSize = 8 // uint64 is 8 bytes
	nanosecondsSize          = 8 // uint64 is 8 bytes
	minFixedSize             = sensorIDSize + sensorLatitudeSize + sensorLongitudeSize + sensorAltitudeSize + secondsSinceMidnightSize + nanosecondsSize
)

// A helper to read exactly len(buf) bytes from the stream
func readExact(s network.Stream, buf []byte) error {
	var total int
	for total < len(buf) {
		n, err := s.Read(buf[total:])
		if err != nil {
			return err
		}
		if n == 0 {
			return io.EOF
		}
		total += n
	}
	return nil
}

func float64FromByte(bytes []byte) float64 {
	bits := binary.BigEndian.Uint64(bytes)
	return math.Float64frombits(bits)
}

func int64FromByte(bytes []byte) int64 {
	bits := binary.BigEndian.Uint64(bytes)
	return int64(bits)
}

func uint64FromByte(bytes []byte) uint64 {
	return binary.BigEndian.Uint64(bytes)
}

// SensorLocation represents a sensor's location
type SensorLocation struct {
	Lat float64
	Lon float64
	Alt float64
}

// locationOverridesData contains public keys and their corresponding locations
var locationOverridesData = []struct {
	PubKey string
	Loc    SensorLocation
}{
	{"027c4a53123b00b54cd004c684a6f93e02fc423d080dc065c2d2efa9b94b660826", SensorLocation{50.102509, -5.68199, 192.8}},
	{"0237ec14e56be526c011c6e6f3c23afead31fceabe2302822985a3c187fc6b72b8", SensorLocation{49.95967, -6.28422, 67.0}},
	{"025d2f109933ce63bb998bc3cf41549e1557fcd1937c5349a9f6f586226ad72fc3", SensorLocation{49.94661, -6.33147, 52.4}},
	{"02e2b54578b8861b5e57aee78bd2452a00d73d30d7706b35856af8b5d56f74d1df", SensorLocation{49.89252, -6.34473, 74.7}},
	{"0359507a76239e5b7db5bd20b900225d72cdc6be4e11599e3e0b97c5c815a75d1a", SensorLocation{49.91428, -6.29487, 91.4}},
	{"02388bdffad38859b6892baaa511a174fcb1dd84f3cb74cc8feb088e004ce5ddf4", SensorLocation{50.10246, -5.66815, 185.8}},
	{"03f34b08f08471eb8049739d2e9c7fa09326e871a648d0b067bc5f517046dd4f15", SensorLocation{50.15498, -5.65337, 180.8}},
	{"021a29e755951b0f0267c0357e8d01489ae41abbe54818e4975b33ca5553b7a902", SensorLocation{50.09917, -5.55674, 146.4}},
	{"022b58a706abfd29d12011f7e08ad037aac80a9c3651ccd5d44679cfb6bfcd409a", SensorLocation{50.20571, -5.49855, 149.4}},
}

// initLocationOverrides converts the hardcoded data to a map of peer ID to location
func initLocationOverrides() map[peer.ID]SensorLocation {
	overrides := make(map[peer.ID]SensorLocation)
	for _, data := range locationOverridesData {
		peerIDStr := keylib.ConvertHederaPublicKeyToPeerID(data.PubKey)
		peerID, err := peer.Decode(peerIDStr)
		if err != nil {
			log.Printf("Warning: failed to decode peer ID %s: %v", peerIDStr, err)
			continue
		}
		overrides[peerID] = data.Loc
	}
	return overrides
}

func main() {
	//"neuron/ADSB/0.0.2"
	var NrnProtocol = protocol.ID("neuron/ADSB/0.0.2")

	// Initialize location overrides
	locationOverrides := initLocationOverrides()
	log.Printf("Initialized %d location overrides", len(locationOverrides))

	neuronsdk.LaunchSDK(
		"0.1",       // Specify your app's version
		NrnProtocol, // Specify a protocol ID
		nil,         // leave nil if you don't need custom key configuration logic
		func(ctx context.Context, h host.Host, b *commonlib.NodeBuffers) { // Define buyer case logic here (if required)
			h.SetStreamHandler(NrnProtocol, func(streamHandler network.Stream) {
				defer streamHandler.Close()
				// I am receiving data from the following peer
				peerID := streamHandler.Conn().RemotePeer()
				b.SetStreamHandler(peerID, &streamHandler)

				fmt.Printf("Stream established with peer %s\n", peerID)

				for {
					isStreamClosed := network.Stream.Conn(streamHandler).IsClosed()
					if isStreamClosed {
						log.Println("Stream seems to be closed ...", peerID)
						break
					}

					// Set a read deadline to avoid blocking indefinitely
					streamHandler.SetReadDeadline(time.Now().Add(5 * time.Second))

					// 1) Read the first byte that tells total message length
					lengthBuf := make([]byte, 1)
					if err := readExact(streamHandler, lengthBuf); err != nil {
						if err != io.EOF {
							log.Printf("Error reading length byte: %v", err)
						}
						break
					}

					totalPacketSize := int(lengthBuf[0])
					if totalPacketSize == 0 {
						log.Println("Got a 0-length payload; ignoring")
						continue
					}

					// 2) Read the rest of the packet
					packetBytes := make([]byte, totalPacketSize)
					if err := readExact(streamHandler, packetBytes); err != nil {
						if err != io.EOF {
							log.Printf("Error reading packet: %v", err)
						}
						break
					}

					// Check if packet is large enough
					if len(packetBytes) < minFixedSize {
						fmt.Println("Packet too short, ignoring")
						continue
					}

					offset := 0

					// (1) sensorID (8 bytes => int64)
					sensorID := int64FromByte(packetBytes[offset : offset+sensorIDSize])
					offset += sensorIDSize

					// (2) latitude (8 bytes => float64)
					sensorLatitude := float64FromByte(packetBytes[offset : offset+sensorLatitudeSize])
					offset += sensorLatitudeSize

					// (3) longitude (8 bytes => float64)
					sensorLongitude := float64FromByte(packetBytes[offset : offset+sensorLongitudeSize])
					offset += sensorLongitudeSize

					// (4) altitude (8 bytes => float64)
					sensorAltitude := float64FromByte(packetBytes[offset : offset+sensorAltitudeSize])
					offset += sensorAltitudeSize

					// (5) secondsSinceMidnight (8 bytes => uint64)
					secondsSinceMidnight := uint64FromByte(packetBytes[offset : offset+secondsSinceMidnightSize])
					offset += secondsSinceMidnightSize

					// (6) nanoseconds (8 bytes => uint64)
					nanoseconds := uint64FromByte(packetBytes[offset : offset+nanosecondsSize])
					offset += nanosecondsSize

					// (7) rawModeS (remaining bytes)
					rawModeS := packetBytes[offset:]

					// Apply location override if available
					if override, exists := locationOverrides[peerID]; exists {
						sensorLatitude = override.Lat
						sensorLongitude = override.Lon
						sensorAltitude = override.Alt
						fmt.Printf("*** Applied location override for peer %s ***\n", peerID)
					}

					// Print the parsed information
					fmt.Printf("=== ModeS Message from Peer %s ===\n", peerID)
					fmt.Printf("Sensor ID: %d\n", sensorID)
					fmt.Printf("Sensor Position: Lat=%.6f, Lon=%.6f, Alt=%.2f\n", sensorLatitude, sensorLongitude, sensorAltitude)
					fmt.Printf("Timestamp: SecondsSinceMidnight=%d, Nanoseconds=%d\n", secondsSinceMidnight, nanoseconds)
					fmt.Printf("Raw ModeS (hex): %x\n", rawModeS)
					fmt.Printf("Raw ModeS (bytes): %v\n", rawModeS)
					fmt.Printf("Raw ModeS length: %d bytes\n", len(rawModeS))
					fmt.Println("---")
				}
			})

		},
		func(msg hedera.TopicMessage) { // Define buyer topic callback logic here (if required)
			fmt.Println(msg)
		},
		func(ctx context.Context, h host.Host, b *commonlib.NodeBuffers) { // Define seller case logic here (if required)
			// every 10 seconds, send a ping message

		},
		func(msg hedera.TopicMessage) {
			// Define seller topic callback logic here (if required)
		},
	)
}

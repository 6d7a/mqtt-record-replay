//
// mqtt-replay.go - tools for recording from and playing back to MQTT topics.
//
//
// License:
//   Copyright (c) 2018 yoggy <yoggy0@gmail.com>
//   Copyright (c) 2021 Bendix Buchheister <buchheister@consider-it.de>
//   Copyright (c) 2022 Jannik Beyerstedt <beyerstedt@consider-it.de>
//   Released under the MIT license
//   http://opensource.org/licenses/mit-license.php;
//
package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	msgpack "github.com/vmihailenco/msgpack/v5"
	"golang.org/x/term"
)

const buildVersion string = "v2.0.0-alpha"

// configuration values
var verbosity int
var brokerURL string
var filename string
var startTimeSec uint
var endTimeSec uint // end time of 0 seconds doesn't make sense, so use it for "full file"

func init() {
	flag.IntVar(&verbosity, "v", 1, "verbosity level: off (0), info (1), debug (2)")

	flag.StringVar(&brokerURL, "b", "tcp://localhost:1883", "MQTT broker URL")
	flag.StringVar(&filename, "i", "", "Input file")
	flag.UintVar(&startTimeSec, "s", 0, "Starting time offset (seconds)")
	flag.UintVar(&endTimeSec, "e", 0, "End time (seconds, leave out for full file)")
	flag.Parse()
}

func nowMillis() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}

type MqttMessage struct {
	Millis  int64
	Topic   string
	Payload []byte
}

func readEntry(file *os.File) (MqttMessage, int64) {
	// read payload size entry
	buf := make([]byte, binary.MaxVarintLen64)
	_, err := file.Read(buf)
	if err != nil {
		return MqttMessage{}, -1 // EOF reached
	}
	payload_size, _ := binary.Varint(buf)

	// read payload buffer
	payload_buf := make([]byte, payload_size)
	_, err = file.Read(payload_buf)
	if err != nil {
		return MqttMessage{}, -1 // EOF reached
	}

	// unpack message
	var msg MqttMessage
	err = msgpack.Unmarshal(payload_buf, &msg)
	if err != nil {
		log.Fatalln("Fatal error unpacking packet in recording file")
	}

	return msg, payload_size
}

func publish(client mqtt.Client, msg MqttMessage) {
	token := client.Publish(msg.Topic, byte(0), false, msg.Payload)
	token.Wait()
}

type Playback struct {
	File   *os.File
	Client mqtt.Client

	// internal playback state
	endTimeAvailable   bool
	lastStartTimeSec   uint
	recordingStartTime int64

	firstMsgMillis    int64
	firstMsgWallclock int64
	haltOffsetMillis  int64

	haltStartWallclock int64
}

func (p *Playback) Init(startTimeSec uint, endTimeSec uint) {
	p.lastStartTimeSec = startTimeSec
	p.endTimeAvailable = endTimeSec > 0

	// get first entry in recording file
	msg, len := readEntry(p.File)
	p.recordingStartTime = msg.Millis // timestamp of first entry in file

	// fast forward to message at requested start time
	for {
		msgMillisRelative := msg.Millis - p.recordingStartTime
		if msgMillisRelative >= int64(startTimeSec*1000) {
			log.Printf("t=%6.2f s, %6d bytes, topic=%s\n", float32(msgMillisRelative)/1000.0, len, msg.Topic)
			publish(p.Client, msg)

			p.firstMsgMillis = msg.Millis
			p.firstMsgWallclock = nowMillis()

			break
		}

		msg, len = readEntry(p.File) // not at start time yet, skip to next message
	}
}

func (p *Playback) PlayNextMessage() bool {
	msg, len := readEntry(p.File)
	if len < 0 {
		log.Println("End of recording reached")
		return false
	}

	msgMillisRelative := msg.Millis - p.recordingStartTime

	// check requested end time
	if p.endTimeAvailable && msgMillisRelative > int64(endTimeSec*1000) {
		log.Println("Requested end time reached")
		return false
	}

	// wait for target time to be reached
	targetWallclock := p.firstMsgWallclock + (msg.Millis - p.firstMsgMillis) + p.haltOffsetMillis
	for {
		if nowMillis() >= targetWallclock {
			log.Printf("t=%6.2f s, %6d bytes, topic=%s\n", float32(msgMillisRelative)/1000.0, len, msg.Topic)
			publish(p.Client, msg)
			break
		}

		time.Sleep(200 * time.Microsecond)
	}

	return true // still messages left
}

func (p *Playback) Halt() {
	p.haltStartWallclock = nowMillis()
}

func (p *Playback) Restart() {
	p.haltOffsetMillis = nowMillis() - p.haltStartWallclock
}

func main() {
	fmt.Println("MQTT Recording Replay " + buildVersion)
	fmt.Println("- MQTT broker:     ", brokerURL)
	fmt.Println("- Input filename:  ", filename)
	if endTimeSec > 0 {
		fmt.Println("- Interval:        ", startTimeSec, "-", endTimeSec, "sec.")
	} else if startTimeSec > 0 {
		fmt.Println("- Start time:      ", startTimeSec, "sec.")
	}
	fmt.Println("")

	if verbosity < 1 {
		log.SetFlags(0)
		log.SetOutput(ioutil.Discard)
	}

	// try opening file for reading
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalln("Error opening file for reading:", err)
	}
	defer file.Close()

	// try connecting to MQTT broker
	opts := mqtt.NewClientOptions()
	opts.AddBroker(brokerURL)

	client := mqtt.NewClient(opts)
	defer client.Disconnect(100)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		log.Fatalln("Error connecting to MQTT broker:", token.Error())
	}
	if verbosity > 1 {
		log.Println("Success connecting to MQTT broker")
	}

	// switch stdin into "raw" mode to get key presses without need for a newline
	oldState, err := term.MakeRaw(0)
	if err != nil {
		log.Fatalln("Error changing terminal input mode:", err)
	}
	defer term.Restore(0, oldState)

	//
	// process recording file
	//
	var playControl Playback
	playControl.File = file
	playControl.Client = client

	playControl.Init(startTimeSec, endTimeSec)

	messagesLeft := true
	for messagesLeft {
		messagesLeft = playControl.PlayNextMessage()

		bytes := make([]byte, 3)
		numRead, err := os.Stdin.Read(bytes)
		const ETX = '\x03' // ^C
		const EOT = '\x04' // ^D
		if err != nil {
			log.Println("Error reading form terminal:", err)
			break // don't just fatal exit here to let term.Restore be run
		}
		if bytes[0] == ETX || bytes[0] == EOT {
			log.Println("Ctrl-C/ Ctrl-D detected exiting")
			break
		}
		if numRead == 3 && bytes[0] == 27 && bytes[1] == 91 {
			// Three-character control sequence, beginning with "ESC-[".

			// Since there are no ASCII codes for arrow keys, we use
			// Javascript key codes.
			if bytes[2] == 65 {
				// Up
				fmt.Println("TODO: Up")
			} else if bytes[2] == 66 {
				// Down
				fmt.Println("TODO: Down")
			} else if bytes[2] == 67 {
				// Right
				fmt.Println("TODO: Right")
			} else if bytes[2] == 68 {
				// Left
				fmt.Println("TODO: Left")
			}
		} else if numRead == 1 {
			fmt.Printf("TODO: Letter %q %d", string(bytes[0]), int(bytes[0]))
		} else {
			fmt.Println("TODO: two chars")
			// Two characters read??
		}
	}

	log.Println("Replay finished")
}

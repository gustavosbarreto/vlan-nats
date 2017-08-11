package main

import (
	"flag"
	"fmt"
	"log"
	"net"

	MQTT "github.com/eclipse/paho.mqtt.golang"
	nats "github.com/nats-io/go-nats"
	"github.com/songgao/packets/ethernet"
	"github.com/songgao/water"
	"github.com/songgao/water/waterutil"
	"github.com/vishvananda/netlink"
)

var (
	natsURL = flag.String("n", nats.DefaultURL, "the NATS URL to connect to")
	vlanID  = flag.Uint("v", 0, "a VLAN ID (default 0)")
)

func main() {
	// parse command-line args
	flag.Parse()

	opts := MQTT.NewClientOptions()
	opts.AddBroker(*natsURL)

	mc := MQTT.NewClient(opts)
	// connect to mqtt
	token := mc.Connect()
	token.Wait()

	if token.Error() != nil {
		log.Fatal(token.Error())
	}
	//	defer token.Close()

	// create a TAP interface
	config := water.Config{
		DeviceType: water.TAP,
	}
	config.Name = fmt.Sprintf("vnats%d", *vlanID)
	ifce, err := water.New(config)
	if err != nil {
		log.Fatal(err)
	}

	// get ethernet address of the interface we just created
	var ownEth net.HardwareAddr
	nifces, err := net.Interfaces()
	if err != nil {
		log.Fatal(err)
	}
	for _, nifce := range nifces {
		if nifce.Name == config.Name {
			ownEth = nifce.HardwareAddr
			break
		}
	}
	if len(ownEth) == 0 {
		log.Fatal("failed to get own ethernet address")
	}

	tap, err := netlink.LinkByName(config.Name)
	if err != nil {
		log.Fatal(err)
	}

	addr, err := netlink.ParseAddr(fmt.Sprintf("10.1.0.%d/16", 11))
	if err != nil {
		log.Fatal(err)
	}

	// set IP address for TAP interface
	if err = netlink.AddrAdd(tap, addr); err != nil {
		log.Fatal(err)
	}

	// enables TAP interface
	if err = netlink.LinkSetUp(tap); err != nil {
		log.Fatal(err)
	}

	// sub to our frame address
	subTopic := fmt.Sprintf("vlan.%d.%x", *vlanID, ownEth)
	sub := mc.Subscribe(subTopic, 0, func(c MQTT.Client, m MQTT.Message) {
		if _, err := ifce.Write(m.Payload()); err != nil {
			log.Print(err)
		}
	})

	sub.Wait()
	if sub.Error() != nil {
		log.Fatal(sub.Error())
	}
	//	defer sub.Unsubscribe()

	// sub to broadcasts
	broadcastTopic := fmt.Sprintf("vlan.%d", *vlanID)
	bsub := mc.Subscribe(broadcastTopic, 0, func(c MQTT.Client, m MQTT.Message) {
		if _, err := ifce.Write(m.Payload()); err != nil {
			log.Print(err)
		}
	})

	bsub.Wait()
	if bsub.Error() != nil {
		log.Fatal(bsub.Error())
	}
	//	defer bsub.Unsubscribe()

	// read each frame and publish it to appropriate topic
	var frame ethernet.Frame
	for {
		frame.Resize(1500)

		// read frame from interface
		n, err := ifce.Read(frame)
		if err != nil {
			log.Fatal(err)
		}
		frame := frame[:n]

		// the topic to publish to
		dst := waterutil.MACDestination(frame)
		var pubTopic string
		if waterutil.IsBroadcast(dst) {
			pubTopic = broadcastTopic

		} else {
			pubTopic = fmt.Sprintf("vlan.%d.%x", *vlanID, dst)
		}

		// publish
		token := mc.Publish(pubTopic, 0, false, []byte(frame))
		token.Wait()
		if token.Error() != nil {
			log.Fatal(token.Error())
		}
	}
}

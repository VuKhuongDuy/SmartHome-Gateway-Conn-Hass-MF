package main

import (
	"fmt"
	"log"
	"time"

	nats "github.com/nats-io/nats.go"
)

var (
	nc               *nats.Conn
	messages         chan []byte
	sub              *nats.Subscription
	agentNatChannel  string
	exportNatChannel string
)

func init() {
	var err error
    
    file := mainflux.Env(envConfigFile, defConfigFile)
    if err := bootstrap.Bootstrap(bsConfig, logger, file); err != nil {
		return c, errors.Wrap(errFetchingBootstrapFailed, err)
	}

	if bsc, err = agent.ReadConfig(file); err != nil {
		return c, errors.Wrap(errFailedToReadConfig, err)
	}

	nc, err = nats.Connect("localhost:4222")
	if err != nil {
		log.Fatalf(fmt.Sprintf("Connect to NAT error: : %s", err))
	}
	messages = make(chan []byte, 100)

	sub, err = nc.SubscribeSync(agentNatChannel)
}

func main() {
	go subscribeNat(agentNatChannel)
	transferToNat()
}

func subscribeNat(chann string) {
	defer sub.Unsubscribe()
    fmt.Println("Begin subscribe NAT at topic " + chann)
	for {
		sub, _ = nc.SubscribeSync(chann)
		m, err := sub.NextMsg(1000 * time.Millisecond)
		if err == nil && m.Data != nil {
			fmt.Println("Message: ", string(m.Data))
			messages <- m.Data
		}
	}
}

func transferToNat() {
	for {
		select {
		case msg := <-messages:
			msgStr := string(msg)
			fmt.Println("Message: " + msgStr)
			publishNat(exportNatChannel, msgStr)
		default:
			fmt.Println("Waitting message nat.")
			time.Sleep(500 * time.Millisecond)
		}
	}
}

func publishNat(chann string, mesg string) {
	nc.Publish(chann, []byte(mesg))
}

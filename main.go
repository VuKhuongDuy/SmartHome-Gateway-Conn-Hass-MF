package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/joho/godotenv"
	nats "github.com/nats-io/nats.go"
)

var (
	nc           *nats.Conn
	messages     chan []byte
	sub          *nats.Subscription
	agentNaChnl  string
	exportNaChnl string
)

type ChanConfig struct {
	Control string `toml:"control"`
	Data    string `toml:"data"`
}

type AgentConf struct {
	Channels ChanConfig `toml:"channels" json:"channels"`
}

type ExportConf struct {
	Routes []Route `json:"routes" toml:"routes" mapstructure:"routes"`
}

type Route struct {
	MqttTopic string `json:"mqtt_topic" toml:"mqtt_topic" mapstructure:"mqtt_topic"`
	NatsTopic string `json:"nats_topic" toml:"nats_topic" mapstructure:"nats_topic"`
	Workers   int    `json:"workers" toml:"workers" mapstructure:"workers"`
}

func init() {
	var err error
	godotenv.Load(".env")
	agentNaChnl = "channels/" + getAgentNatChnl() + "/messages/services/#"
	exportNaChnl = getExportNatChnl()

	nc, err = nats.Connect("localhost:4222")
	if err != nil {
		log.Fatalf(fmt.Sprintf("Connect to NAT error: : %s", err))
	}

	messages = make(chan []byte, 100)
	sub, err = nc.SubscribeSync(agentNaChnl)
	check(err)
}

func main() {
	go subscribeNat(agentNaChnl)
	transferToNat()
}

func subscribeNat(chann string) {
	defer sub.Unsubscribe()
	fmt.Println("Begin subscribe NAT at topic " + chann)
	for {
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
			publishNat(exportNaChnl, msgStr)
		default:
			fmt.Println("Waitting message nat.")
			time.Sleep(500 * time.Millisecond)
		}
	}
}

func publishNat(chann string, mesg string) {
	nc.Publish(chann, []byte(mesg))
}

func getAgentNatChnl() string {
	file := os.Getenv("MF_AGENT_CONFIG_FILE")
	dat, err := ioutil.ReadFile(file)
	check(err)

	var conf AgentConf
	if _, err = toml.Decode(string(dat), &conf); err != nil {
		check(err)
	}
	return conf.Channels.Control
}

func getExportNatChnl() string {
	file := os.Getenv("MF_EXPORT_CONFIG_FILE")
	dat, err := ioutil.ReadFile(file)
	check(err)

	var conf ExportConf
	if _, err = toml.Decode(string(dat), &conf); err != nil {
		check(err)
	}
	fmt.Println(conf.Routes[0].NatsTopic)
	return "conf.Routes"
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

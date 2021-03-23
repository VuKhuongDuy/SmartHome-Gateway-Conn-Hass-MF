package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/joho/godotenv"
	nats "github.com/nats-io/nats.go"
)

var (
	nc            *nats.Conn
	messages      chan *nats.Msg
	sub           *nats.Subscription
	agentNatTopic string
	exportNaChnl  string
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

	nc, err = nats.Connect("localhost:4222")
	if err != nil {
		log.Fatalf(fmt.Sprintf("Connect to NAT error: : %s", err))
	}

	agentNatTopic = "commands.>"

	messages = make(chan *nats.Msg, 100)
	sub, err = nc.SubscribeSync(agentNatTopic)
	check(err)
}

func main() {
	go subscribeNat(agentNatTopic)
	transferToNat()
}

func subscribeNat(chann string) {
	defer sub.Unsubscribe()
	fmt.Println("Begin subscribe NAT at topic: " + chann)
	for {
		m, err := sub.NextMsg(1000 * time.Millisecond)
		if err == nil && m.Data != nil {
			fmt.Println("Message Data: ", m.Data)
			fmt.Println("Message Subject: ", m.Subject)
			fmt.Println("Message Reply: ", m.Reply)

			hassApi, method := getApiHass(m.Subject)
			body := getBody(m.Data)
			sendToHass(hassApi, method)

			messages <- m
		}
	}
}

func sendToHass(api string, method string) {

}

func getApiHass(str string) (string, string) {
	arr := strings.Split(str, ".")
	index := -1
	for i, v := range arr {
		if v == "hass" {
			index = i
		}
	}
	return strings.Join(arr[(index+1):], "/"), arr[index]
}

func getBody(data []byte) {

}

func transferToNat() {
	for {
		select {
		case msg := <-messages:
			arr := strings.Split(msg.Subject, ".")
			newStr := strings.Join(arr[1:], ".")
			publishNat(fmt.Sprintf("export.%s", newStr), msg)
		default:
			time.Sleep(1000 * time.Millisecond)
		}
	}
}

func publishNat(chann string, mesg *nats.Msg) {
	ec, _ := nats.NewEncodedConn(nc, nats.JSON_ENCODER)
	err := ec.Publish(chann, &nats.Msg{
		Data:    []byte("hello"),
		Subject: "/sdfasf/asfas",
	})
	fmt.Println(err)
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

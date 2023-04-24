package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/gocarina/gocsv"
)

type flowFeature struct {
	SrcIp          net.IP `json:"srcIp"`
	DstIp          net.IP `json:"dstIp"`
	SrcPort        uint16 `json:"srcPort`
	DstPort        uint16 `json:"dstPort"`
	TransportLayer uint16 `json:"transportLayer"`
	//上行包到达时间间隔
	UpTimeMean float64 `json:"upTimeMean"`
	UpTimeStd  float64 `json:"upTimeStd"`
	UpTimeMin  float64 `json:"upTimeMin"`
	UpTimeMax  float64 `json:"upTimeMax"`
	//下行包到达时间间隔
	DownTimeMean float64 `json:"downTimeMean"`
	DownTimeStd  float64 `json:"downTimeStd"`
	DownTimeMin  float64 `json:"downTimeMin"`
	DownTimeMax  float64 `json:"downTimeMax"`
	//包到达时间
	TimeMean float64 `json:"timeMean"`
	TimeStd  float64 `json:"timeStd"`
	TimeMin  float64 `json:"timeMin"`
	TimeMax  float64 `json:"timeMax"`

	//上行数据包包长
	UpPacketLenMean float64 `json:"upPacketLenMean"`
	UpPacketLenStd  float64 `json:"upPacketLenStd"`
	UpPacketLenMin  float64 `json:"upPacketLenMin"`
	UpPacketLenMax  float64 `json:"upPacketLenMax"`

	//下行数据包包长
	DownPacketLenMean float64 `json:"downPacketLenMean"`
	DownPacketLenStd  float64 `json:"downPacketLenStd"`
	DownPacketLenMin  float64 `json:"downPacketLenMin"`
	DownPacketLenMax  float64 `json:"downPacketLenMax"`

	//数据包长
	PacketLenMean float64 `json:"packetLenMean"`
	PacketLenStd  float64 `json:"packetLenStd"`
	PacketLenMin  float64 `json:"packetLenMin"`
	PacketLenMax  float64 `json:"packetLenMax"`

	//流持续时间
	Duration float64 `json:"duration"`
	//上行数据包数目
	UpPacketNum int `json:"upPacketNum"`
	//每分钟上行数据包数目
	UpPacketNumMinute float64 `json:"upPacketNumMinute"`
	//下行数据包数目
	DownPacketNum int `json:"downPacketNum"`
	//每分钟下行数据包数目
	DownPacketNumMinute float64 `json:"downPacketNumMinute"`
	//总包数
	PacketNum int `json:"packetNum"`
	//每分钟包数
	PacketNumMinute float64 `json:"packetNumMinute"`
	//下行数据包比上行数据包
	DownUpPacketPercent float64 `json:"downUpPacketPercent"`
	//上行包头占总长度的比例
	UpHeadLenPercent float64 `json:"upHeadLenPercent"`
	//下行包头占总长度的比例
	DownHeadLenPercent float64 `json:"downHeadLenPercent"`
	ExtensionNum       int     `json:"extensionNum"`
	ServerName         string  `json:"serverName"`

	//tcp psh字段数据包占比
	Psh float64 `json:"psh"`
	//tcp urg字段数据包占比
	Urg float64 `json:"urg"`

	FinNum int `json:"finNum"`
	SynNum int `json:"synNum"`
	RstNum int `json:"rstNum"`
	PshNum int `json:"pshNum"`
	AckNum int `json:"ackNum"`
	UrgNum int `json:"urgNum"`

	UpInitWindow   uint16 `json:"upInitWindow"`
	DownInitWindow uint16 `json:"downInitWindow"`
}

type kafkaMessage struct {
	Name    string      `json:"name"`
	Feature flowFeature `json:"feature"`
}

var (
	wg sync.WaitGroup
)

func main() {
	logFile, err := os.OpenFile("./consumer.log", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}
	log.SetOutput(logFile)
	log.SetFlags(log.Llongfile | log.Ldate | log.Ltime)

	featureFile, err := os.OpenFile("feature.csv", os.O_RDWR|os.O_CREATE|os.O_APPEND, os.ModePerm)

	if err != nil {
		log.Fatal(err)
	}

	consumer, err := sarama.NewConsumer([]string{"localhost:9092"}, nil)
	if err != nil {
		log.Fatalln("sarama.NewConsumer Failed: ", err)
	}

	partitionList, err := consumer.Partitions("test")
	if err != nil {
		log.Fatalln("kafka partitonList error: ", err)
	}

	messages := []*kafkaMessage{}

	fmt.Println(len(partitionList))
	for partition := range partitionList {
		cp, err := consumer.ConsumePartition("test", int32(partition), sarama.OffsetNewest)
		if err != nil {
			log.Fatal(err)
		}
		defer cp.AsyncClose()
		fmt.Println("consumer begin")

		wg.Add(1)

		go func(sarama.PartitionConsumer, []*kafkaMessage) {
			defer wg.Done()
			for msg := range cp.Messages() {
				var one kafkaMessage
				err := json.Unmarshal(msg.Value, &one)
				fmt.Println("recive one message")
				if err != nil {
					panic(err)
				}
				messages = append(messages, &one)
			}
			fmt.Println("goroutie done")
		}(cp, messages)
		wg.Wait()
		consumer.Close()
	}

	/*
		configFileH, err := os.OpenFile("./config", os.O_RDONLY, 0666)
		if err != nil {
			log.Fatal(err)
		}
		defer configFileH.Close()

		reader := bufio.NewReader(configFileH)
		for {
			line, _, err := reader.ReadLine()
			if err == io.EOF {
				break
			} else if err != nil {
				log.Fatal(err)
			}

			selectOne := string(line)
		}
	*/
	fmt.Println("csv begin")

	err = gocsv.MarshalFile(&messages, featureFile)
	if err != nil {
		log.Fatal(err)
	}

}

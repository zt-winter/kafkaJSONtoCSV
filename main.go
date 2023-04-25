package main

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"sync"

	"github.com/Shopify/sarama"
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
	defer featureFile.Close()

	head := []string{"SrcIP", "DstIP", "SrcPort", "DstPort", "TransportLayer",
		"UpTimeMean", "UpTimeStd", "UpTimeMin", "UpTimeMax", "DownTimeMean",
		"DownTimeStd", "DownTimeMin", "DownTimeMax", "TimeMean", "TimeStd",
		"TimeMin", "TimeMax", "UpPacketLenMean", "UpPacketLenStd", "UpPacketLenMin",
		"UpPacketLenMax", "DownPacketLenMean", "DownPacketLenStd", "DownPacketLenMin",
		"DownPacketLenMax", "PacketLenMean", "PacketLenStd", "PacketLenMin", "PacketLenMax",
		"Duration", "UpPacketNum", "UpPacketNumMinute", "DownPacketNum", "DownPacketNumMinute",
		"PacketNum", "PacketNumMinute", "DownUpPacketPercent", "UpHeadLenPercent", "DownHeadLenPercent",
		"ExtensionNum", "ServerName"}

	w := csv.NewWriter(featureFile)
	if err := w.Write(head); err != nil {
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

	ch := make(chan []byte)

	fmt.Println(partitionList)
	go func() {
		var wg sync.WaitGroup
		for partition := range partitionList {
			cp, err := consumer.ConsumePartition("test", int32(partition), sarama.OffsetNewest)
			if err != nil {
				log.Fatal(err)
			}
			defer cp.AsyncClose()
			fmt.Println("consumer begin")

			wg.Add(1)

			go func(sarama.PartitionConsumer) {
				defer wg.Done()
				for msg := range cp.Messages() {
					ch <- msg.Value
					/*
						err := json.Unmarshal(msg.Value, &one)
						fmt.Println("recive one message")
						if err != nil {
							panic(err)
						}
					*/
				}
				fmt.Println("goroutie done")
			}(cp)
		}
		wg.Wait()
	}()

	fmt.Println("csv begin")
	for {
		v := <-ch
		if bytes.Equal(v, []byte{'q'}) {
			break
		} else {
			var one kafkaMessage
			err = json.Unmarshal(v, &one)
			if err != nil {
				log.Fatal(err)
			}
			err = w.Write(one.toSlice())
			if err != nil {
				panic(err)
			}
			w.Flush()
		}
	}
}

func (one *kafkaMessage) toSlice() []string {
	ret := make([]string, 0)
	ret = append(ret, one.Feature.SrcIp.String())
	ret = append(ret, one.Feature.DstIp.String())
	ret = append(ret, strconv.FormatUint(uint64(one.Feature.SrcPort), 10))
	ret = append(ret, strconv.FormatUint(uint64(one.Feature.DstPort), 10))
	ret = append(ret, strconv.FormatUint(uint64(one.Feature.TransportLayer), 10))
	ret = append(ret, strconv.FormatFloat(one.Feature.UpTimeMean, 'f', 4, 64))
	ret = append(ret, strconv.FormatFloat(one.Feature.UpTimeStd, 'f', 4, 64))
	ret = append(ret, strconv.FormatFloat(one.Feature.UpTimeMin, 'f', 4, 64))
	ret = append(ret, strconv.FormatFloat(one.Feature.UpTimeMax, 'f', 4, 64))
	ret = append(ret, strconv.FormatFloat(one.Feature.DownTimeMean, 'f', 4, 64))
	ret = append(ret, strconv.FormatFloat(one.Feature.DownTimeStd, 'f', 4, 64))
	ret = append(ret, strconv.FormatFloat(one.Feature.DownTimeMin, 'f', 4, 64))
	ret = append(ret, strconv.FormatFloat(one.Feature.DownTimeMax, 'f', 4, 64))
	ret = append(ret, strconv.FormatFloat(one.Feature.TimeMean, 'f', 4, 64))
	ret = append(ret, strconv.FormatFloat(one.Feature.TimeStd, 'f', 4, 64))
	ret = append(ret, strconv.FormatFloat(one.Feature.TimeMin, 'f', 4, 64))
	ret = append(ret, strconv.FormatFloat(one.Feature.TimeMax, 'f', 4, 64))
	ret = append(ret, strconv.FormatFloat(one.Feature.UpPacketLenMean, 'f', 4, 64))
	ret = append(ret, strconv.FormatFloat(one.Feature.UpPacketLenStd, 'f', 4, 64))
	ret = append(ret, strconv.FormatFloat(one.Feature.UpPacketLenMin, 'f', 4, 64))
	ret = append(ret, strconv.FormatFloat(one.Feature.UpPacketLenMax, 'f', 4, 64))
	ret = append(ret, strconv.FormatFloat(one.Feature.DownPacketLenMean, 'f', 4, 64))
	ret = append(ret, strconv.FormatFloat(one.Feature.DownPacketLenStd, 'f', 4, 64))
	ret = append(ret, strconv.FormatFloat(one.Feature.DownPacketLenMin, 'f', 4, 64))
	ret = append(ret, strconv.FormatFloat(one.Feature.DownPacketLenMax, 'f', 4, 64))
	ret = append(ret, strconv.FormatFloat(one.Feature.PacketLenMean, 'f', 4, 64))
	ret = append(ret, strconv.FormatFloat(one.Feature.PacketLenStd, 'f', 4, 64))
	ret = append(ret, strconv.FormatFloat(one.Feature.PacketLenMin, 'f', 4, 64))
	ret = append(ret, strconv.FormatFloat(one.Feature.PacketLenMax, 'f', 4, 64))
	ret = append(ret, strconv.FormatFloat(one.Feature.Duration, 'f', 4, 64))
	ret = append(ret, strconv.Itoa(one.Feature.UpPacketNum))
	ret = append(ret, strconv.FormatFloat(one.Feature.UpPacketNumMinute, 'f', 4, 64))
	ret = append(ret, strconv.Itoa(one.Feature.DownPacketNum))
	ret = append(ret, strconv.FormatFloat(one.Feature.DownPacketNumMinute, 'f', 4, 64))
	ret = append(ret, strconv.Itoa(one.Feature.PacketNum))
	ret = append(ret, strconv.FormatFloat(one.Feature.PacketNumMinute, 'f', 4, 64))
	ret = append(ret, strconv.FormatFloat(one.Feature.DownUpPacketPercent, 'f', 4, 64))
	ret = append(ret, strconv.FormatFloat(one.Feature.UpHeadLenPercent, 'f', 4, 64))
	ret = append(ret, strconv.FormatFloat(one.Feature.DownHeadLenPercent, 'f', 4, 64))
	ret = append(ret, strconv.Itoa(one.Feature.ExtensionNum))
	ret = append(ret, one.Feature.ServerName)
	return ret
}

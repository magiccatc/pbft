package main

import (
	"crypto/rand"
	"encoding/json"
	"log"
	"math/big"
	"strconv"
	"strings"
	"sync"
	"time"
)

// 客户端消息跟踪结构
type clientMessageTracker struct {
	lock         sync.Mutex
	sendDone     map[int]time.Time      // 消息ID -> 发送完成时间
	receiveCount map[int]int            // 消息ID -> 已收到的回复数量
	doneCh       map[int]chan time.Time // 消息ID -> 回复到达时间
	latencies    map[int]time.Duration  // 消息ID -> 发送完成到回复的时延
	order        []int                  // 发送顺序
}

var clientTracker = clientMessageTracker{
	sendDone:     make(map[int]time.Time),
	receiveCount: make(map[int]int),
	doneCh:       make(map[int]chan time.Time),
	latencies:    make(map[int]time.Duration),
	order:        make([]int, 0),
}

func clientSendMessageAndListen() {
	//开启客户端的本地监听（主要用来接收节点的reply信息）
	go clientWsListen()

	const totalRequests = 20
	timeout := 30 * time.Second

	var data string
	data, _ = GenerateRandomString(1888)
	var sendWg sync.WaitGroup
	for i := 1; i <= totalRequests; i++ {
		data = data + strconv.Itoa(i)
		r := new(Request)
		sendStart := time.Now()
		r.Timestamp = sendStart.UnixNano()
		r.ClientAddr = clientAddr
		msgID := getRandom()
		r.Message.ID = msgID
		//消息内容就是用户的输入
		r.Message.Content = strings.TrimSpace(data)

		done := make(chan time.Time, 1)
		clientTracker.lock.Lock()
		clientTracker.sendDone[msgID] = time.Time{}
		clientTracker.receiveCount[msgID] = 0
		clientTracker.doneCh[msgID] = done
		clientTracker.order = append(clientTracker.order, msgID)
		clientTracker.lock.Unlock()

		br, err := json.Marshal(r)
		if err != nil {
			log.Panic(err)
		}
		content := jointMessage(cRequest, br)
		log.Printf("客户端发送消息，ID: %d, 时间: %s", msgID, sendStart.Format(time.RFC3339Nano))

		// 教学版批处理：客户端只发送Request给主节点
		payload := append([]byte(nil), content...)
		msgIDCopy := msgID
		sendWg.Add(1)
		go func() {
			defer sendWg.Done()
			wsDial(payload, nodeTable["0"])
			sendDone := time.Now()
			clientTracker.lock.Lock()
			clientTracker.sendDone[msgIDCopy] = sendDone
			clientTracker.lock.Unlock()
			log.Printf("客户端消息发送完成（主节点），ID: %d, 时间: %s", msgIDCopy, sendDone.Format(time.RFC3339Nano))
		}()
	}

	sendWg.Wait()
	log.Println("所有消息已发送，等待节点回复...")

	clientTracker.lock.Lock()
	order := append([]int(nil), clientTracker.order...)
	clientTracker.lock.Unlock()
	for _, msgID := range order {
		clientTracker.lock.Lock()
		done := clientTracker.doneCh[msgID]
		clientTracker.lock.Unlock()
		select {
		case recvTime := <-done:
			clientTracker.lock.Lock()
			sendDone := clientTracker.sendDone[msgID]
			clientTracker.lock.Unlock()
			if sendDone.IsZero() {
				log.Printf("消息ID %d: 已收到回复（发送完成时间未知）", msgID)
				continue
			}
			latency := recvTime.Sub(sendDone)
			clientTracker.lock.Lock()
			clientTracker.latencies[msgID] = latency
			clientTracker.lock.Unlock()
			log.Printf("消息ID %d: 发送完成 -> 回复时延: %v", msgID, latency)
		case <-time.After(timeout):
			log.Printf("消息ID %d: 等待回复超时(%v)", msgID, timeout)
		}
	}

	log.Println("\n========== 发送完成到回复时延统计 ==========")
	clientTracker.lock.Lock()
	for _, msgID := range clientTracker.order {
		latency, ok := clientTracker.latencies[msgID]
		if ok {
			log.Printf("消息ID %d: 发送完成 -> 回复时延: %v", msgID, latency)
		} else {
			log.Printf("消息ID %d: 未收到回复", msgID)
		}
	}
	clientTracker.lock.Unlock()
	log.Println("==============================")
}

// 返回一个十位数的随机数，作为msgid
func getRandom() int {
	x := big.NewInt(10000000000)
	for {
		result, err := rand.Int(rand.Reader, x)
		if err != nil {
			log.Panic(err)
		}
		if result.Int64() > 1000000000 {
			return int(result.Int64())
		}
	}
}

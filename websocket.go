package main

import (
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024 * 1024, // 1MB
	WriteBufferSize: 1024 * 1024, // 1MB
	CheckOrigin: func(r *http.Request) bool {
		// 允许所有来源的连接（用于本地测试）
		return true
	},
}

// WebSocket连接管理器
type wsConnectionManager struct {
	lock        sync.RWMutex
	connections map[string]*wsConn // 地址 -> 连接
	dialer      *websocket.Dialer
}

type wsConn struct {
	conn    *websocket.Conn
	writeMu sync.Mutex
}

var wsManager = wsConnectionManager{
	connections: make(map[string]*wsConn),
	dialer: &websocket.Dialer{
		ReadBufferSize:  1024 * 1024, // 1MB
		WriteBufferSize: 1024 * 1024, // 1MB
	},
}

// 客户端使用的WebSocket监听
func clientWsListen() {
	// 为客户端创建独立的HTTP服务器和路由
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Printf("[客户端] WebSocket升级失败: %v", err)
			return
		}
		defer conn.Close()

		// remoteAddr := conn.RemoteAddr().String()
		// log.Printf("[客户端] 收到来自 %s 的WebSocket连接", remoteAddr)

		for {
			_, b, err := conn.ReadMessage()
			if err != nil {
				if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
					log.Printf("[客户端] WebSocket读取错误: %v", err)
				}
				break
			}

			receiveTime := time.Now()
			replyMsg := string(b)
			//log.Printf("[客户端] 从 %s 接收到 %d 字节数据 (类型: %d): %s", remoteAddr, len(b), messageType, replyMsg)

			// 解析消息ID（从回复消息中提取）
			msgIDStr := extractMsgID(replyMsg)
			if msgIDStr != "" {
				msgID, err := strconv.Atoi(msgIDStr)
				if err == nil {
					var (
						doneCh   chan time.Time
						sendDone time.Time
						count    int
						exists   bool
					)
					clientTracker.lock.Lock()
					if doneCh, exists = clientTracker.doneCh[msgID]; exists {
						clientTracker.receiveCount[msgID]++
						count = clientTracker.receiveCount[msgID]
						sendDone = clientTracker.sendDone[msgID]
					}
					clientTracker.lock.Unlock()

					if exists {
						if count == 1 {
							select {
							case doneCh <- receiveTime:
							default:
							}
						}
						if !sendDone.IsZero() {
							// duration := receiveTime.Sub(sendDone)
							// log.Printf("消息ID %d 已收到回复，发送完成 -> 回复时延: %v", msgID, duration)
						} else {
							log.Printf("消息ID %d 已收到回复（发送完成时间未知）", msgID)
						}
					}
				}
			}
		}
	})

	log.Printf("客户端开启WebSocket监听，地址：%s\n", clientAddr)
	// 将地址从 "127.0.0.1:8888" 转换为HTTP服务器地址
	httpAddr := strings.Replace(clientAddr, "127.0.0.1:", ":", 1)
	log.Printf("[客户端] 监听地址: %s (原始地址: %s)", httpAddr, clientAddr)
	server := &http.Server{
		Addr:    httpAddr,
		Handler: mux,
	}
	if err := server.ListenAndServe(); err != nil {
		log.Printf("[客户端] WebSocket服务器启动失败: %v", err)
		log.Printf("[客户端] 可能原因: 1) 端口 %s 已被占用 2) 权限不足 3) 地址格式错误", httpAddr)
		log.Fatal(err) // 使用 log.Fatal 而不是 log.Panic，更优雅地退出
	}
}

// 从回复消息中提取消息ID
func extractMsgID(replyMsg string) string {
	// 回复消息格式: "X节点已提交msgid:Y"
	prefix := "msgid:"
	idx := strings.Index(replyMsg, prefix)
	if idx == -1 {
		return ""
	}

	// 从"msgid:"之后开始提取数字
	start := idx + len(prefix)
	end := start
	for end < len(replyMsg) && (replyMsg[end] >= '0' && replyMsg[end] <= '9') {
		end++
	}

	if end > start {
		return replyMsg[start:end]
	}
	return ""
}

// 节点使用的WebSocket监听（带就绪通知）
func (p *hotsutff) wsListenWithReady(ready chan bool) {
	// 为每个节点创建独立的HTTP服务器和路由
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Printf("[节点%s] WebSocket升级失败: %v", p.node.nodeID, err)
			return
		}
		defer conn.Close()

		remoteAddr := conn.RemoteAddr().String()
		// log.Fatal("[节点%s] 收到来自 %s 的WebSocket连接", p.node.nodeID, remoteAddr)

		// 保存连接以便后续使用（使用wsManager的锁）
		wsManager.lock.Lock()
		wsManager.connections[remoteAddr] = &wsConn{conn: conn}
		wsManager.lock.Unlock()

		var now time.Time
		firstMessage := true

		for {
			_, b, err := conn.ReadMessage()
			if err != nil {
				if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
					log.Fatal("[节点%s] WebSocket读取错误: %v", p.node.nodeID, err)
				}
				break
			}

			if firstMessage {
				now = time.Now()
				firstMessage = false
			}

			// log.Printf("[节点%s] 从 %s 接收到 %d 字节数据 (类型: %d)", p.node.nodeID, remoteAddr, len(b), messageType)
			p.handleRequest(b, now)
		}

		// 连接关闭时清理
		wsManager.lock.Lock()
		delete(wsManager.connections, remoteAddr)
		wsManager.lock.Unlock()
	})

	log.Printf("节点开启WebSocket监听，地址：%s\n", p.node.addr)
	// 通知监听已就绪
	if ready != nil {
		ready <- true
	}

	// 将地址从 "127.0.0.1:8000" 转换为HTTP服务器地址
	httpAddr := strings.Replace(p.node.addr, "127.0.0.1:", ":", 1)
	// log.Printf("[节点%s] 监听地址: %s (原始地址: %s)", p.node.nodeID, httpAddr, p.node.addr)
	server := &http.Server{
		Addr:    httpAddr,
		Handler: mux,
	}
	if err := server.ListenAndServe(); err != nil {
		log.Printf("[节点%s] WebSocket服务器启动失败: %v", p.node.nodeID, err)
		log.Printf("[节点%s] 可能原因: 1) 端口 %s 已被占用 2) 权限不足 3) 地址格式错误", p.node.nodeID, httpAddr)
		log.Fatal(err) // 使用 log.Fatal 而不是 log.Panic，更优雅地退出
	}
}

// 节点使用的WebSocket监听（兼容旧接口）
func (p *hotsutff) wsListen() {
	p.wsListenWithReady(nil)
}

// 使用WebSocket发送消息
// 支持发送任意大小的消息（包括2000字节及以上）
func wsDial(context []byte, addr string) {
	// 将地址从 "127.0.0.1:8000" 转换为WebSocket URL
	wsURL := "ws://" + addr
	// log.Printf("[WebSocket发送] 尝试连接到 %s, 消息长度: %d 字节", wsURL, len(context))

	var conn *wsConn
	var err error

	// 检查是否已有连接
	wsManager.lock.RLock()
	existingConn, exists := wsManager.connections[addr]
	wsManager.lock.RUnlock()

	if exists && existingConn != nil {
		// 使用现有连接
		conn = existingConn
		// log.Printf("[WebSocket发送] 使用现有连接: -> %s", wsURL)
	} else {
		// 建立新连接
		rawConn, _, err := wsManager.dialer.Dial(wsURL, nil)
		if err != nil {
			log.Printf("[WebSocket发送] 连接失败: %s, 错误: %v", wsURL, err)
			return
		}
		if rawConn == nil {
			log.Printf("[WebSocket发送] 连接返回nil: %s", wsURL)
			return
		}

		// 保存连接
		wsManager.lock.Lock()
		conn = &wsConn{conn: rawConn}
		wsManager.connections[addr] = conn
		wsManager.lock.Unlock()

		// log.Printf("[WebSocket发送] 连接成功: -> %s", wsURL)
	}

	// 检查连接是否有效
	if conn == nil || conn.conn == nil {
		log.Printf("[WebSocket发送] 连接为nil，无法发送消息: %s", wsURL)
		return
	}

	// 发送消息（使用二进制消息类型）
	conn.writeMu.Lock()
	err = conn.conn.WriteMessage(websocket.BinaryMessage, context)
	conn.writeMu.Unlock()
	if err != nil {
		log.Printf("[WebSocket发送] 发送数据到 %s 时出错: %v", wsURL, err)
		// 连接可能已断开，清理
		wsManager.lock.Lock()
		delete(wsManager.connections, addr)
		wsManager.lock.Unlock()
		conn.conn.Close()
		return
	}

	// log.Printf("[WebSocket发送] 消息发送完成: -> %s, 总字节数: %d", wsURL, len(context))
}

package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
)

const nodeCount = 64
const threshold = nodeCount*2/3 + 1

// 客户端的监听地址
var clientAddr = "127.0.0.1:8888"

// 节点池，主要用来存储监听地址
var nodeTable map[string]string

func main() {
	// 动态生成nodeTable，支持任意数量的节点
	nodeTable = make(map[string]string)
	basePorts := []int{8600, 8001, 8002, 8003, 8011, 8012, 8013, 8041, 8042, 8043,
		8021, 8022, 8023, 8031, 8032, 8033, 8014, 8044, 8024, 8034,
		8015, 8045, 8025, 8035, 8016, 8046, 8026, 8036, 8017, 8047,
		8027, 8037, 8018, 8048, 8028, 8038, 8019, 8049, 8029, 8039,
		8010, 8040, 8020, 8030, 8110, 8129, 8185, 8166, 8111, 8130,
		8148, 8167, 8112, 8131, 8149, 8168, 8113, 8132, 8150, 8169,
		8114, 8133, 8151, 8170, 8115, 8134, 8152, 8171, 8116, 8135,
		8153, 8172, 8117, 8136, 8154, 8173, 8118, 8137, 8155, 8174,
		8119, 8138, 8156, 8175, 8120, 8139, 8157, 8176, 8121, 8140,
		8158, 8177, 8122, 8141, 8159, 8178, 8123, 8142, 8160, 8179}

	for i := 0; i < nodeCount; i++ {
		var port int
		if i < len(basePorts) {
			// 使用预定义的端口
			port = basePorts[i]
		} else {
			// 如果节点数超过预定义端口数，使用递增端口
			port = 8000 + i
		}
		nodeTable[strconv.Itoa(i)] = fmt.Sprintf("127.0.0.1:%d", port)
	}

	if len(os.Args) < 2 {
		log.Fatal("用法:\n\n" +
			"生成BLS公私钥:\n" +
			"  程序名 gen <节点数量>  (例如: 程序名 gen 100)\n\n" +
			"启动服务端节点（需要在不同终端分别运行）:\n" +
			"  程序名 0  (启动节点0)\n" +
			"  程序名 1  (启动节点1)\n" +
			"  程序名 2  (启动节点2)\n" +
			"  ...\n\n" +
			"启动客户端:\n" +
			"  程序名 client  (启动客户端)")
	}

	command := os.Args[1]

	// 处理 gen 命令
	if command == "gen" {
		if len(os.Args) != 3 {
			log.Fatal("用法: 程序名 gen <节点数量>\n例如: 程序名 gen 100")
		}
		count, err := strconv.Atoi(os.Args[2])
		if err != nil || count <= 0 {
			log.Fatalf("无效的节点数量: %s，必须是正整数", os.Args[2])
		}
		genBlsKeys(count)
		return
	}

	if command == "client" {
		// 启动客户端
		log.Println("========================================")
		log.Println("启动客户端...")
		log.Println("========================================")
		clientSendMessageAndListen()
	} else if addr, ok := nodeTable[command]; ok {
		// 启动指定的服务端节点
		log.Printf("正在启动服务端节点 %s (地址: %s)...", command, addr)
		p := NewHotStuff(command, addr)
		readyCh := make(chan bool, 1)
		go p.wsListenWithReady(readyCh) //启动节点监听

		// 等待节点监听就绪
		<-readyCh
		log.Printf("已启动服务器%s", command)
		log.Println("服务端节点正在运行，等待消息...")

		// 保持程序运行
		select {}
	} else {
		log.Fatalf("无效的命令或节点ID: %s\n有效命令: gen <数量>, client\n有效节点ID: 0-%d", command, nodeCount-1)
	}
}

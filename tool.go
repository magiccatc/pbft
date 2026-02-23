package main

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"log"
	"sort"
)

// 生成随机字符串
func GenerateRandomString(length int) (string, error) {
	const charset = " !\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~"
	result := make([]byte, length)
	for i := range result {
		// 生成一个安全的随机字节
		b := make([]byte, 1)
		_, err := rand.Read(b)
		if err != nil {
			log.Panicf("error is %v", err)
			return "", err
		}
		// 使用模运算映射到字符集（虽然有轻微偏差，但对一般用途可接受）
		result[i] = charset[int(b[0])%len(charset)]
	}

	return string(result), nil
}

// 向除自己外的其他节点进行广播
func (p *hotsutff) broadcast(cmd command, content []byte) {
	log.Printf("[节点%s] 开始广播消息，命令: %s, 目标节点数: %d", p.node.nodeID, cmd, len(nodeTable)-1)
	for i := range nodeTable {
		if i == p.node.nodeID {
			continue
		}
		message := jointMessage(cmd, content)
		log.Printf("[节点%s] 向节点%s广播消息，地址: %s, 消息长度: %d 字节", p.node.nodeID, i, nodeTable[i], len(message))
		go wsDial(message, nodeTable[i])
	}
	log.Printf("[节点%s] 广播消息完成", p.node.nodeID)
}

const prefixCMDLength = 12

// 默认前十二位为命令名称
func jointMessage(cmd command, content []byte) []byte {
	b := make([]byte, prefixCMDLength)
	for i, v := range []byte(cmd) {
		b[i] = v
	}
	//joint := make([]byte, 0)
	joint := append(b, content...)
	return joint
}

// 默认前十二位为命令名称
func splitMessage(message []byte) (cmd string, content []byte) {
	cmdBytes := message[:prefixCMDLength]
	newCMDBytes := make([]byte, 0)
	for _, v := range cmdBytes {
		if v != byte(0) {
			newCMDBytes = append(newCMDBytes, v)
		}
	}
	cmd = string(newCMDBytes)
	content = message[prefixCMDLength:]
	return
}

// 对消息详情进行摘要
func getDigest(request Request) string {
	b, err := json.Marshal(request)
	if err != nil {
		log.Panic(err)
	}
	hash := sha256.Sum256(b)
	//进行十六进制字符串编码
	return hex.EncodeToString(hash[:])
}

// 计算区块哈希（排除Hash/Justify字段，避免递归）
func computeBlockHash(block Block) string {
	type blockForHash struct {
		ParentHash string
		Height     int
		View       int
		Proposer   string
		CommandIDs []int
	}
	data, err := json.Marshal(blockForHash{
		ParentHash: block.ParentHash,
		Height:     block.Height,
		View:       block.View,
		Proposer:   block.Proposer,
		CommandIDs: block.CommandIDs,
	})
	if err != nil {
		log.Panic(err)
	}
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:])
}

// computeAggQCHash 对AggQC中的QC列表做确定性哈希。
func computeAggQCHash(qcs []QC) string {
	type qcForHash struct {
		View      int
		BlockHash string
		Phase     Phase
		Signers   []string
		AggSig    []byte
	}

	sorted := make([]QC, 0, len(qcs))
	for _, qc := range qcs {
		sorted = append(sorted, qc)
	}
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].View != sorted[j].View {
			return sorted[i].View < sorted[j].View
		}
		if sorted[i].BlockHash != sorted[j].BlockHash {
			return sorted[i].BlockHash < sorted[j].BlockHash
		}
		return sorted[i].Phase < sorted[j].Phase
	})

	items := make([]qcForHash, 0, len(sorted))
	for _, qc := range sorted {
		signers := append([]string(nil), qc.Signers...)
		sort.Strings(signers)
		items = append(items, qcForHash{
			View:      qc.View,
			BlockHash: qc.BlockHash,
			Phase:     qc.Phase,
			Signers:   signers,
			AggSig:    qc.AggSig,
		})
	}

	data, err := json.Marshal(items)
	if err != nil {
		log.Panic(err)
	}
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:])
}

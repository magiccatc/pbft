package main

import (
	"crypto/rand"
	"fmt"
	"log"
	"math/big"
	"os"
	"path/filepath"
	"strconv"

	bls12381 "github.com/kilic/bls12-381"
)

const (
	blsScalarSize = 32
)

var (
	blsCurveOrder = func() *big.Int {
		g1 := bls12381.NewG1()
		return g1.Q()
	}()
	blsDst = []byte("BLS_SIG_BLS12381G2_XMD:SHA-256_SSWU_RO_NUL_")
)

type blsSecretKey struct {
	sk *big.Int
}

type blsPublicKey struct {
	pk *bls12381.PointG1
}

func blsPrivKeyPath(nodeID string) string {
	return filepath.Join("tKeys", nodeID, nodeID+"_bls_sk")
}

func blsPubKeyPath(nodeID string) string {
	return filepath.Join("tKeys", nodeID, nodeID+"_bls_pk")
}

// randomScalar 生成曲线阶上的非零标量。
func randomScalar() *big.Int {
	for {
		buf := make([]byte, blsScalarSize)
		if _, err := rand.Read(buf); err != nil {
			log.Panicf("rand.Read failed: %v", err)
		}
		k := new(big.Int).SetBytes(buf)
		k.Mod(k, blsCurveOrder)
		if k.Sign() != 0 {
			return k
		}
	}
}

func serializeScalar(sk *big.Int) []byte {
	buf := make([]byte, blsScalarSize)
	sk.FillBytes(buf)
	return buf
}

func parseScalar(data []byte) (*big.Int, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("empty scalar")
	}
	k := new(big.Int).SetBytes(data)
	k.Mod(k, blsCurveOrder)
	if k.Sign() == 0 {
		return nil, fmt.Errorf("invalid zero scalar")
	}
	return k, nil
}

// loadBlsSecretKey 从磁盘加载本节点私钥。
func loadBlsSecretKey(nodeID string) *blsSecretKey {
	data, err := os.ReadFile(blsPrivKeyPath(nodeID))
	if err != nil {
		log.Panicf("failed to read BLS secret key for node %s: %v", nodeID, err)
	}
	sk, err := parseScalar(data)
	if err != nil {
		log.Panicf("failed to parse BLS secret key for node %s: %v", nodeID, err)
	}
	return &blsSecretKey{sk: sk}
}

// loadBlsPublicKey 从磁盘加载节点公钥。
func loadBlsPublicKey(nodeID string) (*blsPublicKey, error) {
	data, err := os.ReadFile(blsPubKeyPath(nodeID))
	if err != nil {
		return nil, fmt.Errorf("failed to read BLS public key for node %s: %w", nodeID, err)
	}
	g1 := bls12381.NewG1()
	pk, err := g1.FromCompressed(data)
	if err != nil {
		return nil, fmt.Errorf("failed to parse BLS public key for node %s: %w", nodeID, err)
	}
	return &blsPublicKey{pk: pk}, nil
}

// genBlsKeys 在 tKeys/ 下生成各节点 BLS 密钥对。
func genBlsKeys(count int) {
	log.Printf("开始生成 %d 个节点的BLS公私钥...", count)
	g1 := bls12381.NewG1()
	for i := 0; i < count; i++ {
		nodeID := strconv.Itoa(i)
		dir := filepath.Join("tKeys", nodeID)
		if err := os.MkdirAll(dir, 0755); err != nil {
			log.Fatalf("创建目录失败: %v", err)
		}

		sk := randomScalar()
		pk := g1.New()
		g1.MulScalarBig(pk, g1.One(), sk)

		if err := os.WriteFile(blsPrivKeyPath(nodeID), serializeScalar(sk), 0644); err != nil {
			log.Fatalf("写入私钥失败: %v", err)
		}
		if err := os.WriteFile(blsPubKeyPath(nodeID), g1.ToCompressed(pk), 0644); err != nil {
			log.Fatalf("写入公钥失败: %v", err)
		}

		if (i+1)%10 == 0 || i == count-1 {
			log.Printf("已生成 %d/%d 个节点的BLS密钥", i+1, count)
		}
	}
	log.Printf("成功生成 %d 个节点的BLS密钥！", count)
}

func voteSignMessage(phase Phase, view int, blockHash string) string {
	return fmt.Sprintf("HOTSTUFF|%s|%d|%s", phase, view, blockHash)
}

// hashToG2 使用 BLS DST 将消息映射到 G2。
func hashToG2(message string) (*bls12381.PointG2, error) {
	g2 := bls12381.NewG2()
	return g2.HashToCurve([]byte(message), blsDst)
}

func parseSignature(sigBytes []byte) (*bls12381.PointG2, error) {
	g2 := bls12381.NewG2()
	return g2.FromCompressed(sigBytes)
}

func (p *hotsutff) getPublicKey(nodeID string) *blsPublicKey {
	p.pubKeyMu.RLock()
	if pk, ok := p.pubKeys[nodeID]; ok {
		p.pubKeyMu.RUnlock()
		return pk
	}
	p.pubKeyMu.RUnlock()

	pk, err := loadBlsPublicKey(nodeID)
	if err != nil {
		log.Printf("加载BLS公钥失败: %v", err)
		return nil
	}

	p.pubKeyMu.Lock()
	p.pubKeys[nodeID] = pk
	p.pubKeyMu.Unlock()
	return pk
}

// signVote 使用本地私钥对投票消息签名。
func (p *hotsutff) signVote(phase Phase, view int, blockHash string) []byte {
	if p.node.blsSk == nil {
		return nil
	}
	msg := voteSignMessage(phase, view, blockHash)
	hash, err := hashToG2(msg)
	if err != nil {
		log.Printf("BLS hash失败: %v", err)
		return nil
	}
	g2 := bls12381.NewG2()
	sig := g2.New()
	p.signMu.Lock()
	g2.MulScalarBig(sig, hash, p.node.blsSk.sk)
	p.signMu.Unlock()
	return g2.ToCompressed(sig)
}

// verifyVoteSig 使用公钥验证单个投票签名。
func (p *hotsutff) verifyVoteSig(nodeID string, phase Phase, view int, blockHash string, sigBytes []byte) bool {
	if len(sigBytes) == 0 {
		return false
	}
	pub := p.getPublicKey(nodeID)
	if pub == nil {
		return false
	}
	sig, err := parseSignature(sigBytes)
	if err != nil {
		return false
	}
	msg := voteSignMessage(phase, view, blockHash)
	hash, err := hashToG2(msg)
	if err != nil {
		return false
	}
	engine := bls12381.NewEngine()
	engine.AddPair(pub.pk, hash)
	engine.AddPairInv(engine.G1.One(), sig)
	return engine.Check()
}

// verifyQCSignature 验证 QC 中每条数字签名。
func (p *hotsutff) verifyQCSignature(qc *QC) bool {
	if qc == nil || len(qc.Votes) < threshold {
		return false
	}
	seen := make(map[string]bool)
	for _, vote := range qc.Votes {
		if vote.Phase != qc.Phase || vote.View != qc.View || vote.BlockHash != qc.BlockHash {
			return false
		}
		if seen[vote.NodeID] {
			continue
		}
		seen[vote.NodeID] = true
		if !p.verifyVoteSig(vote.NodeID, vote.Phase, vote.View, vote.BlockHash, vote.Sig) {
			return false
		}
	}
	return len(seen) >= threshold
}

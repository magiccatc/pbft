package main

import (
	"encoding/json"
	"log"
	"strconv"
	"sync"
	"time"
)

var ti int = 0

const (
	pacemakerTickMs = 200
	viewTimeoutMs   = 3000
	maxView         = 10
	batchSize       = 2
	batchTimeoutMs  = 1500
	statsViewStart  = 1
	statsViewEnd    = 9
)

type node struct {
	nodeID string
	addr   string
	blsSk  *blsSecretKey
}

type hotsutff struct {
	node node
	lock sync.Mutex

	view int

	requestPool   map[int]Request
	pendingQueue  []int
	blockRequests map[string][]int
	blocks        map[string]*Block

	highQC   *QC
	lockedQC *QC

	pubKeys  map[string]*blsPublicKey
	pubKeyMu sync.RWMutex
	signMu   sync.Mutex

	lastProgress time.Time
	lastBatch    time.Time

	viewStart map[int]time.Time
	viewDelay map[int]time.Duration

	newViewMessages map[int]map[string]*QC
	proposedView    map[int]bool

	lastCommittedView int
	lastCommittedHash string
	statsStart        time.Time
	statsEnd          time.Time
	statsRequests     int
	statsViews        map[int]bool

	prepareVotes map[string]map[string]Vote
	commitVotes  map[string]map[string]Vote

	prepareBroadcasted map[string]bool
	commitBroadcasted  map[string]bool
	commitReadyType    map[string]string

	votedPrepare map[string]bool
	votedCommit  map[string]bool
	decided      map[string]bool
}

func NewHotStuff(nodeID, addr string) *hotsutff {
	p := new(hotsutff)
	p.node.nodeID = nodeID
	p.node.addr = addr
	p.node.blsSk = loadBlsSecretKey(nodeID)

	p.view = 0
	p.requestPool = make(map[int]Request)
	p.pendingQueue = make([]int, 0)
	p.blockRequests = make(map[string][]int)
	p.blocks = make(map[string]*Block)

	p.prepareVotes = make(map[string]map[string]Vote)
	p.commitVotes = make(map[string]map[string]Vote)
	p.prepareBroadcasted = make(map[string]bool)
	p.commitBroadcasted = make(map[string]bool)
	p.commitReadyType = make(map[string]string)

	p.votedPrepare = make(map[string]bool)
	p.votedCommit = make(map[string]bool)
	p.decided = make(map[string]bool)

	genesis := &Block{Hash: "genesis", Height: 0, View: 0, Proposer: "genesis"}
	p.blocks[genesis.Hash] = genesis
	p.highQC = &QC{View: 0, BlockHash: genesis.Hash, Phase: phaseCommit}
	p.lockedQC = p.highQC
	p.lastCommittedView = 0
	p.lastCommittedHash = genesis.Hash
	p.statsViews = make(map[int]bool)

	p.pubKeys = make(map[string]*blsPublicKey)
	p.lastProgress = time.Now()
	p.lastBatch = time.Now()
	p.viewStart = make(map[int]time.Time)
	p.viewDelay = make(map[int]time.Duration)
	p.newViewMessages = make(map[int]map[string]*QC)
	p.proposedView = make(map[int]bool)

	go p.pacemakerLoop()
	log.Printf("节点%s初始化完成（PBFT）", nodeID)
	return p
}

func (p *hotsutff) leaderID() string { return "0" }
func (p *hotsutff) isLeader() bool   { return p.node.nodeID == p.leaderID() }
func (p *hotsutff) currentView() int { p.lock.Lock(); defer p.lock.Unlock(); return p.view }
func (p *hotsutff) touchProgress()   { p.lock.Lock(); p.lastProgress = time.Now(); p.lock.Unlock() }

func (p *hotsutff) advanceView(target int, reason string) int {
	p.lock.Lock()
	if target > maxView {
		target = maxView
	}
	if target <= p.view {
		current := p.view
		p.lock.Unlock()
		return current
	}
	p.view = target
	p.lastProgress = time.Now()
	p.lock.Unlock()
	log.Printf("[节点%s] 视图推进到 %d (%s)", p.node.nodeID, target, reason)
	return target
}

func (p *hotsutff) pacemakerLoop() {
	ticker := time.NewTicker(time.Duration(pacemakerTickMs) * time.Millisecond)
	defer ticker.Stop()
	timeout := time.Duration(viewTimeoutMs) * time.Millisecond
	batchTimeout := time.Duration(batchTimeoutMs) * time.Millisecond
	for range ticker.C {
		if p.currentView() >= maxView || p.pendingRequestCount() == 0 {
			continue
		}
		if p.isLeader() {
			p.lock.Lock()
			batchElapsed := time.Since(p.lastBatch)
			p.lock.Unlock()
			if batchElapsed >= batchTimeout {
				p.tryProposeFromMempool("batch-timeout", true)
				continue
			}
		}
		p.lock.Lock()
		elapsed := time.Since(p.lastProgress)
		p.lock.Unlock()
		if elapsed < timeout {
			continue
		}
		newView := p.advanceView(p.currentView()+1, "timeout")
		p.sendNewView(newView)
	}
}

func (p *hotsutff) sendNewView(view int) {
	if view > maxView {
		return
	}
	p.lock.Lock()
	qc := p.highQC
	p.lock.Unlock()
	msg := NewViewMsg{View: view, NodeID: p.node.nodeID, HighQC: qc}
	b, _ := json.Marshal(msg)
	go wsDial(jointMessage(cnewView, b), nodeTable[p.leaderID()])
}

func (p *hotsutff) handleNewView(content []byte) {
	if !p.isLeader() {
		return
	}
	msg := new(NewViewMsg)
	if err := json.Unmarshal(content, msg); err != nil {
		log.Panic(err)
	}
	if msg.View > maxView {
		return
	}
	p.lock.Lock()
	if msg.View < p.view {
		p.lock.Unlock()
		return
	}
	if _, ok := p.newViewMessages[msg.View]; !ok {
		p.newViewMessages[msg.View] = make(map[string]*QC)
	}
	p.newViewMessages[msg.View][msg.NodeID] = msg.HighQC
	count := len(p.newViewMessages[msg.View])
	p.lock.Unlock()
	if count < threshold {
		return
	}
	p.advanceView(msg.View, "newview")
	batch := p.nextBatch(true)
	if len(batch) > 0 {
		p.proposeBatchForView(batch, msg.View, "newview")
	}
}

func (p *hotsutff) addRequestToPool(req Request) bool {
	p.lock.Lock()
	defer p.lock.Unlock()
	if _, ok := p.requestPool[req.ID]; ok {
		return false
	}
	p.requestPool[req.ID] = req
	if p.isLeader() {
		p.pendingQueue = append(p.pendingQueue, req.ID)
	}
	return true
}
func (p *hotsutff) pendingRequestCount() int {
	p.lock.Lock()
	defer p.lock.Unlock()
	return len(p.requestPool)
}
func (p *hotsutff) hasRequests(ids []int) bool {
	p.lock.Lock()
	defer p.lock.Unlock()
	for _, id := range ids {
		if _, ok := p.requestPool[id]; !ok {
			return false
		}
	}
	return true
}

func (p *hotsutff) nextBatch(force bool) []int {
	p.lock.Lock()
	defer p.lock.Unlock()
	if len(p.pendingQueue) == 0 || (len(p.pendingQueue) < batchSize && !force) {
		return nil
	}
	size := batchSize
	if len(p.pendingQueue) < batchSize {
		size = len(p.pendingQueue)
	}
	batch := append([]int(nil), p.pendingQueue[:size]...)
	p.pendingQueue = append([]int(nil), p.pendingQueue[size:]...)
	p.lastBatch = time.Now()
	return batch
}

func (p *hotsutff) canProposeNext() bool {
	p.lock.Lock()
	defer p.lock.Unlock()
	return p.view < maxView && p.highQC != nil && p.highQC.View >= p.view
}

func (p *hotsutff) tryProposeFromMempool(reason string, force bool) {
	if !p.isLeader() || !p.canProposeNext() {
		return
	}
	batch := p.nextBatch(force)
	if len(batch) == 0 || p.currentView() >= maxView {
		return
	}
	view := p.advanceView(p.currentView()+1, reason)
	p.proposeBatchForView(batch, view, reason)
}

func (p *hotsutff) proposeBatchForView(batchIDs []int, view int, reason string) {
	if !p.isLeader() || len(batchIDs) == 0 || view > maxView {
		return
	}
	p.lock.Lock()
	if p.proposedView[view] {
		p.lock.Unlock()
		return
	}
	reqs := make([]Request, 0, len(batchIDs))
	for _, id := range batchIDs {
		req, ok := p.requestPool[id]
		if !ok {
			p.lock.Unlock()
			return
		}
		reqs = append(reqs, req)
	}
	p.proposedView[view] = true
	if _, ok := p.viewStart[view]; !ok {
		p.viewStart[view] = time.Now()
	}
	parent := p.highQC.BlockHash
	height := 1
	if parentBlock, ok := p.blocks[parent]; ok {
		height = parentBlock.Height + 1
	}
	block := Block{ParentHash: parent, Height: height, View: view, Proposer: p.node.nodeID, CommandIDs: append([]int(nil), batchIDs...), Justify: p.highQC}
	block.Hash = computeBlockHash(block)
	p.blocks[block.Hash] = &block
	p.blockRequests[block.Hash] = append([]int(nil), batchIDs...)
	p.lock.Unlock()

	prop := Proposal{View: view, Block: block, Requests: reqs}
	b, _ := json.Marshal(prop)
	log.Printf("[节点%s] 广播Pre-prepare(view=%d, reason=%s)，区块哈希: %s", p.node.nodeID, view, reason, block.Hash[:16]+"...")
	p.broadcast(cproposal, b)
	p.touchProgress()
	p.broadcastPrepareVote(block.View, block.Hash)
}

func (p *hotsutff) handleRequest(data []byte, now time.Time) {
	cmd, content := splitMessage(data)
	switch command(cmd) {
	case cRequest:
		p.handleClientRequest(content)
	case cproposal:
		p.handleproposal(content)
	case cnewView:
		p.handleNewView(content)
	case cmsgPrepare:
		p.handlePrepareMsg(content)
	case cmsgCommit:
		p.handleCommitMsg(content, now)
	}
}

func (p *hotsutff) handleClientRequest(content []byte) {
	r := new(Request)
	if err := json.Unmarshal(content, r); err != nil {
		log.Panic(err)
	}
	if p.currentView() >= maxView {
		return
	}
	if added := p.addRequestToPool(*r); !added {
		return
	}
	if p.isLeader() {
		p.tryProposeFromMempool("batch", false)
	}
}

func (p *hotsutff) handleproposal(content []byte) {
	prop := new(Proposal)
	if err := json.Unmarshal(content, prop); err != nil {
		log.Panic(err)
	}
	block := prop.Block
	if prop.View != block.View || block.View > maxView || block.Justify == nil || block.Proposer != p.leaderID() {
		return
	}
	computed := computeBlockHash(block)
	if block.Hash == "" {
		block.Hash = computed
	}
	if block.Hash != computed || block.ParentHash != block.Justify.BlockHash || len(block.CommandIDs) == 0 || len(prop.Requests) != len(block.CommandIDs) {
		return
	}
	for i, id := range block.CommandIDs {
		if prop.Requests[i].ID != id {
			return
		}
		p.addRequestToPool(prop.Requests[i])
	}
	if !p.hasRequests(block.CommandIDs) {
		return
	}
	cur := p.currentView()
	if block.View < cur {
		return
	}
	if block.View > cur {
		p.advanceView(block.View, "preprepare")
	}
	if !p.isSafeBlock(&block) {
		return
	}
	p.updateHighQC(block.Justify)
	p.lock.Lock()
	if _, ok := p.blocks[block.Hash]; !ok {
		p.blocks[block.Hash] = &block
	}
	p.blockRequests[block.Hash] = append([]int(nil), block.CommandIDs...)
	p.lock.Unlock()
	p.touchProgress()
	p.broadcastPrepareVote(block.View, block.Hash)
}

func (p *hotsutff) broadcastPrepareVote(view int, blockHash string) {
	p.lock.Lock()
	if p.votedPrepare[blockHash] {
		p.lock.Unlock()
		return
	}
	p.votedPrepare[blockHash] = true
	p.lock.Unlock()
	sig := p.signVote(phasePrepare, view, blockHash)
	vote := Vote{View: view, BlockHash: blockHash, Phase: phasePrepare, NodeID: p.node.nodeID, Sig: sig}
	b, _ := json.Marshal(vote)
	p.handlePrepareMsg(b)
	p.broadcast(cmsgPrepare, b)
}

func (p *hotsutff) handlePrepareMsg(content []byte) {
	vote := new(Vote)
	if err := json.Unmarshal(content, vote); err != nil {
		log.Panic(err)
	}
	if vote.Phase != phasePrepare || vote.View > maxView || !p.verifyVoteSig(vote.NodeID, vote.Phase, vote.View, vote.BlockHash, vote.Sig) {
		return
	}
	p.lock.Lock()
	block, ok := p.blocks[vote.BlockHash]
	if !ok || block.View != vote.View {
		p.lock.Unlock()
		return
	}
	if _, ok := p.prepareVotes[vote.BlockHash]; !ok {
		p.prepareVotes[vote.BlockHash] = make(map[string]Vote)
	}
	p.prepareVotes[vote.BlockHash][vote.NodeID] = *vote
	count := len(p.prepareVotes[vote.BlockHash])
	already := p.commitBroadcasted[vote.BlockHash]
	p.lock.Unlock()
	p.touchProgress()
	if count >= threshold && !already {
		p.broadcastCommitVote(vote.View, vote.BlockHash)
	}
}

func (p *hotsutff) broadcastCommitVote(view int, blockHash string) {
	p.lock.Lock()
	if p.votedCommit[blockHash] {
		p.lock.Unlock()
		return
	}
	p.votedCommit[blockHash] = true
	p.commitBroadcasted[blockHash] = true
	p.lock.Unlock()
	sig := p.signVote(phaseCommit, view, blockHash)
	vote := Vote{View: view, BlockHash: blockHash, Phase: phaseCommit, NodeID: p.node.nodeID, Sig: sig}
	b, _ := json.Marshal(vote)
	p.handleCommitMsg(b, time.Now())
	p.broadcast(cmsgCommit, b)
}

func (p *hotsutff) handleCommitMsg(content []byte, now time.Time) {
	vote := new(Vote)
	if err := json.Unmarshal(content, vote); err != nil {
		log.Panic(err)
	}
	if vote.Phase != phaseCommit || vote.View > maxView || !p.verifyVoteSig(vote.NodeID, vote.Phase, vote.View, vote.BlockHash, vote.Sig) {
		return
	}
	p.lock.Lock()
	block, ok := p.blocks[vote.BlockHash]
	if !ok || block.View != vote.View {
		p.lock.Unlock()
		return
	}
	if _, ok := p.commitVotes[vote.BlockHash]; !ok {
		p.commitVotes[vote.BlockHash] = make(map[string]Vote)
	}
	p.commitVotes[vote.BlockHash][vote.NodeID] = *vote
	count := len(p.commitVotes[vote.BlockHash])
	p.lock.Unlock()
	p.touchProgress()
	if count < threshold {
		return
	}
	qc := p.buildQCFromVotes(vote.BlockHash, phaseCommit, vote.View, p.commitVotes)
	p.updateHighQC(qc)
	p.markCommitReady(vote.BlockHash, "pbft", now)
}

func (p *hotsutff) buildQCFromVotes(blockHash string, phase Phase, view int, votePool map[string]map[string]Vote) *QC {
	p.lock.Lock()
	defer p.lock.Unlock()
	votes, ok := votePool[blockHash]
	if !ok {
		return nil
	}
	list := make([]Vote, 0, len(votes))
	for _, v := range votes {
		list = append(list, v)
	}
	return &QC{View: view, BlockHash: blockHash, Phase: phase, Votes: list}
}

func (p *hotsutff) markCommitReady(blockHash, commitType string, now time.Time) {
	p.lock.Lock()
	if _, ok := p.commitReadyType[blockHash]; ok {
		p.lock.Unlock()
		return
	}
	p.commitReadyType[blockHash] = commitType
	p.lock.Unlock()
	p.tryCommitChain(now)
}

func (p *hotsutff) tryCommitChain(now time.Time) {
	for {
		var commitHash string
		var blockView int
		var replies []Request
		p.lock.Lock()
		for hash := range p.commitReadyType {
			block, ok := p.blocks[hash]
			if !ok || block.ParentHash != p.lastCommittedHash {
				continue
			}
			if p.decided[hash] {
				delete(p.commitReadyType, hash)
				continue
			}
			commitHash = hash
			blockView = block.View
			p.decided[hash] = true
			delete(p.commitReadyType, hash)
			p.lastCommittedHash = hash
			p.lastCommittedView = block.View
			if start, ok := p.viewStart[block.View]; ok {
				p.viewDelay[block.View] = time.Since(start)
			}
			ids := p.blockRequests[hash]
			for _, id := range ids {
				if req, exists := p.requestPool[id]; exists {
					replies = append(replies, req)
					delete(p.requestPool, id)
				}
			}
			delete(p.blockRequests, hash)
			break
		}
		p.lock.Unlock()
		if commitHash == "" {
			return
		}
		commitTime := time.Now()
		p.updateThroughputStats(blockView, len(replies), commitTime)
		if p.node.nodeID == p.leaderID() {
			for _, req := range replies {
				go wsDial([]byte(p.node.nodeID+"节点提交msgid:"+strconv.Itoa(req.ID)), req.ClientAddr)
			}
			p.printConsensusStats()
			p.tryProposeFromMempool("commit", false)
		}
		ti++
		if ti > 3 {
			log.Println(time.Since(now))
		}
		p.touchProgress()
	}
}

func (p *hotsutff) updateThroughputStats(view, requestCount int, commitTime time.Time) {
	if view < statsViewStart || view > statsViewEnd {
		return
	}
	p.lock.Lock()
	if p.statsViews[view] {
		p.lock.Unlock()
		return
	}
	p.statsViews[view] = true
	if p.statsStart.IsZero() || commitTime.Before(p.statsStart) {
		p.statsStart = commitTime
	}
	if p.statsEnd.IsZero() || commitTime.After(p.statsEnd) {
		p.statsEnd = commitTime
	}
	p.statsRequests += requestCount
	p.lock.Unlock()
}

func (p *hotsutff) printConsensusStats() {
	if !p.isLeader() {
		return
	}
	p.lock.Lock()
	delays := make(map[int]time.Duration)
	for view := statsViewStart; view <= statsViewEnd; view++ {
		if delay, ok := p.viewDelay[view]; ok {
			delays[view] = delay
		}
	}
	statsStart, statsEnd, statsRequests := p.statsStart, p.statsEnd, p.statsRequests
	p.lock.Unlock()
	if len(delays) == 0 {
		return
	}
	log.Println("==== 主节点共识时延（pre-prepare -> commit） ====")
	for view := statsViewStart; view <= statsViewEnd; view++ {
		if delay, ok := delays[view]; ok {
			log.Printf("view %d: %v", view, delay)
		}
	}
	if statsRequests > 0 && !statsStart.IsZero() && !statsEnd.IsZero() {
		window := statsEnd.Sub(statsStart)
		if window > 0 {
			throughput := float64(statsRequests) / window.Seconds()
			log.Printf("统计视图[%d-%d] 吞吐量: %.2f req/s (requests=%d, window=%v)", statsViewStart, statsViewEnd, throughput, statsRequests, window)
		}
	}
}

func (p *hotsutff) isSafeBlock(block *Block) bool {
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.lockedQC == nil || block.Justify == nil {
		return true
	}
	if block.Justify.View >= p.lockedQC.View {
		return true
	}
	return block.ParentHash == p.lockedQC.BlockHash
}

func (p *hotsutff) updateHighQC(qc *QC) {
	if qc == nil {
		return
	}
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.highQC == nil || qc.View >= p.highQC.View {
		p.highQC = qc
		p.lockedQC = qc
	}
}

package main

import (
	"encoding/json"
	"log"
	"sort"
	"strconv"
	"sync"
	"time"
)

// 本地消息池（模拟持久化层），只有确认提交成功后才会存入此池
var ti int = 0

const (
	pacemakerTickMs = 200
	viewTimeoutMs   = 3000
	maxView         = 10
	batchSize       = 2
	batchTimeoutMs  = 1500
	slowPathWaitMs  = 200
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

	prepareVotes   map[string]map[string]bool
	precommitVotes map[string]map[string]bool
	commitVotes    map[string]map[string]bool

	prepareVoteSigs   map[string]map[string][]byte
	precommitVoteSigs map[string]map[string][]byte
	commitVoteSigs    map[string]map[string][]byte

	precommitBroadcasted map[string]bool
	commitBroadcasted    map[string]bool
	commitQCBroadcasted   map[string]bool
	fastQCBroadcasted     map[string]bool
	slowPathReady         map[string]bool

	fastQCReady      map[string]bool
	commitReadyType  map[string]string
	aggQCRequests    map[int]*AggQCRequest
	aggQCVotes       map[int]map[string][]byte
	aggQCs           map[int]*AggQC

	votedPrepare   map[string]bool
	votedPrecommit map[string]bool
	votedCommit    map[string]bool
	decided        map[string]bool
}

// NewHotStuff 初始化节点状态并启动 pacemaker.
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

	p.prepareVotes = make(map[string]map[string]bool)
	p.precommitVotes = make(map[string]map[string]bool)
	p.commitVotes = make(map[string]map[string]bool)

	p.prepareVoteSigs = make(map[string]map[string][]byte)
	p.precommitVoteSigs = make(map[string]map[string][]byte)
	p.commitVoteSigs = make(map[string]map[string][]byte)

	p.precommitBroadcasted = make(map[string]bool)
	p.commitBroadcasted = make(map[string]bool)
	p.commitQCBroadcasted = make(map[string]bool)
	p.fastQCBroadcasted = make(map[string]bool)
	p.slowPathReady = make(map[string]bool)
	p.fastQCReady = make(map[string]bool)
	p.commitReadyType = make(map[string]string)
	p.aggQCRequests = make(map[int]*AggQCRequest)
	p.aggQCVotes = make(map[int]map[string][]byte)
	p.aggQCs = make(map[int]*AggQC)

	p.votedPrepare = make(map[string]bool)
	p.votedPrecommit = make(map[string]bool)
	p.votedCommit = make(map[string]bool)
	p.decided = make(map[string]bool)

	genesis := &Block{
		Hash:       "genesis",
		ParentHash: "",
		Height:     0,
		View:       0,
		Proposer:   "genesis",
		CommandIDs: nil,
		Justify:    nil,
	}
	p.blocks[genesis.Hash] = genesis
	p.highQC = &QC{View: 0, BlockHash: genesis.Hash, Phase: phaseCommit, Signers: nil}
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

	log.Printf("节点%s初始化完成（教学版Fast-HotStuff）", nodeID)
	return p
}

func (p *hotsutff) leaderID() string {
	return "0"
}

func (p *hotsutff) isLeader() bool {
	return p.node.nodeID == p.leaderID()
}

func (p *hotsutff) currentView() int {
	p.lock.Lock()
	defer p.lock.Unlock()
	return p.view
}

func (p *hotsutff) touchProgress() {
	p.lock.Lock()
	p.lastProgress = time.Now()
	p.lock.Unlock()
}

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

// pacemakerLoop 驱动超时与主节点批处理。
func (p *hotsutff) pacemakerLoop() {
	ticker := time.NewTicker(time.Duration(pacemakerTickMs) * time.Millisecond)
	defer ticker.Stop()
	timeout := time.Duration(viewTimeoutMs) * time.Millisecond
	batchTimeout := time.Duration(batchTimeoutMs) * time.Millisecond
	for range ticker.C {
		if p.currentView() >= maxView {
			continue
		}

		if p.pendingRequestCount() == 0 {
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
		if newView >= maxView {
			log.Printf("[节点%s] pacemaker超时，达到最大视图: %d", p.node.nodeID, newView)
		} else {
			log.Printf("[节点%s] pacemaker超时，触发新视图: %d", p.node.nodeID, newView)
		}
		p.sendNewView(newView)
	}
}

// sendNewView 在超时后把本地 HighQC 发送给主节点。
func (p *hotsutff) sendNewView(view int) {
	if view > maxView {
		return
	}
	p.lock.Lock()
	qc := p.highQC
	p.lock.Unlock()
	msg := NewViewMsg{
		View:   view,
		NodeID: p.node.nodeID,
		HighQC: qc,
	}
	b, err := json.Marshal(msg)
	if err != nil {
		log.Panic(err)
	}
	message := jointMessage(cnewView, b)
	leader := p.leaderID()
	log.Printf("[节点%s] 发送NewView(view=%d)给主节点%s", p.node.nodeID, view, leader)
	go wsDial(message, nodeTable[leader])
}

// handleNewView 收集 NewView，并在满足条件时触发提案。
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
	bestQC := p.selectHighestQC(p.newViewMessages[msg.View])
	p.lock.Unlock()

	if count < threshold {
		return
	}

	if bestQC != nil {
		p.updateHighQC(bestQC)
	}
	p.startAggQC(msg.View, p.newViewMessages[msg.View])
	p.advanceView(msg.View, "newview")
	if msg.View <= maxView {
		batch := p.nextBatch(true)
		if len(batch) > 0 {
			p.proposeBatchForView(batch, msg.View, "newview")
		}
	}
}

// selectHighestQC 从集合中选出视图最高的 QC。
func (p *hotsutff) selectHighestQC(qcs map[string]*QC) *QC {
	var best *QC
	for _, qc := range qcs {
		if qc == nil {
			continue
		}
		if best == nil || qc.View > best.View {
			best = qc
		}
	}
	return best
}

// startAggQC 收集 NewView 的 QC 列表并触发 AggQC 签名收集。
func (p *hotsutff) startAggQC(view int, qcs map[string]*QC) {
	if !p.isLeader() {
		return
	}
	if view > maxView {
		return
	}
	p.lock.Lock()
	if _, ok := p.aggQCRequests[view]; ok {
		p.lock.Unlock()
		return
	}
	list := make([]QC, 0, len(qcs))
	for _, qc := range qcs {
		if qc == nil {
			continue
		}
		list = append(list, *qc)
	}
	p.lock.Unlock()

	if len(list) == 0 {
		return
	}

	hash := computeAggQCHash(list)
	req := AggQCRequest{
		View: view,
		Hash: hash,
		QCs:  list,
	}
	b, err := json.Marshal(req)
	if err != nil {
		log.Panic(err)
	}

	p.lock.Lock()
	p.aggQCRequests[view] = &req
	p.lock.Unlock()

	selfSig := p.signAggQC(view, hash)
	selfVote := AggQCVote{
		View:   view,
		Hash:   hash,
		NodeID: p.node.nodeID,
		Sig:    selfSig,
	}
	vb, err := json.Marshal(selfVote)
	if err == nil {
		p.handleAggQCVote(vb)
	}

	log.Printf("[节点%s] 广播aggQCReq(view=%d)", p.node.nodeID, view)
	p.broadcast(caggQCReq, b)
}

// addRequestToPool 存储新请求，并为主节点排队。
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

// nextBatch 从主节点队列弹出下一批请求 ID。
func (p *hotsutff) nextBatch(force bool) []int {
	p.lock.Lock()
	defer p.lock.Unlock()
	if len(p.pendingQueue) == 0 {
		return nil
	}
	if len(p.pendingQueue) < batchSize && !force {
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
	if p.view >= maxView {
		return false
	}
	if p.highQC == nil {
		return false
	}
	if p.highQC.View < p.view {
		return false
	}
	return true
}

// tryProposeFromMempool 在空闲时最多推进一个新视图提案。
func (p *hotsutff) tryProposeFromMempool(reason string, force bool) {
	if !p.isLeader() {
		return
	}
	if !p.canProposeNext() {
		return
	}

	batch := p.nextBatch(force)
	if len(batch) == 0 {
		return
	}
	if p.currentView() >= maxView {
		return
	}
	view := p.advanceView(p.currentView()+1, reason)
	if view > maxView {
		return
	}
	p.proposeBatchForView(batch, view, reason)
}

// proposeBatchForView 构造区块/提案并启动 prepare。
func (p *hotsutff) proposeBatchForView(batchIDs []int, view int, reason string) {
	if !p.isLeader() {
		return
	}
	if view > maxView {
		log.Printf("[节点%s] 视图超过上限，拒绝提案: %d", p.node.nodeID, view)
		return
	}
	if len(batchIDs) == 0 {
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
			log.Printf("[节点%s] 批次缺少请求msgid:%d，拒绝提案", p.node.nodeID, id)
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
	block := Block{
		ParentHash: parent,
		Height:     height,
		View:       view,
		Proposer:   p.node.nodeID,
		CommandIDs: append([]int(nil), batchIDs...),
		Justify:    p.highQC,
	}
	block.Hash = computeBlockHash(block)
	p.blocks[block.Hash] = &block
	p.blockRequests[block.Hash] = append([]int(nil), batchIDs...)
	aggQC := p.aggQCs[view]
	p.lock.Unlock()

	prop := Proposal{View: view, Block: block, Requests: reqs, AggQC: aggQC}
	b, err := json.Marshal(prop)
	if err != nil {
		log.Panic(err)
	}

	log.Printf("[节点%s] 广播proposal(view=%d, reason=%s)，区块哈希: %s", p.node.nodeID, view, reason, block.Hash[:16]+"...")
	p.broadcast(cproposal, b)
	p.touchProgress()
	p.armSlowPath(block.Hash)

	selfSig := p.signVote(phasePrepare, block.View, block.Hash)
	p.addVote(p.prepareVotes, p.prepareVoteSigs, block.Hash, p.node.nodeID, selfSig)
	p.tryBroadcastFastQC(block.Hash)
	p.tryBroadcastPreCommit(block.Hash)
}

// armSlowPath 为 slow path 预留等待窗口，避免过早进入慢路径。
func (p *hotsutff) armSlowPath(blockHash string) {
	if !p.isLeader() {
		return
	}
	if slowPathWaitMs <= 0 {
		p.lock.Lock()
		p.slowPathReady[blockHash] = true
		p.lock.Unlock()
		return
	}
	p.lock.Lock()
	p.slowPathReady[blockHash] = false
	p.lock.Unlock()
	go func() {
		time.Sleep(time.Duration(slowPathWaitMs) * time.Millisecond)
		p.lock.Lock()
		if p.decided[blockHash] || p.commitReadyType[blockHash] == "fast" {
			p.lock.Unlock()
			return
		}
		p.slowPathReady[blockHash] = true
		p.lock.Unlock()
		p.tryBroadcastPreCommit(blockHash)
	}()
}

// handleRequest 按命令类型分发消息。
func (p *hotsutff) handleRequest(data []byte, now time.Time) {
	cmd, content := splitMessage(data)
	log.Printf("[节点%s] 收到消息，命令类型: %s, 消息长度: %d 字节", p.node.nodeID, cmd, len(data))
	switch command(cmd) {
	case cRequest:
		p.handleClientRequest(content)
	case cproposal:
		log.Printf("[节点%s] 收到proposal消息", p.node.nodeID)
		p.handleproposal(content)
	case cnewView:
		log.Printf("[节点%s] 收到newView消息", p.node.nodeID)
		p.handleNewView(content)
	case cvotePrepare:
		log.Printf("[节点%s] 收到votePrepare投票", p.node.nodeID)
		p.handleprepare(content)
	case cmsgPreCommit:
		log.Printf("[节点%s] 收到msgPreCommit消息", p.node.nodeID)
		p.handledepreCommit(content)
	case cvotePreCommit:
		log.Printf("[节点%s] 收到votePreCommit投票", p.node.nodeID)
		p.handlepreCommit(content)
	case cmsgCommit:
		log.Printf("[节点%s] 收到msgCommit消息", p.node.nodeID)
		p.handledeCommit(content)
	case cvoteCommit:
		log.Printf("[节点%s] 收到voteCommit投票", p.node.nodeID)
		p.handleCommit(content)
	case cmsgCommitQC:
		log.Printf("[节点%s] 收到msgCommitQC消息", p.node.nodeID)
		p.handleCommitQC(content, now)
	case cmsgFastQC:
		log.Printf("[节点%s] 收到msgFastQC消息", p.node.nodeID)
		p.handleFastQC(content, now)
	case caggQCReq:
		log.Printf("[节点%s] 收到aggQCReq消息", p.node.nodeID)
		p.handleAggQCRequest(content)
	case caggQCVote:
		log.Printf("[节点%s] 收到aggQCVote消息", p.node.nodeID)
		p.handleAggQCVote(content)
	}
}

// handleFastQC 处理主节点广播的 fast QC。
func (p *hotsutff) handleFastQC(content []byte, now time.Time) {
	msg := new(PhaseMsg)
	if err := json.Unmarshal(content, msg); err != nil {
		log.Panic(err)
	}
	if msg.NodeID != p.leaderID() {
		return
	}
	if msg.View > maxView {
		return
	}

	currentView := p.currentView()
	if msg.View < currentView {
		return
	}
	if msg.View > currentView {
		p.advanceView(msg.View, "fastQC")
	}

	p.lock.Lock()
	block, ok := p.blocks[msg.BlockHash]
	p.lock.Unlock()
	if !ok || block.View != msg.View {
		return
	}

	if !p.isFastQCValid(&msg.QC, msg.BlockHash) {
		log.Printf("[节点%s] FastQC消息QC验证失败", p.node.nodeID)
		return
	}
	p.updateHighQC(&msg.QC)
	p.touchProgress()
	p.markFastQC(msg.BlockHash, now)

	if p.isLeader() {
		p.tryProposeFromMempool("fastqc", false)
	}
}

// handleAggQCRequest 处理 leader 发起的 AggQC 签名请求。
func (p *hotsutff) handleAggQCRequest(content []byte) {
	req := new(AggQCRequest)
	if err := json.Unmarshal(content, req); err != nil {
		log.Panic(err)
	}
	if req.View > maxView {
		return
	}
	if req.Hash != computeAggQCHash(req.QCs) {
		log.Printf("[节点%s] AggQCRequest哈希校验失败", p.node.nodeID)
		return
	}
	sig := p.signAggQC(req.View, req.Hash)
	vote := AggQCVote{
		View:   req.View,
		Hash:   req.Hash,
		NodeID: p.node.nodeID,
		Sig:    sig,
	}
	b, err := json.Marshal(vote)
	if err != nil {
		log.Panic(err)
	}
	message := jointMessage(caggQCVote, b)
	go wsDial(message, nodeTable[p.leaderID()])
}

// handleAggQCVote 在主节点收集 AggQC 签名。
func (p *hotsutff) handleAggQCVote(content []byte) {
	if !p.isLeader() {
		return
	}
	vote := new(AggQCVote)
	if err := json.Unmarshal(content, vote); err != nil {
		log.Panic(err)
	}
	if vote.View > maxView {
		return
	}
	p.lock.Lock()
	req := p.aggQCRequests[vote.View]
	p.lock.Unlock()
	if req == nil || vote.Hash != req.Hash {
		return
	}
	if !p.verifyAggQCVoteSig(vote.NodeID, vote.View, vote.Hash, vote.Sig) {
		log.Printf("[节点%s] AggQCVote签名验证失败", p.node.nodeID)
		return
	}

	p.lock.Lock()
	if _, ok := p.aggQCVotes[vote.View]; !ok {
		p.aggQCVotes[vote.View] = make(map[string][]byte)
	}
	p.aggQCVotes[vote.View][vote.NodeID] = append([]byte(nil), vote.Sig...)
	voteCount := len(p.aggQCVotes[vote.View])
	if p.aggQCs[vote.View] != nil || voteCount < threshold {
		p.lock.Unlock()
		return
	}
	signers := make([]string, 0, voteCount)
	for id := range p.aggQCVotes[vote.View] {
		signers = append(signers, id)
	}
	sort.Strings(signers)
	sigList := make([][]byte, 0, len(signers))
	for _, id := range signers {
		sigList = append(sigList, append([]byte(nil), p.aggQCVotes[vote.View][id]...))
	}
	p.lock.Unlock()

	aggSig, ok := aggregateSignatures(sigList)
	if !ok {
		return
	}
	agg := AggQC{
		View:    vote.View,
		Hash:    vote.Hash,
		QCs:     req.QCs,
		Signers: signers,
		AggSig:  aggSig,
	}
	p.lock.Lock()
	p.aggQCs[vote.View] = &agg
	p.lock.Unlock()
}

// handleClientRequest 入池并触发批处理提案。
func (p *hotsutff) handleClientRequest(content []byte) {
	r := new(Request)
	if err := json.Unmarshal(content, r); err != nil {
		log.Panic(err)
	}
	if p.currentView() >= maxView {
		log.Printf("[节点%s] 已达到最大视图%d，忽略请求msgid:%d", p.node.nodeID, maxView, r.ID)
		return
	}
	if added := p.addRequestToPool(*r); !added {
		return
	}
	log.Printf("[节点%s] 请求已加入消息池，msgid: %d", p.node.nodeID, r.ID)
	if p.isLeader() {
		p.tryProposeFromMempool("batch", false)
	}
}

// handleproposal 校验主节点提案并投票 prepare。
func (p *hotsutff) handleproposal(content []byte) {
	prop := new(Proposal)
	if err := json.Unmarshal(content, prop); err != nil {
		log.Panic(err)
	}

	block := prop.Block
	if prop.View != block.View {
		log.Printf("[节点%s] proposal视图不一致，拒绝", p.node.nodeID)
		return
	}
	if block.View > maxView {
		log.Printf("[节点%s] proposal视图超过上限，拒绝: %d", p.node.nodeID, block.View)
		return
	}
	computed := computeBlockHash(block)
	if block.Hash == "" {
		block.Hash = computed
	}
	if block.Hash != computed {
		log.Printf("[节点%s] proposal哈希不一致，拒绝", p.node.nodeID)
		return
	}
	if block.Justify == nil {
		log.Printf("[节点%s] proposal缺少QC，拒绝", p.node.nodeID)
		return
	}
	if prop.AggQC != nil {
		if !p.isAggQCValid(prop.AggQC) {
			log.Printf("[节点%s] proposal的AggQC验证失败，拒绝", p.node.nodeID)
			return
		}
	}
	if block.ParentHash != block.Justify.BlockHash {
		log.Printf("[节点%s] proposal父哈希不匹配QC，拒绝", p.node.nodeID)
		return
	}
	if block.Proposer != p.leaderID() {
		log.Printf("[节点%s] proposal主节点不匹配，拒绝", p.node.nodeID)
		return
	}
	if len(block.CommandIDs) == 0 {
		log.Printf("[节点%s] proposal批次为空，拒绝", p.node.nodeID)
		return
	}
	if len(prop.Requests) == 0 {
		log.Printf("[节点%s] proposal缺少完整请求，拒绝", p.node.nodeID)
		return
	}
	if len(prop.Requests) != len(block.CommandIDs) {
		log.Printf("[节点%s] proposal请求数量不匹配，拒绝", p.node.nodeID)
		return
	}
	for i, id := range block.CommandIDs {
		if prop.Requests[i].ID != id {
			log.Printf("[节点%s] proposal请求顺序不匹配，拒绝", p.node.nodeID)
			return
		}
		p.addRequestToPool(prop.Requests[i])
	}
	if !p.hasRequests(block.CommandIDs) {
		log.Printf("[节点%s] proposal包含未知请求，拒绝", p.node.nodeID)
		return
	}

	currentView := p.currentView()
	if block.View < currentView {
		log.Printf("[节点%s] proposal视图过旧，拒绝（%d < %d）", p.node.nodeID, block.View, currentView)
		return
	}
	if block.View > currentView {
		p.advanceView(block.View, "proposal")
	}

	if !p.isSafeBlock(&block) {
		log.Printf("[节点%s] proposal不满足安全规则，拒绝", p.node.nodeID)
		return
	}

	p.updateHighQC(block.Justify)

	p.lock.Lock()
	if _, ok := p.blocks[block.Hash]; !ok {
		p.blocks[block.Hash] = &block
	}
	p.blockRequests[block.Hash] = append([]int(nil), block.CommandIDs...)
	if p.votedPrepare[block.Hash] {
		p.lock.Unlock()
		return
	}
	p.votedPrepare[block.Hash] = true
	p.lock.Unlock()

	p.touchProgress()
	sig := p.signVote(phasePrepare, block.View, block.Hash)
	vote := Vote{
		View:      block.View,
		BlockHash: block.Hash,
		Phase:     phasePrepare,
		NodeID:    p.node.nodeID,
		Sig:       sig,
	}
	b, err := json.Marshal(vote)
	if err != nil {
		log.Panic(err)
	}
	message := jointMessage(cvotePrepare, b)
	log.Printf("[节点%s] 发送Prepare Vote给主节点，区块哈希: %s", p.node.nodeID, block.Hash[:16]+"...")
	go wsDial(message, nodeTable[p.leaderID()])
}

// handleprepare 在主节点收集 prepare 投票。
func (p *hotsutff) handleprepare(content []byte) {
	if !p.isLeader() {
		return
	}
	vote := new(Vote)
	if err := json.Unmarshal(content, vote); err != nil {
		log.Panic(err)
	}
	if vote.Phase != phasePrepare {
		return
	}
	if vote.View > maxView {
		return
	}
	if !p.verifyVoteSig(vote.NodeID, vote.Phase, vote.View, vote.BlockHash, vote.Sig) {
		log.Printf("[节点%s] Prepare投票签名验证失败", p.node.nodeID)
		return
	}

	p.lock.Lock()
	block, ok := p.blocks[vote.BlockHash]
	p.lock.Unlock()
	if !ok || block.View != vote.View {
		return
	}

	p.addVote(p.prepareVotes, p.prepareVoteSigs, vote.BlockHash, vote.NodeID, vote.Sig)
	p.touchProgress()
	p.tryBroadcastFastQC(vote.BlockHash)
	p.tryBroadcastPreCommit(vote.BlockHash)
}

// handledepreCommit 处理主节点的 precommit QC。
func (p *hotsutff) handledepreCommit(content []byte) {
	msg := new(PhaseMsg)
	if err := json.Unmarshal(content, msg); err != nil {
		log.Panic(err)
	}
	if msg.NodeID != p.leaderID() {
		return
	}
	if msg.View > maxView {
		return
	}

	currentView := p.currentView()
	if msg.View < currentView {
		return
	}
	if msg.View > currentView {
		p.advanceView(msg.View, "precommit")
	}

	p.lock.Lock()
	block, ok := p.blocks[msg.BlockHash]
	p.lock.Unlock()
	if !ok || block.View != msg.View {
		return
	}

	if !p.isQCValid(&msg.QC, phasePrepare, msg.BlockHash) {
		log.Printf("[节点%s] PreCommit消息QC验证失败", p.node.nodeID)
		return
	}
	p.updateLockedQC(&msg.QC)
	p.touchProgress()

	p.lock.Lock()
	if p.votedPrecommit[msg.BlockHash] {
		p.lock.Unlock()
		return
	}
	p.votedPrecommit[msg.BlockHash] = true
	p.lock.Unlock()

	sig := p.signVote(phasePreCommit, msg.View, msg.BlockHash)
	vote := Vote{
		View:      msg.View,
		BlockHash: msg.BlockHash,
		Phase:     phasePreCommit,
		NodeID:    p.node.nodeID,
		Sig:       sig,
	}
	b, err := json.Marshal(vote)
	if err != nil {
		log.Panic(err)
	}
	message := jointMessage(cvotePreCommit, b)
	log.Printf("[节点%s] 发送PreCommit Vote给主节点，区块哈希: %s", p.node.nodeID, msg.BlockHash[:16]+"...")
	go wsDial(message, nodeTable[p.leaderID()])
}

// handlepreCommit 在主节点收集 precommit 投票。
func (p *hotsutff) handlepreCommit(content []byte) {
	if !p.isLeader() {
		return
	}
	vote := new(Vote)
	if err := json.Unmarshal(content, vote); err != nil {
		log.Panic(err)
	}
	if vote.Phase != phasePreCommit {
		return
	}
	if vote.View > maxView {
		return
	}
	if !p.verifyVoteSig(vote.NodeID, vote.Phase, vote.View, vote.BlockHash, vote.Sig) {
		log.Printf("[节点%s] PreCommit投票签名验证失败", p.node.nodeID)
		return
	}

	p.lock.Lock()
	block, ok := p.blocks[vote.BlockHash]
	p.lock.Unlock()
	if !ok || block.View != vote.View {
		return
	}

	p.addVote(p.precommitVotes, p.precommitVoteSigs, vote.BlockHash, vote.NodeID, vote.Sig)
	p.touchProgress()
	p.tryBroadcastCommit(vote.BlockHash)
}

// handledeCommit 处理主节点的 precommit QC。
func (p *hotsutff) handledeCommit(content []byte) {
	msg := new(PhaseMsg)
	if err := json.Unmarshal(content, msg); err != nil {
		log.Panic(err)
	}
	if msg.NodeID != p.leaderID() {
		return
	}
	if msg.View > maxView {
		return
	}

	currentView := p.currentView()
	if msg.View < currentView {
		return
	}
	if msg.View > currentView {
		p.advanceView(msg.View, "commit")
	}

	p.lock.Lock()
	block, ok := p.blocks[msg.BlockHash]
	p.lock.Unlock()
	if !ok || block.View != msg.View {
		return
	}

	if !p.isQCValid(&msg.QC, phasePreCommit, msg.BlockHash) {
		log.Printf("[节点%s] Commit消息QC验证失败", p.node.nodeID)
		return
	}
	p.updateHighQC(&msg.QC)
	p.touchProgress()

	p.lock.Lock()
	if p.votedCommit[msg.BlockHash] {
		p.lock.Unlock()
		return
	}
	p.votedCommit[msg.BlockHash] = true
	p.lock.Unlock()

	sig := p.signVote(phaseCommit, msg.View, msg.BlockHash)
	vote := Vote{
		View:      msg.View,
		BlockHash: msg.BlockHash,
		Phase:     phaseCommit,
		NodeID:    p.node.nodeID,
		Sig:       sig,
	}
	b, err := json.Marshal(vote)
	if err != nil {
		log.Panic(err)
	}
	message := jointMessage(cvoteCommit, b)
	log.Printf("[节点%s] 发送Commit Vote给主节点，区块哈希: %s", p.node.nodeID, msg.BlockHash[:16]+"...")
	go wsDial(message, nodeTable[p.leaderID()])
}

// handleCommit 在主节点收集 commit 投票。
func (p *hotsutff) handleCommit(content []byte) {
	if !p.isLeader() {
		return
	}
	vote := new(Vote)
	if err := json.Unmarshal(content, vote); err != nil {
		log.Panic(err)
	}
	if vote.Phase != phaseCommit {
		return
	}
	if vote.View > maxView {
		return
	}
	if !p.verifyVoteSig(vote.NodeID, vote.Phase, vote.View, vote.BlockHash, vote.Sig) {
		log.Printf("[节点%s] Commit投票签名验证失败", p.node.nodeID)
		return
	}

	p.lock.Lock()
	block, ok := p.blocks[vote.BlockHash]
	p.lock.Unlock()
	if !ok || block.View != vote.View {
		return
	}

	p.addVote(p.commitVotes, p.commitVoteSigs, vote.BlockHash, vote.NodeID, vote.Sig)
	p.touchProgress()
	p.tryBroadcastCommitQC(vote.BlockHash)
}

// handleCommitQC 处理主节点的 commit QC。
func (p *hotsutff) handleCommitQC(content []byte, now time.Time) {
	msg := new(PhaseMsg)
	if err := json.Unmarshal(content, msg); err != nil {
		log.Panic(err)
	}
	if msg.NodeID != p.leaderID() {
		return
	}
	if msg.View > maxView {
		return
	}

	currentView := p.currentView()
	if msg.View < currentView {
		return
	}
	if msg.View > currentView {
		p.advanceView(msg.View, "commitQC")
	}

	p.lock.Lock()
	block, ok := p.blocks[msg.BlockHash]
	p.lock.Unlock()
	if !ok || block.View != msg.View {
		return
	}

	if !p.isQCValid(&msg.QC, phaseCommit, msg.BlockHash) {
		log.Printf("[节点%s] CommitQC消息QC验证失败", p.node.nodeID)
		return
	}
	p.updateHighQC(&msg.QC)
	p.touchProgress()

	p.markCommitReady(msg.BlockHash, "slow", now)
}

func (p *hotsutff) markFastQC(blockHash string, now time.Time) {
	p.lock.Lock()
	if p.fastQCReady[blockHash] {
		p.lock.Unlock()
		return
	}
	p.fastQCReady[blockHash] = true
	block, ok := p.blocks[blockHash]
	parentHash := ""
	if ok {
		parentHash = block.ParentHash
	}
	parentReady := parentHash != "" && p.fastQCReady[parentHash]
	p.lock.Unlock()

	if parentReady {
		p.markCommitReady(parentHash, "fast", now)
	}
}

func (p *hotsutff) markCommitReady(blockHash, commitType string, now time.Time) {
	p.lock.Lock()
	if existing, ok := p.commitReadyType[blockHash]; ok {
		if existing == "fast" || existing == commitType {
			p.lock.Unlock()
			return
		}
		if commitType == "fast" {
			p.commitReadyType[blockHash] = commitType
		}
		p.lock.Unlock()
	} else {
		p.commitReadyType[blockHash] = commitType
		p.lock.Unlock()
	}
	p.tryCommitChain(now)
}

func (p *hotsutff) tryCommitChain(now time.Time) {
	for {
		var (
			commitHash string
			commitType string
			blockView  int
			replies    []Request
		)
		p.lock.Lock()
		for hash, typ := range p.commitReadyType {
			block, ok := p.blocks[hash]
			if !ok {
				continue
			}
			if block.ParentHash != p.lastCommittedHash {
				continue
			}
			if p.decided[hash] {
				delete(p.commitReadyType, hash)
				continue
			}
			commitHash = hash
			commitType = typ
			blockView = block.View
			p.decided[hash] = true
			delete(p.commitReadyType, hash)
			p.lastCommittedHash = hash
			p.lastCommittedView = block.View
			if start, ok := p.viewStart[block.View]; ok {
				p.viewDelay[block.View] = time.Since(start)
			}
			ids, ok := p.blockRequests[hash]
			if ok {
				for _, id := range ids {
					if req, exists := p.requestPool[id]; exists {
						replies = append(replies, req)
						delete(p.requestPool, id)
					}
				}
				delete(p.blockRequests, hash)
			}
			break
		}
		p.lock.Unlock()

		if commitHash == "" {
			return
		}

		commitTime := time.Now()
		p.updateThroughputStats(blockView, len(replies), commitTime)

		log.Printf("[节点%s] 区块已提交(%s)，哈希: %s", p.node.nodeID, commitType, commitHash[:16]+"...")

		if p.node.nodeID == p.leaderID() {
			for _, req := range replies {
				info := p.node.nodeID + "节点提交msgid:" + strconv.Itoa(req.ID)
				log.Printf("[节点%s] 向客户端发送回复: %s", p.node.nodeID, info)
				go wsDial([]byte(info), req.ClientAddr)
			}
			p.printConsensusStats()
			p.tryProposeFromMempool("commit", false)
		}

		ti++
		if ti > 3 {
			tmp := time.Since(now)
			log.Println(tmp)
		}

		p.touchProgress()
	}
}

// addVote 记录投票及其签名，供聚合使用。
func (p *hotsutff) addVote(votes map[string]map[string]bool, sigs map[string]map[string][]byte, blockHash, nodeID string, sig []byte) int {
	p.lock.Lock()
	defer p.lock.Unlock()
	if _, ok := votes[blockHash]; !ok {
		votes[blockHash] = make(map[string]bool)
	}
	votes[blockHash][nodeID] = true
	if sigs != nil {
		if _, ok := sigs[blockHash]; !ok {
			sigs[blockHash] = make(map[string][]byte)
		}
		sigs[blockHash][nodeID] = append([]byte(nil), sig...)
	}
	return len(votes[blockHash])
}

// snapshotVoteSigs 按签名者集合取出对应签名。
func snapshotVoteSigs(sigMap map[string]map[string][]byte, blockHash string, signers []string) ([][]byte, bool) {
	m, ok := sigMap[blockHash]
	if !ok {
		return nil, false
	}
	sigs := make([][]byte, 0, len(signers))
	for _, id := range signers {
		sigBytes, ok := m[id]
		if !ok {
			return nil, false
		}
		sigs = append(sigs, append([]byte(nil), sigBytes...))
	}
	return sigs, true
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
	statsStart := p.statsStart
	statsEnd := p.statsEnd
	statsRequests := p.statsRequests
	p.lock.Unlock()

	if len(delays) == 0 {
		return
	}
	log.Println("==== 主节点共识时延（proposal -> commit） ====")
	for view := statsViewStart; view <= statsViewEnd; view++ {
		if delay, ok := delays[view]; ok {
			log.Printf("view %d: %v", view, delay)
		}
	}
	log.Println("============================================")

	if statsRequests > 0 && !statsStart.IsZero() && !statsEnd.IsZero() {
		window := statsEnd.Sub(statsStart)
		if window > 0 {
			throughput := float64(statsRequests) / window.Seconds()
			log.Printf("统计视图[%d-%d] 吞吐量: %.2f req/s (requests=%d, window=%v)", statsViewStart, statsViewEnd, throughput, statsRequests, window)
		}
	}
}

func (p *hotsutff) getSigners(votes map[string]map[string]bool, blockHash string) []string {
	signers := make([]string, 0)
	if m, ok := votes[blockHash]; ok {
		for id := range m {
			signers = append(signers, id)
		}
	}
	return signers
}

// isQCValid 校验 QC 字段并验证聚合签名。
func (p *hotsutff) isQCValid(qc *QC, phase Phase, blockHash string) bool {
	if qc == nil {
		return false
	}
	if qc.Phase != phase {
		return false
	}
	if qc.BlockHash != blockHash {
		return false
	}
	if len(qc.Signers) < threshold {
		return false
	}
	if len(qc.AggSig) == 0 {
		return false
	}
	if !p.verifyQCSignature(qc) {
		return false
	}
	return true
}

func (p *hotsutff) isAggQCValid(agg *AggQC) bool {
	if agg == nil {
		return false
	}
	if agg.Hash != computeAggQCHash(agg.QCs) {
		return false
	}
	if len(agg.Signers) < threshold {
		return false
	}
	if len(agg.AggSig) == 0 {
		return false
	}
	if !p.verifyAggQCSignature(agg) {
		return false
	}
	for i := range agg.QCs {
		qc := agg.QCs[i]
		if qc.BlockHash == "" || len(qc.Signers) < threshold || len(qc.AggSig) == 0 {
			return false
		}
		if !p.verifyQCSignature(&qc) {
			return false
		}
	}
	return true
}

// isFastQCValid 校验 fast QC（基于prepare阶段且签名者>=threshold）。
func (p *hotsutff) isFastQCValid(qc *QC, blockHash string) bool {
	if qc == nil {
		return false
	}
	if qc.Phase != phasePrepare {
		return false
	}
	if qc.BlockHash != blockHash {
		return false
	}
	if len(qc.Signers) < threshold {
		return false
	}
	if len(qc.AggSig) == 0 {
		return false
	}
	if !p.verifyQCSignature(qc) {
		return false
	}
	return true
}

// isSafeBlock 按简化安全规则检查 lockedQC。
func (p *hotsutff) isSafeBlock(block *Block) bool {
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.lockedQC == nil {
		return true
	}
	if block.Justify == nil {
		return false
	}
	if block.Justify.View >= p.lockedQC.View {
		return true
	}
	return block.ParentHash == p.lockedQC.BlockHash
}

func (p *hotsutff) updateLockedQC(qc *QC) {
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.lockedQC == nil || qc.View >= p.lockedQC.View {
		p.lockedQC = qc
	}
}

func (p *hotsutff) updateHighQC(qc *QC) {
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.highQC == nil || qc.View >= p.highQC.View {
		p.highQC = qc
	}
}

// tryBroadcastFastQC 生成 fast QC 并广播。
func (p *hotsutff) tryBroadcastFastQC(blockHash string) {
	if !p.isLeader() {
		return
	}
	p.lock.Lock()
	if p.fastQCBroadcasted[blockHash] {
		p.lock.Unlock()
		return
	}
	if len(p.prepareVotes[blockHash]) < threshold {
		p.lock.Unlock()
		return
	}
	block, ok := p.blocks[blockHash]
	if !ok {
		p.lock.Unlock()
		return
	}
	signers := p.getSigners(p.prepareVotes, blockHash)
	sigList, ok := snapshotVoteSigs(p.prepareVoteSigs, blockHash, signers)
	if !ok {
		p.lock.Unlock()
		return
	}
	p.lock.Unlock()

	aggSig, ok := aggregateSignatures(sigList)
	if !ok {
		return
	}

	qc := QC{
		View:      block.View,
		BlockHash: blockHash,
		Phase:     phasePrepare,
		Signers:   signers,
		AggSig:    aggSig,
	}
	p.lock.Lock()
	p.highQC = &qc
	p.fastQCBroadcasted[blockHash] = true
	msg := PhaseMsg{View: block.View, BlockHash: blockHash, NodeID: p.node.nodeID, QC: qc}
	b, err := json.Marshal(msg)
	if err != nil {
		p.lock.Unlock()
		log.Panic(err)
	}
	p.lock.Unlock()

	log.Printf("[节点%s] 广播msgFastQC，区块哈希: %s", p.node.nodeID, blockHash[:16]+"...")
	p.broadcast(cmsgFastQC, b)
	p.touchProgress()
	p.handleFastQC(b, time.Now())
}

// tryBroadcastPreCommit 生成 prepare QC 并广播。
func (p *hotsutff) tryBroadcastPreCommit(blockHash string) {
	if !p.isLeader() {
		return
	}
	p.lock.Lock()
	if p.precommitBroadcasted[blockHash] {
		p.lock.Unlock()
		return
	}
	if ready, ok := p.slowPathReady[blockHash]; ok && !ready {
		p.lock.Unlock()
		return
	}
	if len(p.prepareVotes[blockHash]) < threshold {
		p.lock.Unlock()
		return
	}
	block, ok := p.blocks[blockHash]
	if !ok {
		p.lock.Unlock()
		return
	}
	signers := p.getSigners(p.prepareVotes, blockHash)
	sigList, ok := snapshotVoteSigs(p.prepareVoteSigs, blockHash, signers)
	if !ok {
		p.lock.Unlock()
		return
	}
	p.lock.Unlock()

	aggSig, ok := aggregateSignatures(sigList)
	if !ok {
		return
	}

	qc := QC{
		View:      block.View,
		BlockHash: blockHash,
		Phase:     phasePrepare,
		Signers:   signers,
		AggSig:    aggSig,
	}
	p.lock.Lock()
	p.highQC = &qc
	p.precommitBroadcasted[blockHash] = true
	msg := PhaseMsg{View: block.View, BlockHash: blockHash, NodeID: p.node.nodeID, QC: qc}
	b, err := json.Marshal(msg)
	if err != nil {
		p.lock.Unlock()
		log.Panic(err)
	}
	p.lock.Unlock()

	log.Printf("[节点%s] 广播msgPreCommit，区块哈希: %s", p.node.nodeID, blockHash[:16]+"...")
	p.broadcast(cmsgPreCommit, b)
	p.touchProgress()

	selfSig := p.signVote(phasePreCommit, block.View, blockHash)
	p.addVote(p.precommitVotes, p.precommitVoteSigs, blockHash, p.node.nodeID, selfSig)
	p.tryBroadcastCommit(blockHash)
}

// tryBroadcastCommit 生成 precommit QC 并广播。
func (p *hotsutff) tryBroadcastCommit(blockHash string) {
	if !p.isLeader() {
		return
	}
	p.lock.Lock()
	if p.commitBroadcasted[blockHash] {
		p.lock.Unlock()
		return
	}
	if p.commitQCBroadcasted[blockHash] {
		p.lock.Unlock()
		return
	}
	if len(p.precommitVotes[blockHash]) < threshold {
		p.lock.Unlock()
		return
	}
	block, ok := p.blocks[blockHash]
	if !ok {
		p.lock.Unlock()
		return
	}
	signers := p.getSigners(p.precommitVotes, blockHash)
	sigList, ok := snapshotVoteSigs(p.precommitVoteSigs, blockHash, signers)
	if !ok {
		p.lock.Unlock()
		return
	}
	p.lock.Unlock()

	aggSig, ok := aggregateSignatures(sigList)
	if !ok {
		return
	}

	qc := QC{
		View:      block.View,
		BlockHash: blockHash,
		Phase:     phasePreCommit,
		Signers:   signers,
		AggSig:    aggSig,
	}
	p.lock.Lock()
	p.highQC = &qc
	p.commitBroadcasted[blockHash] = true
	msg := PhaseMsg{View: block.View, BlockHash: blockHash, NodeID: p.node.nodeID, QC: qc}
	b, err := json.Marshal(msg)
	if err != nil {
		p.lock.Unlock()
		log.Panic(err)
	}
	p.lock.Unlock()

	log.Printf("[节点%s] 广播msgCommit，区块哈希: %s", p.node.nodeID, blockHash[:16]+"...")
	p.broadcast(cmsgCommit, b)
	p.touchProgress()

	selfSig := p.signVote(phaseCommit, block.View, blockHash)
	p.addVote(p.commitVotes, p.commitVoteSigs, blockHash, p.node.nodeID, selfSig)
	p.tryBroadcastCommitQC(blockHash)
}

// tryBroadcastCommitQC 生成 commit QC 并广播。
func (p *hotsutff) tryBroadcastCommitQC(blockHash string) {
	if !p.isLeader() {
		return
	}
	p.lock.Lock()
	if p.commitQCBroadcasted[blockHash] {
		p.lock.Unlock()
		return
	}
	if len(p.commitVotes[blockHash]) < threshold {
		p.lock.Unlock()
		return
	}
	block, ok := p.blocks[blockHash]
	if !ok {
		p.lock.Unlock()
		return
	}
	signers := p.getSigners(p.commitVotes, blockHash)
	sigList, ok := snapshotVoteSigs(p.commitVoteSigs, blockHash, signers)
	if !ok {
		p.lock.Unlock()
		return
	}
	p.lock.Unlock()

	aggSig, ok := aggregateSignatures(sigList)
	if !ok {
		return
	}

	qc := QC{
		View:      block.View,
		BlockHash: blockHash,
		Phase:     phaseCommit,
		Signers:   signers,
		AggSig:    aggSig,
	}
	p.lock.Lock()
	p.highQC = &qc
	p.commitQCBroadcasted[blockHash] = true
	msg := PhaseMsg{View: block.View, BlockHash: blockHash, NodeID: p.node.nodeID, QC: qc}
	b, err := json.Marshal(msg)
	if err != nil {
		p.lock.Unlock()
		log.Panic(err)
	}
	p.lock.Unlock()

	log.Printf("[节点%s] 广播msgCommitQC，区块哈希: %s", p.node.nodeID, blockHash[:16]+"...")
	p.broadcast(cmsgCommitQC, b)
	p.touchProgress()
	p.handleCommitQC(b, time.Now())
}


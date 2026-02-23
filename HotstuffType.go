package main

// <REQUEST,o,t,c>
type Request struct {
	Message
	Timestamp  int64
	ClientAddr string
}

type Message struct {
	Content string
	ID      int
}

type Phase string

const (
	phasePrePrepare Phase = "preprepare"
	phasePrepare    Phase = "prepare"
	phaseCommit     Phase = "commit"
)

// 教学版Fast-HotStuff的区块结构（固定leader、简化view切换、批处理）
type Block struct {
	Hash       string
	ParentHash string
	Height     int
	View       int
	Proposer   string
	CommandIDs []int
	Justify    *QC
}

// Quorum Certificate（简化：只包含签名者列表）
type QC struct {
	View      int
	BlockHash string
	Phase     Phase
	Votes     []Vote
}

// Proposal消息：leader提议新区块（携带完整请求）
type Proposal struct {
	View     int
	Block    Block
	Requests []Request
}

// Vote消息：副本对不同阶段投票
type Vote struct {
	View      int
	BlockHash string
	Phase     Phase
	NodeID    string
	Sig       []byte
}

// PhaseMsg消息：leader广播QC推进阶段
type PhaseMsg struct {
	View      int
	BlockHash string
	NodeID    string
	QC        QC
}

// NewView消息：副本超时后向主节点发送
type NewViewMsg struct {
	View   int
	NodeID string
	HighQC *QC
}

type command string

const (
	cRequest     command = "request"
	cproposal    command = "proposal"
	cnewView     command = "newView"
	cmsgPrepare  command = "msgPrepare"
	cvotePrepare command = "votePrepare"
	cmsgCommit   command = "msgCommit"
	cvoteCommit  command = "voteCommit"
)

package paxoscommm

type VoteInfo struct {
	Seq         int
	Proposevote int //提议阶段的选择
	Acceptvote  int //接受阶段的选择
}

type PaMsg interface {
	Propose(t PaMsg)
	Accept(t PaMsg)
	GetVoteInfo() VoteInfo
}

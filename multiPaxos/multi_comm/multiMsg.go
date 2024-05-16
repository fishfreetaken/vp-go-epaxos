package multicomm

import (
	paxoscommm "vp-go-epaxos/paxosCommm"
)

type MultiMsg struct {
	paxoscommm.PaCommnMsg
	//master 只有一个操作，accept和commit

}

func (m *MultiMsg) Propose(t MultiMsg) {

}

func (m *MultiMsg) Accept(t MultiMsg) {

}

func (m *MultiMsg) GetVoteInfo(t MultiMsg) paxoscommm.VoteInfo {
	return paxoscommm.VoteInfo{}
}

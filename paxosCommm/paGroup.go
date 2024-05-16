package paxoscommm

import (
	"fmt"
	"math/rand"
	"time"
)

type PaGroup struct {
	list map[int]PaNode

	recvseq  chan VoteInfo
	mpResult map[int]int //seq -> nodeid
	begin    time.Time

	ttSeq int

	decidenum   int
	nodecidenum int
	invalid     int
}

func (m *PaGroup) Sendto(idx int, st PaCommnMsg) {
	m.list[idx].Recv(st)
}

/*
func (m *PaGroup) len() int {
	return len(m.list)
}
*/

func (m *PaGroup) Index(idx int) PaNode {
	return m.list[idx]
}

func (m *PaGroup) Broadcast(st *PaCommnMsg) {
	m.Broadcastexcept(*st, -1)
}

func (m *PaGroup) randsleep() {
	t := rand.Intn(100)
	time.Sleep(time.Duration(t) * time.Millisecond)
}

func (m *PaGroup) Broadcastexcept(st PaCommnMsg, index int) {
	for i := range m.list {
		if i == index {
			continue
		}
		m.Sendto(i, st)
		//m.randsleep() //没有必要
	}
}

func (m *PaGroup) Report(seq int) {
	var rc = make(map[int][]int)
	for _, v := range m.list {
		rc[v.GetSeqMsg(seq).GetVoteInfo().AcceptVote] = append(rc[v.GetSeqMsg(seq).GetVoteInfo().AcceptVote], v.GetId())
	}

	masterIdx := -1
	for key, v := range rc {
		if len(v) >= (len(m.list)>>1 + 1) {
			if masterIdx != -1 {
				panic("invalid master index")
			}
			masterIdx = key
		}
		//fmt.Printf("vote:%d list:%+v \n", key, v)
		//fmt.Printf("msg:%+v\n", m.Index(key).GetSeqMsg(seq))
	}

	var printRc = func() {
		for key, v := range rc {
			fmt.Printf("vote:%d list:%+v \n", key, v)
			//fmt.Printf("msg:%+v\n", m.Index(key).GetSeqMsg(seq))
		}
	}

	tmplocalgp := -1
	if v, ok := m.mpResult[seq]; ok {
		tmplocalgp = v
	}
	if tmplocalgp == masterIdx && masterIdx != -1 {
		m.decidenum++
		return
	} else if tmplocalgp != masterIdx {
		m.invalid++
		fmt.Printf("not equale seq:%d localresult:%d masteridx:%d\n", seq, tmplocalgp, masterIdx)
	} else {
		fmt.Printf("seq:%d not desicde \n", seq)
		m.nodecidenum++
	}
	printRc()
	fmt.Printf("\n")
}

func (m *PaGroup) GetLastCalc() {
	fmt.Printf("total:%d:%d decide %d:%f nodecide:%d:%f invalid:%d 时间消耗：%+v\n", m.ttSeq, m.decidenum+m.nodecidenum, m.decidenum, GetPercent(m.decidenum, m.ttSeq), m.nodecidenum, GetPercent(m.nodecidenum, m.ttSeq), m.invalid, time.Since(m.begin))
}

func (m *PaGroup) InformVoteResult(seq, masterid int) {
	//看所有的seq都已经计算过了
	m.recvseq <- VoteInfo{
		Seq:        seq,
		Acceptvote: masterid,
	}
}

func (m *PaGroup) Wait(seq int) {
	t := time.NewTicker(time.Second * 30)
	fmt.Printf("begin to wait seq:%d", seq)
	for {
		select {
		case v := <-m.recvseq:
			if m.mpResult == nil {
				m.mpResult = make(map[int]int)
			}

			if hasMasterid, ok := m.mpResult[seq]; ok {
				if hasMasterid != v.Acceptvote {
					panic(fmt.Sprintf("seq:%d hasmaster:%d masterid:%d", seq, hasMasterid, v.Acceptvote))
				}
				return
			}
			m.mpResult[seq] = v.Acceptvote
			if v.Seq == seq || len(m.mpResult) == m.ttSeq {
				//检查是否完成
				//fmt.Printf("seq over:%d\n", seq)
				return
			}
		case <-t.C:
			//时间过期了
			t.Stop()
			fmt.Printf("wait expire cntvalue:%d\n", 0)
			return
		}
	}
}

func (m *PaGroup) Init(membernum int) {
	rand.New(rand.NewSource(time.Now().Unix()))
	m.begin = time.Now()

	m.recvseq = make(chan VoteInfo, 100)
}

func (m *PaGroup) AddNewMember(t PaNode) {
	if m.list == nil {
		m.list = map[int]PaNode{}
	}
	//已经存在的能否进行更改
	if _, ok := m.list[t.GetId()]; ok {
		fmt.Printf("add new member has exist:%d", t.GetId())
		//todo 关闭原来的 端口
	} else {
		m.list[t.GetId()] = t
		go t.AsyncReceive()
	}
}

func (m *PaGroup) RandNodeIndex() int {
	return int(rand.Int31()) % len(m.list)
}

func (m *PaGroup) GetNumber() int {
	return len(m.list)
}

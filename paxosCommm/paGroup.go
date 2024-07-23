package paxoscommm

import (
	"fmt"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

type PaGroup struct {
	list []*PaNode

	//通道用来接受结果
	recvseq  chan *SwapMsgVoteInfo
	mpResult map[uint64]uint32 //seq -> nodeid

	begin time.Time

	decidenum   int
	nodecidenum int
	invalid     int
}

func (m *PaGroup) Sendto(idx uint32, st *SwapMsgVoteInfo) {
	m.list[idx].Recv2(*st)
}

func (m *PaGroup) Index(idx int) *PaNode {
	return m.list[idx]
}

func (m *PaGroup) Broadcast(st *SwapMsgVoteInfo) {
	for _, v := range m.list {
		m.Sendto(v.GetId(), st)
	}
}

func (m *PaGroup) Broadcastexcept(st *SwapMsgVoteInfo) {
	for i, v := range m.list {
		if i == int(st.GetFromID()) {
			continue
		}
		m.Sendto(v.GetId(), st)
	}
}

func (m *PaGroup) ResultCheck() uint64 {
	var totalSeq uint64
	var ttcnt int
	var proposesum int
	for _, v := range m.list {
		fmt.Printf("%s\n", v.CalcLastReport())
		if atomic.LoadUint64(&v.curseq) > totalSeq {
			totalSeq = atomic.LoadUint64(&v.curseq)
		}
		proposesum += int(v.instanceid)
	}

	for i := uint64(1); i <= totalSeq; i++ {
		m.Report(i)
	}

	fmt.Printf("GetLastCalc total:%d:%d:%d sumproposenum:%d:%f mmtsdecide %d:%f nodecide:%d:%f invalid:%d 时间消耗：%+v\n", totalSeq, m.decidenum+m.nodecidenum, ttcnt, proposesum, GetPercent(int(totalSeq), proposesum), m.decidenum, GetPercent(m.decidenum, int(totalSeq)), m.nodecidenum, GetPercent(m.nodecidenum, int(totalSeq)), m.invalid, time.Since(m.begin))
	return totalSeq
}

//获取到最大的seqid，就要一个一个的check结果
func (m *PaGroup) Report(seq uint64) {
	var rc = make(map[uint32][]uint32)
	for _, v := range m.list {
		tsp := v.GetSeqMsg(seq)
		if !tsp.State.HasAccept() || tsp.State.IsFailed() {
			//没有accept就不需要统计了
			//fmt.Printf("[ERROR]Laset report seq:%d  nodeid:%d not accept:%+v\n", seq, v.GetId(), tsp)
			continue
		}
		rc[tsp.State.GetVote()] = append(rc[tsp.State.GetVote()], v.GetId())
	}

	masterIdx := uint32(math.MaxUint32)
	validnum := len(m.list)>>1 + 1
	for key, v := range rc {
		if key == 0 {
			panic(func() string {
				at := fmt.Sprintf("invalid key seq:%d v:%+v:\n", seq, v)
				for _, idex := range v {
					at += fmt.Sprintf("%+v\n", m.list[idex].GetSeqMsg(seq))
				}
				return at
			}())
		}
		if len(v) >= validnum {
			if masterIdx != math.MaxUint32 {
				panic("invalid master index")
			}
			masterIdx = key
		}
	}

	var printRc = func() {
		for key, v := range rc {
			fmt.Printf("vote:%d list:%+v \n", key, v)
		}

		//把手下每一个有问题的msg都打印出来，我就不信了
		for idx := range m.list {
			fmt.Printf("sub idx:%d msg:%+v\n", idx, m.list[idx].GetSeqMsg(seq))
		}
	}

	tmplocalgp := uint32(math.MaxUint32)
	if v, ok := m.mpResult[seq]; ok {
		tmplocalgp = v
	}
	if tmplocalgp == masterIdx && masterIdx != math.MaxUint32 {
		m.decidenum++
		//fmt.Printf("Last Report seq:%d decideid:%d\n", seq, masterIdx)
		return
	} else if tmplocalgp != masterIdx {
		m.invalid++
		if masterIdx == math.MaxUint32 {
			fmt.Printf("1 node not equal seq:%d localresult:%d masteridx:%d \n", seq, tmplocalgp, masterIdx)
		} else {
			fmt.Printf("2 node not equal seq:%d localresult:%d masteridx:%d msg:%+v\n", seq, tmplocalgp, masterIdx, m.list[masterIdx].GetSeqMsg(seq))
		}
	} else {
		fmt.Printf("seq:%d not descide \n", seq)
		m.nodecidenum++
	}
	printRc()
	fmt.Printf("\n")
}

func (m *PaGroup) InformVoteResult(t *SwapMsgVoteInfo) {
	//看所有的seq都已经计算过了
	m.recvseq <- t
}

//总的master来统计各个提交的数据是否有冲突
func (m *PaGroup) AsyncWaitResult() {
	for v := range m.recvseq {
		//fmt.Printf("AsyncWaitResult seq:%d acceptinfo:%+v\n", v.Seq, v)
		if hasMasterid, ok := m.mpResult[v.Seq]; ok {
			if hasMasterid != v.State.GetVote() {
				panic(func() string {
					sa := fmt.Sprintf("seq:%d hasmaster:%d masterid:%+v \n", v.Seq, hasMasterid, v)
					if hasMasterid != math.MaxUint32 {
						sa += fmt.Sprintf("hasMasterid :%d %+v\n", hasMasterid, m.list[hasMasterid].GetSeqMsg(v.Seq))
					}
					sa += fmt.Sprintf("fromid :%d %+v\n", v.State.GetVote(), m.list[v.GetFromID()].GetSeqMsg(v.Seq))
					return sa
				}())
				//panic(fmt.Sprintf("seq:%d hasmaster:%d masterid:%+v :\n %+v \n%+v", v.Seq, hasMasterid, v, m.list[hasMasterid].GetSeqMsg(v.Seq), m.list[v.AcceptVote].GetSeqMsg(v.Seq)))
			}
		} else {
			m.mpResult[v.Seq] = v.State.GetVote()
		}
	}
}

func (m *PaGroup) WaitForNode() {
	//这里应该已经没有新的任务来提交了
	var wg sync.WaitGroup
	for _, v := range m.list {
		v.AsyncWork(&wg)
	}
	wg.Wait()
}

func (m *PaGroup) Init(membernum uint32) {
	rand.New(rand.NewSource(time.Now().Unix()))
	m.begin = time.Now()
	for i := uint32(0); i < membernum; i++ {
		m.list = append(m.list, &PaNode{
			id:       i,
			priority: i,
		})
		m.list[i].SetVecLkNums(m, membernum)
	}
	m.recvseq = make(chan *SwapMsgVoteInfo, 1500)
	m.mpResult = make(map[uint64]uint32)

	//异步统计所有的结果的通知
	go m.AsyncWaitResult()
}

func (m *PaGroup) RandNodeIndex() int {
	return int(rand.Int31()) % len(m.list)
}

func (m *PaGroup) GetNumber() uint32 {
	return uint32(len(m.list))
}

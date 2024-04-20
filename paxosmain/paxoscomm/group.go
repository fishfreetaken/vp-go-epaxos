package paxoscomm

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

type Group struct {
	list        []*Pnode
	lk          sync.Mutex
	mpResult    map[int]int //seq -> nodeid
	decidenum   int
	nodecidenum int
	invalid     int
	seq         int
	recvseq     chan int
	begin       time.Time
	cntcc       int32

	Totalseq int
}

func (m *Group) RefreshSeq(seq int) {
	if m.seq < seq {
		m.seq = seq
	}
}
func (m *Group) send(idx int, st pmsg) {
	m.list[idx].recv <- st
}

func (m *Group) len() int {
	return len(m.list)
}

func (m *Group) Index(idx int) *Pnode {
	return m.list[idx]
}

func (m *Group) broadcast(st pmsg) {
	for i := range m.list {
		//m.lk.Lock()
		m.list[i].recv <- st
		//m.lk.Unlock()
		m.randsleep(st.from, i)
	}
}

func (m *Group) broadcastexcept(st pmsg, index int) {
	for i := range m.list {
		if i == index {
			continue
		}
		//m.lk.Lock()
		m.list[i].recv <- st
		//m.lk.Unlock()
		m.randsleep(st.from, i)
	}
}

func (m *Group) randsleep(from, to int) {
	t := rand.Intn(100)
	time.Sleep(time.Duration(t) * time.Millisecond)
	//fmt.Printf("randsleep from:%d to :%d sleep t:%d millisecond\n", from, to, t)
}

func (m *Group) Report(seq int) {
	var rc = make(map[int][]int)
	var pr = make(map[int][]int)
	for _, v := range m.list {
		rc[v.getmsg(seq).vote] = append(rc[v.getmsg(seq).vote], v.id)
		pr[v.getmsg(seq).proposevote] = append(pr[v.getmsg(seq).proposevote], v.id)
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
		//fmt.Printf("msg:%+v\n", m.Index(key).getmsg(seq))
	}

	var printRc = func() {
		for key, v := range rc {
			fmt.Printf("vote:%d list:%+v \n", key, v)
			//fmt.Printf("msg:%+v\n", m.Index(key).getmsg(seq))
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
	return
	//if masterIdx == -1 {

	for key, v := range pr {
		fmt.Printf("propose vote:%d list:%+v \n", key, v)
		if key == -1 {
			continue
		}
		//fmt.Printf("msg:%+v\n", m.Index(key).getmsg(seq))
	}

	//}
	fmt.Printf("seq:%d Last Master:%d  calresult:%d  \n\n", seq, masterIdx, tmplocalgp)
	//todo broadcast
}

func GetPercent(a, b int) float32 {
	return float32(a*100) / float32(b)
}

func (m *Group) GetLastCalc() {
	fmt.Printf("total:%d:%d decide %d:%f nodecide:%d:%f invalid:%d 时间消耗：%+v\n", m.seq, m.decidenum+m.nodecidenum, m.decidenum, GetPercent(m.decidenum, m.seq), m.nodecidenum, GetPercent(m.nodecidenum, m.seq), m.invalid, time.Since(m.begin))
}

func (m *Group) calcVoteResult(seq, masterid int) {
	m.lk.Lock()
	defer m.lk.Unlock()

	if m.mpResult == nil {
		m.mpResult = make(map[int]int)
	}

	if hasMasterid, ok := m.mpResult[seq]; ok {
		if hasMasterid != masterid {
			panic(fmt.Sprintf("seq:%d hasmaster:%d masterid:%d", seq, hasMasterid, masterid))
		}
		return
	}
	m.mpResult[seq] = masterid
	atomic.AddInt32(&m.cntcc, 1)

	//看所有的seq都已经计算过了
	m.recvseq <- seq
}

func (m *Group) Wait(seq int) {
	t := time.NewTicker(time.Second * 30)
	fmt.Printf("begin to wait seq:%d", seq)
	for {
		select {
		case v := <-m.recvseq:
			cntvalue := atomic.LoadInt32(&m.cntcc)
			if v == seq || cntvalue == int32(m.Totalseq) {
				//检查是否完成
				//fmt.Printf("seq over:%d\n", seq)
				return
			}
		case <-t.C:
			//时间过期了
			t.Stop()
			cntvalue := atomic.LoadInt32(&m.cntcc)
			fmt.Printf("wait expire cntvalue:%d\n", cntvalue)
			return
		}
	}
}

func (m *Group) Init(k int) {
	if m.Totalseq == 0 {
		panic("null total seq")
	}
	rand.New(rand.NewSource(time.Now().Unix()))
	m.begin = time.Now()

	m.recvseq = make(chan int, 100)

	for i := 0; i < k; i++ {
		tmp := &Pnode{
			id:   i,
			recv: make(chan pmsg, m.Totalseq),
			g:    m,
		}
		//fmt.Printf("master i:%d m.Totalseq：%d recv:%d \n", i, m.Totalseq, cap(tmp.recv))
		m.list = append(m.list, tmp)
		go tmp.receive()
	}
}

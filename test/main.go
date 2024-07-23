package main

import (
	"fmt"
	"net/http"
	"net/http/pprof"
	"sync/atomic"
	"time"
	paxoscommm "vp-go-epaxos/paxosCommm"
)

func SetupPProf() {
	r := http.NewServeMux()
	r.HandleFunc("/debug/pprof/", pprof.Index)
	r.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	r.HandleFunc("/debug/pprof/profile", pprof.Profile)
	r.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	r.HandleFunc("/debug/pprof/trace", pprof.Trace)

	if err := http.ListenAndServe(":9909", r); err != nil {
		fmt.Printf("Err: http.ListenAndServe err; Info: err=%v\n", err)
	}
}

func NewClientReq() *paxoscommm.ClientReq {
	return &paxoscommm.ClientReq{
		RetryTimes: 3,
	}
}

func main() {
	go SetupPProf()
	seqNum := 500000
	nodenumber := uint32(11)
	/*
		f, _ := os.Create("myTrace.dat")
		defer f.Close()
		_ = trace.Start(f)
		defer trace.Stop()
	*/
	//GoTest(nodenumber, seqNum)
	var gClose = make(chan int)

	gt := time.NewTicker(time.Second * 300)

	var vctlist = make([]*paxoscommm.ClientReq, nodenumber)
	var gSeqNum int32
	var chKillTerminal int32
	var g paxoscommm.PaGroup
	//return
	go func() {
		g.Init(nodenumber)
		for i := 0; i < seqNum; i++ {
			if chKillTerminal > 0 {
				fmt.Printf("Terminal i:%d\n", i)
				atomic.AddInt32(&chKillTerminal, 1)
				return
			}
			gSeqNum = int32(i)

			g.Index(5).BeginNewCommit(NewClientReq())
			g.Index(8).BeginNewCommit(NewClientReq())
			g.Index(2).BeginNewCommit(NewClientReq())
			g.Index(9).BeginNewCommit(NewClientReq())
			//g.Index(1).BeginNewCommit(NewClientReq())
		}
		fmt.Printf("wait 1 \n")
		g.WaitForNode()
		fmt.Printf("wait 2 \n")

		//time.Sleep(time.Millisecond * 500)
		//每一个seq值都需要check一下最后的结果

		g.ResultCheck()
		fmt.Printf("wait 3 \n")
		gClose <- 1
	}()

	select {
	case <-gClose:
		fmt.Printf("last close \n")
	case <-gt.C:
		atomic.AddInt32(&chKillTerminal, 1)
		//让异步进程里的所有活动全部结束
		time.Sleep(time.Second)
		fmt.Printf("last timeout last gseqnum:%d terminal kill:%d\n", atomic.LoadInt32(&gSeqNum), atomic.LoadInt32(&chKillTerminal))
		for idx, v := range vctlist {
			if v == nil {
				continue
			}
			fmt.Printf("cur idx:%d value:%+v \n", idx, v)
		}
		gt.Stop()
	}
}

package main

import (
	"fmt"
	"net/http"
	"net/http/pprof"
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

func main() {
	go SetupPProf()
	seqNum := 400000
	nodenumber := 11
	/*
		f, _ := os.Create("myTrace.dat")
		defer f.Close()
		_ = trace.Start(f)
		defer trace.Stop()
	*/
	//GoTest(nodenumber, seqNum)
	var gClose = make(chan int)

	gt := time.NewTicker(time.Second * 120)
	//return
	go func() {
		var g paxoscommm.PaGroup
		g.Init(nodenumber)

		for i := 0; i < seqNum; i++ {
			g.Index(5).BeginNewCommit(&paxoscommm.ClientReq{})
			g.Index(8).BeginNewCommit(&paxoscommm.ClientReq{})
			g.Index(2).BeginNewCommit(&paxoscommm.ClientReq{})
			g.Index(9).BeginNewCommit(&paxoscommm.ClientReq{})
			g.Index(1).BeginNewCommit(&paxoscommm.ClientReq{})
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
		fmt.Printf("last timeout \n")
		gt.Stop()
	}
}

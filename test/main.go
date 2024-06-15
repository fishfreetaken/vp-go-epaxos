package main

import (
	"fmt"
	"net/http"
	"net/http/pprof"
	"sync"
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
	seqNum := 10000
	nodenumber := 11

	//GoTest(nodenumber, seqNum)

	//return

	var g paxoscommm.PaGroup
	g.Init(nodenumber)

	var wg sync.WaitGroup
	for i := 0; i < seqNum; i++ {
		wg.Add(5)
		go func() {
			g.Index(5).NewProPoseMsg(nil, 0)
			wg.Done()
		}()
		go func() {
			g.Index(8).NewProPoseMsg(nil, 0)
			wg.Done()
		}()

		go func() {
			g.Index(2).NewProPoseMsg(nil, 0)
			wg.Done()
		}()

		go func() {
			g.Index(9).NewProPoseMsg(nil, 0)
			wg.Done()
		}()

		go func() {
			g.Index(1).NewProPoseMsg(nil, 0)
			wg.Done()
		}()
	}
	wg.Wait()

	g.WaitForNode()

	//time.Sleep(time.Millisecond * 500)
	//每一个seq值都需要check一下最后的结果

	g.ResultCheck()
}

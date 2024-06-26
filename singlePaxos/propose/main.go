package main

import (
	"fmt"
	"net/http"
	"net/http/pprof"
	"os"
	"strconv"
	"vp-go-epaxos/paxosmain/paxoscomm"
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
	seqnum := 10000

	var iwateSec int

	if len(os.Args) > 1 {
		if len(os.Args[1]) > 0 {
			iseq, _ := strconv.ParseInt(os.Args[1], 10, 64)
			seqnum = int(iseq)
			fmt.Printf("input update seqnum:%d\n", seqnum)
		}
		if len(os.Args) > 2 {
			iwaitenume, _ := strconv.ParseInt(os.Args[2], 10, 64)
			iwateSec = int(iwaitenume)
			fmt.Printf("waite waite:%d\n", iwateSec)
		}
	}

	var g = paxoscomm.Group{
		Totalseq: seqnum,
	}

	g.Init(11)

	go SetupPProf()

	for i := 0; i < seqnum; i++ {
		beginnum := g.RandNodeIndex() //随机出数量和随机的节点
		if beginnum == 0 {
			beginnum = 1
		}
		/*
			go func(seq int) {
				go g.Index(1).ToBeProposemaster(seq, -1)
				go g.Index(4).ToBeProposemaster(seq, -1)
				go g.Index(7).ToBeProposemaster(seq, -1)
				go g.Index(10).ToBeProposemaster(seq, -1)
				go g.Index(2).ToBeProposemaster(seq, -1)
			}(i)*/
		var st = make(map[int]int)
		go func(seq int) {
			for k := 0; k < beginnum; k++ {
				index := g.RandNodeIndex()
				if _, ok := st[index]; ok {
					continue
				}
				st[index] = 1
				go g.Index(index).ToBeProposemaster(seq, -1)
			}
		}(i)
		g.RefreshSeq(i + 1)
	}

	g.Wait(-1, iwateSec)
	//time.Sleep(time.Second * 3)
	for i := 0; i < seqnum; i++ {
		g.Report(i)
	}
	g.GetLastCalc()
}

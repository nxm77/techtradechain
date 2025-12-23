/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cmd

import (
	"encoding/csv"
	"fmt"
	"net/http"

	_ "net/http/pprof"
	"os"
	"os/signal"
	"path"
	"runtime"
	"syscall"
	"time"

	"techtradechain.com/techtradechain-go/module/blockchain"
	"techtradechain.com/techtradechain-go/module/monitor"
	"techtradechain.com/techtradechain-go/module/rpcserver"
	"techtradechain.com/techtradechain/localconf/v2"
	"techtradechain.com/techtradechain/logger/v2"
	"code.cloudfoundry.org/bytefmt"
	"github.com/spf13/cobra"
)

var log = logger.GetLogger(logger.MODULE_CLI)

func StartCMD() *cobra.Command {
	startCmd := &cobra.Command{
		Use:   "start",
		Short: "Startup TechTradeChain",
		Long:  "Startup TechTradeChain",
		RunE: func(cmd *cobra.Command, _ []string) error {
			initLocalConfig(cmd)
			mainStart()
			fmt.Println("TechTradeChain exit")
			return nil
		},
	}
	attachFlags(startCmd, []string{flagNameOfConfigFilepath})
	return startCmd
}

func mainStart() {
	if localconf.TechTradeChainConfig.DebugConfig.IsTraceMemoryUsage {
		traceMemoryUsage()
	}

	// init techtradechain server
	techTradeChainServer := blockchain.NewTechTradeChainServer()
	if err := techTradeChainServer.Init(); err != nil {
		log.Errorf("techtradechain server init failed, %s", err.Error())
		return
	}

	// init rpc server
	rpcServer, err := rpcserver.NewRPCServer(techTradeChainServer)
	if err != nil {
		log.Errorf("rpc server init failed, %s", err.Error())
		return
	}

	// init monitor server
	monitorServer := monitor.NewMonitorServer()

	//// p2p callback to validate
	//txpool.RegisterCallback(rpcServer.Gateway().Invoke)

	// new an error channel to receive errors
	errorC := make(chan error, 1)

	// handle exit signal in separate go routines
	go handleExitSignal(errorC)

	// start blockchains in separate go routines
	if err := techTradeChainServer.Start(); err != nil {
		log.Errorf("techtradechain server startup failed, %s", err.Error())
		return
	}

	// start rpc server and listen in another go routine
	if err := rpcServer.Start(); err != nil {
		errorC <- err
	}

	// start monitor server and listen in another go routine
	if err := monitorServer.Start(); err != nil {
		errorC <- err
	}

	if localconf.TechTradeChainConfig.PProfConfig.Enabled {
		startPProf()
	}

	printLogo()

	// listen error signal in main function
	errC := <-errorC
	if errC != nil {
		log.Error("techtradechain encounters error ", errC)
	}
	log.Info("Stopping RPCServer... ")
	rpcServer.Stop()
	log.Info("Stopping TechTradeChain server... ")
	techTradeChainServer.Stop()
	log.Info("All is stopped!")

}

func handleExitSignal(exitC chan<- error) {

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGTERM, os.Interrupt, syscall.SIGINT)
	defer signal.Stop(signalChan)

	for sig := range signalChan {
		log.Infof("received signal: %d (%s)", sig, sig)
		exitC <- nil
	}
}

func printLogo() {
	log.Infof(logo())
}

func startPProf() {
	go func() {
		addr := fmt.Sprintf(":%d", localconf.TechTradeChainConfig.PProfConfig.Port)
		log.Infof("pprof start at [%s]", addr)
		server := &http.Server{
			Addr:         addr,
			ReadTimeout:  60 * time.Second,
			WriteTimeout: 60 * time.Second,
			IdleTimeout:  60 * time.Second,
		}
		if err := server.ListenAndServe(); err != nil {
			fmt.Println(err)
		}
	}()
}

func traceMemoryUsage() {
	go func() {
		p := path.Join(path.Dir(localconf.TechTradeChainConfig.LogConfig.SystemLog.FilePath), "mem.csv")
		f, err := os.OpenFile(p, os.O_RDWR|os.O_CREATE, 0755)
		if err != nil {
			panic(err)
		}
		w := csv.NewWriter(f)
		err = w.Write([]string{
			"Alloc", "TotalAlloc", "Sys", "Mallocs", "Frees", "HeapAlloc", "HeapSys",
			"HeapIdle", "HeapInuse", "HeapReleased", "HeapObjects", "StackInuse",
			"StackSys", "MSpanInuse", "MSpanSys", "MCacheInuse", "MCacheSys",
			"BuckHashSys", "GCSys", "OtherSys",
		})
		if err != nil {
			panic(err)
		}
		for range time.Tick(time.Second) {
			mem := new(runtime.MemStats)
			runtime.ReadMemStats(mem)
			err = w.Write([]string{
				bytefmt.ByteSize(mem.Alloc),
				bytefmt.ByteSize(mem.TotalAlloc),
				bytefmt.ByteSize(mem.Sys),
				bytefmt.ByteSize(mem.Mallocs),
				bytefmt.ByteSize(mem.Frees),
				bytefmt.ByteSize(mem.HeapAlloc),
				bytefmt.ByteSize(mem.HeapSys),
				bytefmt.ByteSize(mem.HeapIdle),
				bytefmt.ByteSize(mem.HeapInuse),
				bytefmt.ByteSize(mem.HeapReleased),
				bytefmt.ByteSize(mem.HeapObjects),
				bytefmt.ByteSize(mem.StackInuse),
				bytefmt.ByteSize(mem.StackSys),
				bytefmt.ByteSize(mem.MSpanInuse),
				bytefmt.ByteSize(mem.MSpanSys),
				bytefmt.ByteSize(mem.MCacheInuse),
				bytefmt.ByteSize(mem.MCacheSys),
				bytefmt.ByteSize(mem.BuckHashSys),
				bytefmt.ByteSize(mem.GCSys),
				bytefmt.ByteSize(mem.OtherSys),
			})
			if err != nil {
				panic(err)
			}
			w.Flush()
		}
	}()
}

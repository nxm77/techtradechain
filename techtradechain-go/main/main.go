/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package main

import (
	"fmt"

	"techtradechain.com/techtradechain-go/main/cmd"
	"github.com/spf13/cobra"
)

// ./techtradechain start -c ../config/wx-org1-solo/techtradechain.yml
func main() {
	mainCmd := &cobra.Command{Use: "techtradechain"}
	mainCmd.AddCommand(cmd.StartCMD())
	mainCmd.AddCommand(cmd.VersionCMD())
	mainCmd.AddCommand(cmd.ConfigCMD())
	mainCmd.AddCommand(cmd.RebuildDbsCMD())

	err := mainCmd.Execute()
	if err != nil {
		fmt.Println(err)
	}
}

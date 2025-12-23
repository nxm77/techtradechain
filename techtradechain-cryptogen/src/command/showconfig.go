/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package command

import (
	"fmt"
	"io/ioutil"

	"github.com/spf13/cobra"
)

func ShowConfigCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "showconfig",
		Short: "Show config",
		Long:  "Show config",
		RunE: func(cmd *cobra.Command, args []string) error {
			return showConfig()
		},
	}
}

func showConfig() error {
	data, err := ioutil.ReadFile(ConfigPath)
	if err != nil {
		return err
	}
	fmt.Println(string(data))
	return nil
}

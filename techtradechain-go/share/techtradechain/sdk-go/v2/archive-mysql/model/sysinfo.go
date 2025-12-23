// Copyright (C) BABEC. All rights reserved.
// Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

package model

import (
	"strconv"

	"gorm.io/gorm"
)

const (
	// KArchivedblockheight archived_block_height
	KArchivedblockheight = "archived_block_height"
)

// Sysinfo define system info db model
type Sysinfo struct {
	BaseModel
	K string `gorm:"unique;type:varchar(64) NOT NULL"`
	V string `gorm:"type:varchar(8000) NOT NULL"`
}

// GetArchivedBlockHeight get archived block height from db
func GetArchivedBlockHeight(db *gorm.DB) (uint64, error) {
	var sysinfo Sysinfo
	err := db.First(&sysinfo, "k = ?", KArchivedblockheight).Error
	if err != nil {
		return 0, err
	}

	return strconv.ParseUint(sysinfo.V, 10, 64)
}

// UpdateArchivedBlockHeight update archived block height in db
func UpdateArchivedBlockHeight(db *gorm.DB, archivedBlockHeight uint64) error {
	return db.Model(&Sysinfo{}).Where("k = ?", KArchivedblockheight).
		Update("v", archivedBlockHeight).Error
}

// InitArchiveStatusData init archive status data
// @param db
// @return error
func InitArchiveStatusData(db *gorm.DB) error {
	return db.Create(&Sysinfo{
		K: KArchivedblockheight,
		V: "0",
	}).Error
}

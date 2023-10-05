package clients

import (
	"context"
	"fmt"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/chain/types/ethtypes"
	"github.com/jinzhu/gorm"
	"strings"
	"time"
)

func NewLatestChainHeightStore(db *gorm.DB) *LatestChainHeightStore {
	return &LatestChainHeightStore{
		db: db,
	}
}

type LatestChainHeightStore struct {
	db *gorm.DB
}

func (s *LatestChainHeightStore) Close() error {
	return s.db.Close()
}

func (s *LatestChainHeightStore) Get() (abi.ChainEpoch, error) {
	var height int64
	err := s.db.DB().QueryRow("SELECT latest_chain_height FROM fetch_history ORDER BY latest_chain_height LIMIT 1").Scan(&height)
	return abi.ChainEpoch(height), err
}

func (s *LatestChainHeightStore) Set(ctx context.Context, height abi.ChainEpoch) error {
	_, err := s.db.DB().Exec("INSERT INTO fetch_history (`latest_chain_height`)VALUES(?)", int64(height))
	if strings.Contains(err.Error(), "DUPLICATE") {
		return nil
	}
	return err
}

type EthLogStore struct {
	db *gorm.DB
}

func NewEthLogStore(db *gorm.DB) *EthLogStore {
	return &EthLogStore{
		db: db,
	}
}

type CalculateSafepledgeLog struct {
	Address         string
	Data            string
	TransactionHash string
	UpdateMsgHash   string
}

func (s *EthLogStore) SaveLogs(ctx context.Context, logs []ethtypes.EthLog) error {
	d := []string{}
	for _, log := range logs {
		d = append(d, fmt.Sprintf("(%v, %v, %v, 'pending')", log.Address.String(), log.Data.String(), log.TransactionHash.String()))
	}
	_, err := s.db.DB().Exec("INSERT INTO calculate_safepledge_log (`address`, `data`, `transaction_hash`, `status`)VALUES ?", strings.Join(d, ","))
	if err != nil {
		return err
	}
	return nil
}

func (s *EthLogStore) LogProcessed(ctx context.Context, address ethtypes.EthAddress, logHash ethtypes.EthHash, updateMsgHash ethtypes.EthHash) error {
	_, err := s.db.DB().Exec("UPDATE calculate_safepledge_log SET `status`='finished', `update_msg_hash`=? WHERE `transaction_hash`=? AND `address`=?",
		updateMsgHash.String(), logHash.String(), address.String())
	return err
}

func (s *EthLogStore) LogsPending(ctx context.Context, createdBefore time.Time) ([]ethtypes.EthLog, error) {
	rows, err := s.db.DB().Query("SELECT `address`, transaction_hash`, `data` FROM `calculate_safepledge_log` WHERE `status`='pending' AND created_time<?", createdBefore.Unix())
	if err != nil {
		return nil, err
	}
	logs := []ethtypes.EthLog{}
	for rows.Next() {
		var log ethtypes.EthLog
		err = rows.Scan(&log)
		if err != nil {
			return logs, err
		}
		logs = append(logs, log)
	}
	return logs, nil
}

type LatestConsumeOffsetStore struct {
	db *gorm.DB
}

func NewLatestConsumeOffsetStore(db *gorm.DB) *LatestConsumeOffsetStore {
	return &LatestConsumeOffsetStore{
		db: db,
	}
}

func (s *LatestConsumeOffsetStore) GetLatestConsumeOffset(ctx context.Context, partition int32) (int64, error) {
	type offset struct {
		Offset int64
	}
	var o offset
	err := s.db.First(&o, "partition = ?", partition).Error
	return o.Offset, err
}
func (s *LatestConsumeOffsetStore) SetLatestConsumeOffset(ctx context.Context, partition int32, offset int64) error {
	_, err := s.db.DB().Exec("UPDATE `latest_consume_offset` SET `offset`=? WHERE `partition`=?", offset, partition)
	return err
}

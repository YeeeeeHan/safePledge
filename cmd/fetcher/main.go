package fetcher

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/filecoin-project/lotus/chain/types/ethtypes"
	"github.com/filecoin-project/lotus/itests/kit"
	"github.com/jinzhu/gorm"
	"go_filecoin/clients"
	"io"
	"time"

	jsonrpc "github.com/filecoin-project/go-jsonrpc"
	"github.com/filecoin-project/go-state-types/abi"
	lotusapi "github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/chain/types"
)

// Declare integer constant
const (
	ADDR                            = "filecoin.chainup.net"
	NUM_EPOCH_IN_DAY                = 2880
	LIFETIME_CAP                    = 140
	TERMINATION_REWARD_FACTOR_DENOM = 2
)

func main() {
	config, err := ParseConfig()
	if err != nil {
		panic(err)
	}
	db, err := gorm.Open("pg", config.DBAddr)
	if err != nil {
		panic(err)
	}
	var api lotusapi.FullNodeStruct
	closer, err := jsonrpc.NewMergeClient(context.Background(), "wss://"+ADDR+"/rpc/v1", "Filecoin", []interface{}{&api.Internal, &api.CommonStruct.Internal}, nil, jsonrpc.WithTimeout(120*time.Second))
	if err != nil {
		panic(err)
	}
	defer closer()
	latestChainHeightStore := clients.NewLatestChainHeightStore(db)
	ethLogStorer := clients.NewEthLogStore(db)
	ethLogsBroadcaster, err := clients.NewKafkaProducer(config.KafkaAddrs, config.KafkaTopic)
	if err != nil {
		panic(err)
	}
	fetcher := NewFetcher(config, api, latestChainHeightStore, ethLogStorer, ethLogsBroadcaster)
	go fetcher.Start()

	c := make(chan int)
	<-c
}

func ParseConfig() (*FetcherConfig, error) {
	// TODO
	return nil, nil
}

type LatestChainHeightStore interface {
	Get() (abi.ChainEpoch, error)
	Set(ctx context.Context, height abi.ChainEpoch) error
	io.Closer
}

type EthLogStorer interface {
	SaveLogs(ctx context.Context, logs []ethtypes.EthLog) error
	LogsPending(ctx context.Context, createdBefore time.Time) ([]ethtypes.EthLog, error)
}

type Fetcher struct {
	config            *FetcherConfig
	api               lotusapi.FullNodeStruct
	apiCloser         jsonrpc.ClientCloser
	heightStore       LatestChainHeightStore
	logStorer         EthLogStorer
	taskChan          chan ethtypes.EthLog
	contractTopicHash ethtypes.EthHash
	logBroadcaster    *clients.KafkaProducer
}

type FetcherConfig struct {
	DBAddr                   string
	CalculateSafePledgeTopic string
	KafkaAddrs               []string
	KafkaTopic               string
}

func NewFetcher(c *FetcherConfig, api lotusapi.FullNodeStruct, heightStore LatestChainHeightStore, logStore EthLogStorer, kafkaProducer *clients.KafkaProducer) *Fetcher {
	return &Fetcher{
		config:            c,
		api:               api,
		heightStore:       heightStore,
		logStorer:         logStore,
		contractTopicHash: kit.EthTopicHash(c.CalculateSafePledgeTopic),
		taskChan:          make(chan ethtypes.EthLog, 1024),
		logBroadcaster:    kafkaProducer,
	}
}

func (f *Fetcher) Stop() error {
	return f.heightStore.Close()
}

func (f *Fetcher) Start() {
	go f.broadcastTask()
	go f.RetryTasks()
	var height abi.ChainEpoch
	var filterSpec = ethtypes.EthFilterSpec{
		Topics: []ethtypes.EthHashList{
			[]ethtypes.EthHash{
				f.contractTopicHash,
			},
		},
		BlockHash: nil,
	}
	ticker := time.NewTicker(time.Second * 3)
	for range ticker.C {
		storedHeight, err := f.heightStore.Get()
		if err != nil {
			fmt.Println("Failed to get latest chain height: ", err)
			continue
		}
		currentHeight := storedHeight + 1
		ts, err := f.api.ChainGetTipSetByHeight(context.Background(), currentHeight, types.EmptyTSK)
		if err != nil {
			fmt.Println("Error found when getting tipset by height: ", err)
			continue
		}
		key := ts.Key().String()
		filterSpec.FromBlock = &key
		filterSpec.ToBlock = &key
		result, err := f.api.EthGetLogs(context.Background(), &filterSpec)
		if err != nil {
			fmt.Printf("Error found when downloading logs at height: %v, error: %v\n", height, err)
			continue
		}
		logs := []ethtypes.EthLog{}
		logsBytes, err := json.Marshal(result.Results)
		if err != nil {
			fmt.Printf("Unexpected data found returned by EthGetLogs, err: %v, data: %v\n", err, result.Results)
			continue
		}
		err = json.Unmarshal(logsBytes, &logs)
		if err != nil {
			fmt.Printf("Unexpected data found returned by EthGetLogs, err: %v, data: %v\n", err, result.Results)
			continue
		}
		err = f.logStorer.SaveLogs(context.Background(), logs)
		if err != nil {
			fmt.Printf("Failed to save eth logs into db, err: %v\n", err)
			continue
		}
		err = f.heightStore.Set(context.Background(), currentHeight)
		if err != nil {
			fmt.Printf("Failed to save latest chain height %v into db, err: %v\n", currentHeight, err)
			continue
		}
		for _, log := range logs {
			f.taskChan <- log
		}
	}
}

func (f *Fetcher) broadcastTask() {
	for log := range f.taskChan {
		err := f.logBroadcaster.Send(context.Background(), log.TransactionHash.String(), log)
		if err != nil {
			fmt.Printf("Failed to send task to kafka, err: %v\n", err)
		}
	}
}

func (f *Fetcher) RetryTasks() {
	ticker := time.NewTicker(time.Second * 5)
	for range ticker.C {
		logs, err := f.logStorer.LogsPending(context.Background(), time.Now().Add(-time.Second*5))
		if err != nil {
			fmt.Printf("Failed to load pending logs from db. err: %v", err)
			continue
		}
		for _, log := range logs {
			f.taskChan <- log
		}
	}
}

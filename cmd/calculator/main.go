package calculator

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/filecoin-project/lotus/chain/types/ethtypes"
	"github.com/jinzhu/gorm"
	"go_filecoin/clients"
	"log"
	"sort"
	"time"

	"github.com/filecoin-project/go-address"
	jsonrpc "github.com/filecoin-project/go-jsonrpc"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/builtin/v8/miner"
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
		log.Fatalf("connecting with lotus failed: %s", err)
	}
	defer closer()
	store := clients.NewLatestConsumeOffsetStore(db)
	consumer, err := clients.NewKafkaConsumer(config.KafkaAddrs, config.KafkaTopic, store)
	ethLogStorer := clients.NewEthLogStore(db)
	fetcher := NewCalculator(config, api, consumer, ethLogStorer)
	go fetcher.Start()

	c := make(chan int)
	<-c
}

func ParseConfig() (*CalculatorConfig, error) {
	// TODO
	return nil, nil
}

type LogProcessedMarker interface {
	LogProcessed(ctx context.Context, address ethtypes.EthAddress, logHash ethtypes.EthHash, updateMsgHash ethtypes.EthHash) error
}

type Calculator struct {
	config             *CalculatorConfig
	api                lotusapi.FullNodeStruct
	apiCloser          jsonrpc.ClientCloser
	consumer           *clients.KafkaConsumer
	logProcessedMarker LogProcessedMarker
}

type CalculatorConfig struct {
	DBAddr                   string
	CalculateSafePledgeTopic string
	KafkaAddrs               []string
	KafkaTopic               string
}

func NewCalculator(c *CalculatorConfig, api lotusapi.FullNodeStruct, consumer *clients.KafkaConsumer, logProcessedMarker LogProcessedMarker) *Calculator {
	return &Calculator{
		config:             c,
		api:                api,
		logProcessedMarker: logProcessedMarker,
		consumer:           consumer,
	}
}

func (c *Calculator) Stop() error {
	return nil
}

type LogBody struct {
	MinerID string
}

func (c *Calculator) Start() {
	for {
		msg := c.consumer.Next()
		_ = c.consumer.Commit()
		if msg == nil { // TODO
			continue
		}
		ethLog := &ethtypes.EthLog{}
		err := json.Unmarshal(msg.Value, ethLog)
		if err != nil {
			fmt.Printf("Unexpected message found in kafka mq: %v\n", msg)
			continue
		}
		logBody := &LogBody{}
		err = json.Unmarshal(ethLog.Data, logBody)
		if err != nil {
			fmt.Printf("Unexpected message found in kafka mq: %v\n", msg)
			continue
		}
		safePledge, err := c.calculateSafePledge(logBody.MinerID)
		if err != nil {
			fmt.Printf("Failed to calculate safe pledge of miner: %v\n", logBody.MinerID)
			continue
		}
		msgHash, err := c.UpdateContractSafePledge(context.Background(), ethLog.Address, safePledge)
		if err != nil {
			fmt.Printf("Failed to update safe pledge in contract. address: %v, safePledge: %v\n", ethLog.Address, safePledge)
			continue
		}
		err = c.logProcessedMarker.LogProcessed(context.Background(), ethLog.Address, ethLog.TransactionHash, msgHash)
		if err != nil {
			fmt.Printf("Failed to update safe pledge in contract. address: %v, safePledge: %v\n", ethLog.Address, safePledge)
			continue
		}
	}
}

func (c *Calculator) calculateSafePledge(minerID string) (big.Int, error) {
	minerAddress, _ := address.NewFromString(minerID) // Initialize the variable with an address
	fmt.Print("Serving request: ", minerAddress, "\n\n")

	// Log out current information
	tipset, err := c.api.ChainHead(context.Background())
	if err != nil {
		log.Fatalf("calling chain head: %s", err)
	}
	// fmt.Printf(w, "Current chain head is: %s\n", tipset.String())
	ctx := context.Background()

	/// Get miner balance
	availableBal, err := c.api.StateMinerAvailableBalance(ctx, minerAddress, tipset.Key())
	if err != nil {
		return big.Int{}, err
	}

	// Get all sectors for a miner
	sectors, err := c.api.StateMinerSectors(ctx, minerAddress, nil, tipset.Key())
	if err != nil {
		return big.Int{}, err
	}

	// Get miner actorState
	actorState, err := c.api.StateReadState(ctx, minerAddress, tipset.Key())
	if err != nil {
		return big.Int{}, err
	}

	miner := miner.State{}
	d, err := json.Marshal(actorState.State)
	if err != nil {
		return big.Int{}, err
	}
	err = json.Unmarshal(d, &miner)
	if err != nil {
		return big.Int{}, err
	}
	fmt.Printf("\n__________________[Eligible Asset Calculation]__________________\n\n")
	fmt.Printf("   initial_pledge                           = %v \n", types.FIL(miner.InitialPledge).Short())
	fmt.Printf(" + available_bal                            = %v\n", types.FIL(availableBal).Short())
	fmt.Printf(" + locked_funds                             = %v\n", types.FIL(miner.LockedFunds).Short())
	fmt.Printf(" - fee_debt                                 = %v\n", types.FIL(miner.FeeDebt).Short())
	fmt.Printf(" = eligible_asset (formula 1)               = %v \n", types.FIL(big.Sub(big.Add(big.Add(miner.InitialPledge, availableBal), miner.LockedFunds), miner.FeeDebt)).Short())
	fmt.Printf("\n\n")
	fmt.Printf("   actor_balance                            = %v\n", types.FIL(actorState.Balance).Short())
	fmt.Printf(" - fee_debt                                 = %v\n", types.FIL(miner.FeeDebt).Short())
	fmt.Printf(" - precommit_deposits                       = %v\n", types.FIL(miner.PreCommitDeposits).Short())
	fmt.Printf(" = eligible_asset (formula 2)               = %v \n", types.FIL(big.Sub(big.Sub(actorState.Balance, miner.FeeDebt), miner.PreCommitDeposits)).Short())

	// Calculation termination fee of all sectors
	totalTerminationFee := big.Zero()
	totalInitialPledge := big.Zero()
	feeList := []big.Int{}
	numExpiredSectors := 0
	dayRewardList := []big.Int{}
	initialPledgeList := []big.Int{}
	epochsToExpirationList := []big.Int{}
	sectorEpochAgeList := []big.Int{}
	weightedDayReward := big.Zero()
	// Print all sector details
	for _, sector := range sectors {
		if sector.Expiration < tipset.Height() {
			numExpiredSectors += 1
			continue
		}
		if sector.ReplacedSectorAge > 0 {
			fmt.Printf("Replaced Sector age												 = %v\n", sector.ReplacedSectorAge)
		}
		fee, sectorAge := calculateBaseTerminationFee(tipset.Height(), sector.Activation, sector.ReplacedSectorAge, sector.ExpectedStoragePledge, sector.ExpectedDayReward, sector.ReplacedDayReward)
		// fmt.Printf("%d, Sector info: Activation=%v, ExpectedDayReward=%v, InitialPledge=%v, ExpectedStoragePledge=%v, ReplacedDayReward=%v\n",
		// i, sector.Activation, sector.ExpectedDayReward, sector.InitialPledge, sector.ExpectedStoragePledge, sector.ReplacedDayReward)
		totalTerminationFee = big.Add(totalTerminationFee, fee)
		totalInitialPledge = big.Add(totalInitialPledge, sector.InitialPledge)
		feeList = append(feeList, fee)
		initialPledgeList = append(initialPledgeList, sector.InitialPledge)
		epochsToExpirationList = append(epochsToExpirationList, big.NewInt(int64(sector.Expiration-tipset.Height())))
		dayRewardList = append(dayRewardList, sector.ExpectedDayReward)
		sectorEpochAgeList = append(sectorEpochAgeList, big.NewInt(int64(sectorAge)))
		weightedDayReward = big.Add(weightedDayReward, big.Mul(sector.ExpectedDayReward, big.NewInt(int64(sectorAge))))
	}

	eligibleAsset := big.Sub(big.Sub(actorState.Balance, miner.FeeDebt), miner.PreCommitDeposits)
	safePledge := big.Sub(eligibleAsset, totalTerminationFee)
	numActiveSectors := len(sectors) - numExpiredSectors

	fmt.Printf("\n")
	fmt.Printf("\n__________________[BaseTerminationFee Calculation]__________________\n\n")
	fmt.Printf("BaseTerminationFee (all active sectors)     = %v\n", types.FIL(totalTerminationFee).Short())
	fmt.Printf("\n__________________[Stats]__________________\n\n")
	fmt.Printf("baseTermination/eligibleAsset               = %v %%\n", big.Div(big.Mul(totalTerminationFee, big.NewInt(100)), eligibleAsset))
	fmt.Printf("baseTermination/initialPledge               = %v %%\n", big.Div(big.Mul(totalTerminationFee, big.NewInt(100)), miner.InitialPledge))
	fmt.Printf("safePledge            	                    = %v\n", types.FIL(safePledge).Short())
	fmt.Printf("safePledge/eligibleAsset		                = %v %%\n", big.Div(big.Mul(safePledge, big.NewInt(100)), eligibleAsset))
	fmt.Printf("safePledge/initialPledge		                = %v %%\n\n", big.Div(big.Mul(safePledge, big.NewInt(100)), miner.InitialPledge))
	fmt.Printf("avg initial_pledge per sector               = %v\n", types.FIL(big.Div(totalInitialPledge, big.NewInt(int64(numActiveSectors)))).Short())
	fmt.Printf("median initial_pledge per sector            = %v\n", types.FIL(bigIntMedian(initialPledgeList)).Short())
	fmt.Printf("median expected_day_reward                  = %v\n", types.FIL(bigIntMedian(dayRewardList)).Short())
	fmt.Printf("median sector_age                           = %v\n", big.Div(bigIntMedian(sectorEpochAgeList), big.NewInt(int64(NUM_EPOCH_IN_DAY))))
	fmt.Printf("median sector BTF                           = %v\n", types.FIL(bigIntMedian(feeList)).Short())
	fmt.Printf("median days to expiration                   = %v\n", big.Div(bigIntMedian(epochsToExpirationList), big.NewInt(int64(NUM_EPOCH_IN_DAY))))
	fmt.Printf("Daily vesting rewards                       = %v\n", types.FIL(big.Div(miner.LockedFunds, big.NewInt(180))).Short())

	// funds := big.Add(totalInitialPledge, availableBal)
	// fmt.Printf("TotalInitialPledge                          = %v\n", types.FIL(totalInitialPledge).Short())
	// fmt.Printf("AvailableBalance                            = %v\n", types.FIL(availableBal).Short())
	// fmt.Printf("TotalInitialPledge + AvailableBalance       = %v \n", types.FIL(funds).Short())
	// fmt.Printf("Current epoch is                            = %s\n", tipset.Height())

	fmt.Printf("\n__________________[Sector Expiry]__________________\n\n")
	fmt.Printf("%d total sectors\n", len(sectors))
	fmt.Printf("%d expired sectors\n", numExpiredSectors)
	fmt.Printf("%d active sectors\n", numActiveSectors)
	return totalInitialPledge, nil
}

func calculateBaseTerminationFee(currentEpoch, activationEpoch, replacedSectorAge abi.ChainEpoch, TDRAA, dayReward, replacedDayReward abi.TokenAmount) (abi.TokenAmount, abi.ChainEpoch) {
	// Lifetime cap in epochs
	lifeTimeCapInEpoch := abi.ChainEpoch(LIFETIME_CAP * NUM_EPOCH_IN_DAY)

	// Calculating new sector age
	sectorAge := currentEpoch - (replacedSectorAge + activationEpoch)
	cappedSectorAge := minEpoch(sectorAge, lifeTimeCapInEpoch)

	// Calculating replaced sector age
	relevantReplacedAge := minEpoch(replacedSectorAge, lifeTimeCapInEpoch-cappedSectorAge)

	// Expected reward for lifetime of new sector
	expectedNewReward := big.Mul(dayReward, big.NewInt(int64(cappedSectorAge)))

	// Expected reward for lifetime of replaced sector
	expectedReplacedReward := big.Mul(replacedDayReward, big.NewInt(int64(relevantReplacedAge)))

	// penalty = half of totalExpectedReward
	totalExpectedReward := big.Add(expectedNewReward, expectedReplacedReward)
	penalty := big.Div(totalExpectedReward, big.NewInt(int64(TERMINATION_REWARD_FACTOR_DENOM)))

	// terminationFee = penalty + TDRAA
	fee := big.Add(TDRAA, big.Div(penalty, big.NewInt(int64(NUM_EPOCH_IN_DAY))))

	return fee, sectorAge
}

func (c *Calculator) UpdateContractSafePledge(ctx context.Context, addr ethtypes.EthAddress, safePledge big.Int) (ethtypes.EthHash, error) {
	return ethtypes.EthHash{}, nil
}

func minEpoch(a, b abi.ChainEpoch) abi.ChainEpoch {
	if a < b {
		return a
	}
	return b
}

func bigIntMedian(data []big.Int) big.Int {
	dataCopy := make([]big.Int, len(data))
	copy(dataCopy, data)

	// Sort the dataCopy in ascending order.
	sort.Slice(dataCopy, func(i, j int) bool {
		return dataCopy[i].LessThan(dataCopy[j])
	})

	var median big.Int
	l := len(dataCopy)
	if l == 0 {
		return big.NewInt(0)
	} else if l%2 == 0 {
		median = big.Div(big.Add(dataCopy[l/2-1], dataCopy[l/2]), big.NewInt(2))
	} else {
		median = dataCopy[l/2]
	}

	return median
}

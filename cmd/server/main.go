package server

import (
	"fmt"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-jsonrpc"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	lotusapi "github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/filecoin-project/lotus/chain/types/ethtypes"
	"github.com/filecoin-project/lotus/itests/kit"
	"github.com/jinzhu/gorm"
	"go_filecoin/clients"
	"log"
	"time"
)

// Declare integer constant
const (
	ADDR                            = "filecoin.chainup.net"
	NUM_EPOCH_IN_DAY                = 2880
	LIFETIME_CAP                    = 140
	TERMINATION_REWARD_FACTOR_DENOM = 2
)

func main() {
	// TODO
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
	latestChainHeightStore := clients.NewLatestChainHeightStore(db)
	ethLogStorer := clients.NewEthLogStore(db)
	fetcher := NewFetcher(config, api, latestChainHeightStore, ethLogStorer)
	go fetcher.Start()

	c := make(chan int)
	<-c
}

func ParseConfig() (*FetcherConfig, error) {
	// TODO
	return nil, nil
}

type Server struct {
	config      *ServerConfig
	api         lotusapi.FullNodeStruct
	apiCloser   jsonrpc.ClientCloser
	heightStore LatestChainHeightStore
	logStorer   EthLogStorer
	topicHash   ethtypes.EthHash
}

type ServerConfig struct {
	DBAddr                   string
	CalculateSafePledgeTopic string
}

func NewServer(c *ServerConfig, api lotusapi.FullNodeStruct, heightStore LatestChainHeightStore, logStore EthLogStorer) *Server {
	return &Fetcher{
		config:      c,
		api:         api,
		heightStore: heightStore,
		logStorer:   logStore,
		topicHash:   kit.EthTopicHash(c.CalculateSafePledgeTopic),
	}
}

func (f *Server) Stop() error {
	return f.heightStore.Close()
}

func (f *Server) Start() {
	var height abi.ChainEpoch
	var filterSpec = ethtypes.EthFilterSpec{
		Topics: []ethtypes.EthHashList{
			[]ethtypes.EthHash{
				f.topicHash,
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
	}
}

func main1() {
	minerIdList := []string{"f01889512", "f0123261", "f01137150", "f01697248"}

	// For minerID in minderIDlist
	for _, minerId := range minerIdList {
		addr := "filecoin.chainup.net"
		minerAddress, _ := address.NewFromString(minerId) // Initialize the variable with an address
		fmt.Print("Serving request: ", minerAddress, "\n\n")

		var api lotusapi.FullNodeStruct
		closer, err := jsonrpc.NewMergeClient(context.Background(), "wss://"+addr+"/rpc/v1", "Filecoin", []interface{}{&api.Internal, &api.CommonStruct.Internal}, nil, jsonrpc.WithTimeout(120*time.Second))
		if err != nil {
			log.Fatalf("connecting with lotus failed: %s", err)
		}
		defer closer()

		// Log out current information
		tipset, err := api.ChainHead(context.Background())
		if err != nil {
			log.Fatalf("calling chain head: %s", err)
		}
		// fmt.Printf(w, "Current chain head is: %s\n", tipset.String())
		ctx := context.Background()

		/// Get miner balance
		availableBal, err := api.StateMinerAvailableBalance(ctx, minerAddress, tipset.Key())
		if err != nil {
			panic(err)
		}

		// Get all sectors for a miner
		sectors, err := api.StateMinerSectors(ctx, minerAddress, nil, tipset.Key())
		if err != nil {
			panic(err)
		}

		// Get miner actorState
		actorState, err := api.StateReadState(ctx, minerAddress, tipset.Key())
		if err != nil {
			panic(err)
		}

		miner := miner.State{}
		d, err := json.Marshal(actorState.State)
		if err != nil {
			panic(err)
		}
		err = json.Unmarshal(d, &miner)
		if err != nil {
			panic(err)
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
	}
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

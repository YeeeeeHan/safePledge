package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
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
	NUM_EPOCH_IN_DAY                = 2880
	LIFETIME_CAP                    = 540
	TERMINATION_REWARD_FACTOR_DENOM = 2
)

func main() {
	minerIdList := []string{"f01889512", "f0123261", "f01137150", "f01697248"}

	// For minerID in minderIDlist
	for _, minerId := range minerIdList {
		fmt.Printf("___________________________ %v________________________________\n", minerId)

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

		s := miner.State{}
		d, err := json.Marshal(actorState.State)
		if err != nil {
			panic(err)
		}
		err = json.Unmarshal(d, &s)
		if err != nil {
			panic(err)
		}
		fmt.Printf("   initial_pledge                           = %v \n", types.FIL(s.InitialPledge).Short())
		fmt.Printf(" + available_bal                            = %v\n", types.FIL(availableBal).Short())
		fmt.Printf(" + locked_funds                             = %v\n", types.FIL(s.LockedFunds).Short())
		fmt.Printf(" - fee_debt                                 = %v\n", types.FIL(s.FeeDebt).Short())
		fmt.Printf("___________________________________________________________\n")
		fmt.Printf(" = eligible_asset (formula 1)               = %v \n", types.FIL(big.Sub(big.Add(big.Add(s.InitialPledge, availableBal), s.LockedFunds), s.FeeDebt)).Short())
		fmt.Printf("\n\n")
		fmt.Printf("   actor_balance                            = %v\n", types.FIL(actorState.Balance).Short())
		fmt.Printf(" - fee_debt                                 = %v\n", types.FIL(s.FeeDebt).Short())
		fmt.Printf(" - precommit_deposits                       = %v\n", types.FIL(s.PreCommitDeposits).Short())
		fmt.Printf("___________________________________________________________\n")
		fmt.Printf(" = eligible_asset (formula 2)               = %v \n", types.FIL(big.Sub(big.Sub(actorState.Balance, s.FeeDebt), s.PreCommitDeposits)).Short())

		// Calculation termination fee of all sectors
		totalTerminationFee := big.Zero()
		totalInitialPledge := big.Zero()
		expiredSectorCount := 0
		// Print all sector details
		for _, sector := range sectors {
			if sector.Expiration < tipset.Height() {
				expiredSectorCount += 1
				continue
			}
			fee := calculateBaseTerminationFee(tipset.Height(), sector.Activation, sector.Activation, sector.ExpectedStoragePledge, sector.ExpectedDayReward, sector.ReplacedDayReward)
			// fmt.Printf("%d, Sector info: Activation=%v, ExpectedDayReward=%v, InitialPledge=%v, ExpectedStoragePledge=%v, ReplacedDayReward=%v\n",
			// i, sector.Activation, sector.ExpectedDayReward, sector.InitialPledge, sector.ExpectedStoragePledge, sector.ReplacedDayReward)
			totalTerminationFee = big.Add(totalTerminationFee, fee)
			totalInitialPledge = big.Add(totalInitialPledge, sector.InitialPledge)
		}
		fmt.Printf("\n\n")
		fmt.Printf("BaseTerminationFee for all sectors          = %v\n", types.FIL(totalTerminationFee).Short())
		fmt.Printf("safePledge                                  = %v %%\n", big.Div(big.Mul(totalTerminationFee, big.NewInt(100)), big.Sub(big.Sub(actorState.Balance, s.FeeDebt), s.PreCommitDeposits)))

		// funds := big.Add(totalInitialPledge, availableBal)
		// fmt.Printf("TotalInitialPledge                          = %v\n", types.FIL(totalInitialPledge).Short())
		// fmt.Printf("AvailableBalance                            = %v\n", types.FIL(availableBal).Short())
		// fmt.Printf("TotalInitialPledge + AvailableBalance       = %v \n", types.FIL(funds).Short())
		// fmt.Printf("Current epoch is                            = %s\n", tipset.Height())

		fmt.Printf("\nminer %v has %d total sectors\n", minerId, len(sectors))
		fmt.Printf("miner %v has %d expired sectors\n", minerId, expiredSectorCount)
		fmt.Printf("miner %v has %d active sectors\n", minerId, len(sectors)-expiredSectorCount)
	}
}

func calculateBaseTerminationFee(currentEpoch, activationEpoch, powerBaseEpoch, abi.ChainEpoch, TDRAA, dayReward, replacedDayReward abi.TokenAmount) abi.TokenAmount {
	// Lifetime cap in epochs
	lifeTimeCapInEpoch := abi.ChainEpoch(LIFETIME_CAP * NUM_EPOCH_IN_DAY)

	// Calculating new sector age
	sectorAge := currentEpoch - powerBaseEpoch
	cappedSectorAge := minEpoch(sectorAge, lifeTimeCapInEpoch)

	// Calculating replaced sector age
	replacedSectorAge := powerBaseEpoch - activationEpoch
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

	return fee
}

func minEpoch(a, b abi.ChainEpoch) abi.ChainEpoch {
	if a < b {
		return a
	}
	return b
}

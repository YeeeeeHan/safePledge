package main

import (
	"context"
	"fmt"
	"log"
	"net/http"

	"github.com/filecoin-project/go-address"
	jsonrpc "github.com/filecoin-project/go-jsonrpc"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	lotusapi "github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/gorilla/mux"
)

// Declare integer constant
const (
	NUM_EPOCH_IN_DAY                = 2880
	LIFETIME_CAP                    = 540
	TERMINATION_REWARD_FACTOR_DENOM = 2
)

func main() {
	// Create a new router using Gorilla Mux
	r := mux.NewRouter()

	// Define your API routes and handlers
	r.HandleFunc("/{id:[fF0-9]+}", Handler).Methods("GET")

	// Start the HTTP server on port 8080 using the router
	port := 8080
	fmt.Printf("Server is running on port %d...\n", port)
	http.Handle("/", r)
	err := http.ListenAndServe(fmt.Sprintf(":%d", port), nil)
	if err != nil {
		fmt.Printf("Server failed to start: %v\n", err)
	}

}

func calculateBaseTerminationFee(currentEpoch, activationEpoch, powerBaseEpoch abi.ChainEpoch, TDRAA, dayReward, replacedDayReward abi.TokenAmount) abi.TokenAmount {
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

// Handler is a sample request handler
func Handler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	minerID := vars["id"]

	if r.Method == http.MethodGet {
		ConnectNode(minerID, w, r)
	} else {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func ConnectNode(minerId string, w http.ResponseWriter, r *http.Request) {
	addr := "filecoin.chainup.net"
	minerAddress, _ := address.NewFromString(minerId) // Initialize the variable with an address
	fmt.Print("Serving request: ", minerAddress, "\n")

	var api lotusapi.FullNodeStruct
	closer, err := jsonrpc.NewMergeClient(context.Background(), "wss://"+addr+"/rpc/v1", "Filecoin", []interface{}{&api.Internal, &api.CommonStruct.Internal}, nil)
	if err != nil {
		log.Fatalf("connecting with lotus failed: %s", err)
	}
	defer closer()

	// Log out current information
	tipset, err := api.ChainHead(context.Background())
	if err != nil {
		log.Fatalf("calling chain head: %s", err)
	}
	// fmt.Fprintf(w, "Current chain head is: %s\n", tipset.String())
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

	// Calculation termination fee of all sectors
	totalTerminationFee := big.Zero()
	totalInitialPledge := big.Zero()
	fmt.Fprintf(w, "miner %v has %d sectors\n", minerId, len(sectors))
	fmt.Fprintf(w, "Current epoch is                         	  = %s\n", tipset.Height())
	// Print all sector details
	for _, sector := range sectors {
		fee := calculateBaseTerminationFee(tipset.Height(), sector.Activation, sector.Activation, sector.ExpectedStoragePledge, sector.ExpectedDayReward, sector.ReplacedDayReward)
		// fmt.Printf("%d, Sector info: Activation=%v, ExpectedDayReward=%v, InitialPledge=%v, ExpectedStoragePledge=%v, ReplacedDayReward=%v\n",
		// i, sector.Activation, sector.ExpectedDayReward, sector.InitialPledge, sector.ExpectedStoragePledge, sector.ReplacedDayReward)
		totalTerminationFee = big.Add(totalTerminationFee, fee)
		totalInitialPledge = big.Add(totalInitialPledge, sector.InitialPledge)
	}
	funds := big.Add(totalInitialPledge, availableBal)
	fmt.Fprintf(w, "TotalInitialPledge                          = %v\n", types.FIL(totalInitialPledge).Short())
	fmt.Fprintf(w, "AvailableBalance                            = %v\n", types.FIL(availableBal).Short())
	fmt.Fprintf(w, "TotalInitialPledge + AvailableBalance       = %v \n", types.FIL(funds).Short())
	fmt.Fprintf(w, "BaseTerminationFee for all sectors          = %v\n", types.FIL(totalTerminationFee).Short())
}

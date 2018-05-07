package main

import (
	"net"
	"math/rand"
	"encoding/json"

	"github.com/op/go-logging"
	"github.com/chepeftw/bchainlibs"

	"time"
	"os"
	"sort"
	"github.com/onrik/gomerkle"
	"crypto/sha256"
	"strconv"
)

// +++++++++++++++++++++++++++
// +++++++++ Go-Logging Conf
// +++++++++++++++++++++++++++
var log = logging.MustGetLogger("miner")

// +++++++++ Global vars
var me = net.ParseIP(bchainlibs.LocalhostAddr)
//var miningRetries = 100
//var miningWaitTime = 100

var lastBlock = ""
//var randomGen = rand.New(rand.NewSource(time.Now().UnixNano()))

// +++++++++ Channels
// For the Miner the Input and Output will be to Router
var input = make(chan string)
var output = make(chan string)
var done = make(chan bool)

//// +++++++++ Unverified Blocks MAP with sync
//var unverifiedBlocks = bchainlibs.MapBlocks{make(map[string]bchainlibs.Packet), make(map[string]int64), make(map[string]int64), sync.RWMutex{}}
var preBlocks = make(map[string]bchainlibs.Block)

func toOutput(payload bchainlibs.Packet) {
	log.Debug("Sending Packet with ID " + payload.ID + " to channel output")
	bchainlibs.SendGeneric(output, payload, log)
}

func attendOutputChannel() {
	log.Debug("Starting output channel")
	bchainlibs.SendToNetwork(me.String(), bchainlibs.RouterPort, output, false, log, me)
}

// Function that handles the buffer channel
func attendInputChannel() {

	for {
		j, more := <-input
		if more {
			// First we take the json, unmarshal it to an object
			payload := bchainlibs.Packet{}
			json.Unmarshal([]byte(j), &payload)

			//source := payload.Source
			id := payload.ID

			switch payload.Type {

			case bchainlibs.LastBlockType:
				delete(preBlocks, payload.Block.QueryID)
				lastBlock = payload.Block.ID
				break

			case bchainlibs.RaftResult:
				log.Info("Election RESULTS!!!")
				log.Info("There are " + strconv.Itoa(len(preBlocks)) + " items in preBlocks")

				var indexes []string

				for index, preBlock := range preBlocks {
					// Setting all the important data
					preBlock.MerkleTreeRoot = getMerkleTreeRoot( preBlock.Transactions )
					preBlock.Timestamp = time.Now().UnixNano()
					preBlock.Nonce = randStringRunes(64)
					preBlock.PreviousID = lastBlock

					// Building THE ID
					concat := preBlock.PreviousID
					concat += preBlock.Nonce
					concat += strconv.FormatInt(preBlock.Timestamp, 10)
					concat += preBlock.MerkleTreeRoot

					sum1 := sha256.Sum256([]byte( concat ))
					sum2 := sha256.Sum256( sum1[:] )

					preBlock.ID = string(sum2[:])

					// Adding the index to an array to delete the preblock after
					indexes = append(indexes, index)

					// Just debugging the block
					log.Info("BLOCK READY TO GO!!!")
					log.Info( preBlock.String() )

					// Send it to the world
					payload := bchainlibs.CreateBlockPacket(me, preBlock)
					toOutput(payload)

					// There should only be one
				}

				for _, index := range indexes {
					delete(preBlocks, index)
				}

				break

			case bchainlibs.LaunchElection:
				log.Info("Election TIME!!!")

				// Adding this for the next stage
				preBlocks[ payload.Block.QueryID ] = *payload.Block

				break

			case bchainlibs.InternalPing:
				log.Info("Receiving PING from router with ID = " + id)
				payload := bchainlibs.BuildPong(me)
				toOutput(payload)
				break

			}

		} else {
			log.Debug("closing channel")
			done <- true
			return
		}

	}
}

func getMerkleTreeRoot(transactions []bchainlibs.Transaction) string {
	sort.Slice(transactions, func(i, j int) bool {
		return transactions[i].Order < transactions[j].Order
	})

	var data [][]byte

	for _, transaction := range transactions {
		byteArr := []byte( transaction.String() )
		data = append(data, byteArr)
	}

	tree := gomerkle.NewTree(sha256.New())
	tree.AddData(data...)

	err := tree.Generate()
	if err != nil {
		panic(err)
	}

	return string(tree.Root())
}

func randStringRunesInit() {
	rand.Seed(time.Now().UnixNano())
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randStringRunes(n int) string {
	randStringRunesInit()
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

//func toMilliseconds( nano int64 ) int64 {
//	return nano / int64(time.Millisecond)
//}

//func eqIp( a net.IP, b net.IP ) bool {
//	return treesiplibs.CompareIPs(a, b)
//}

func main() {

	confPath := "/app/conf.yml"
	if len(os.Args[1:]) >= 1 {
		confPath = os.Args[1]
	}
	var c bchainlibs.Conf
	c.GetConf(confPath)

	targetSync := c.TargetSync
	//miningRetries = c.MiningRetry
	//miningWaitTime = c.MiningWait

	logPath := c.LogPath

	// Logger configuration
	logName := "miner"
	f := bchainlibs.PrepareLogGen(logPath, logName, "data")
	defer f.Close()
	f2 := bchainlibs.PrepareLog(logPath, logName)
	defer f2.Close()
	backend := logging.NewLogBackend(f, "", 0)
	backend2 := logging.NewLogBackend(f2, "", 0)
	backendFormatter := logging.NewBackendFormatter(backend, bchainlibs.LogFormat)

	backend2Formatter := logging.NewBackendFormatter(backend2, bchainlibs.LogFormatPimp)

	backendLeveled := logging.AddModuleLevel(backendFormatter)
	backendLeveled.SetLevel(logging.DEBUG, "")

	// Only errors and more severe messages should be sent to backend1
	backend2Leveled := logging.AddModuleLevel(backend2Formatter)
	backend2Leveled.SetLevel(logging.INFO, "")

	logging.SetBackend(backendLeveled, backend2Leveled)

	log.Info("")
	log.Info("------------------------------------------------------------------------")
	log.Info("")
	log.Info("Starting Routing process, waiting some time to get my own IP...")

	// Wait for sync
	bchainlibs.WaitForSync(targetSync, log)

	// But first let me take a selfie, in a Go lang program is getting my own IP
	me = bchainlibs.SelfieIP()
	log.Info("Good to go, my ip is " + me.String())

	// Lets prepare a address at any address at port bchainlibs.MinerPort
	ServerAddr, err := net.ResolveUDPAddr(bchainlibs.Protocol, bchainlibs.MinerPort)
	bchainlibs.CheckError(err, log)

	// Now listen at selected port
	ServerConn, err := net.ListenUDP(bchainlibs.Protocol, ServerAddr)
	bchainlibs.CheckError(err, log)
	defer ServerConn.Close()

	// Run the Input!
	go attendInputChannel()
	// Run the Output channel! The direct messages to the router layer
	go attendOutputChannel()

	buf := make([]byte, 1024)

	for {
		n, _, err := ServerConn.ReadFromUDP(buf)
		input <- string(buf[0:n])
		bchainlibs.CheckError(err, log)
	}

	close(input)
	close(output)

	<-done
}

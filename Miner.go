package main

import (
    //"os"
    "net"
	"math/rand"
    "encoding/json"

    "github.com/op/go-logging"
    "github.com/chepeftw/treesiplibs"
	"github.com/chepeftw/bchainlibs"

	"crypto/sha256"
	"strings"
	"time"
	"sync"
	"strconv"
	"os"
	"encoding/hex"
	"github.com/onrik/gomerkle"
)


// +++++++++++++++++++++++++++
// +++++++++ Go-Logging Conf
// +++++++++++++++++++++++++++
var log = logging.MustGetLogger("miner")

// +++++++++ Global vars
var me = net.ParseIP(bchainlibs.LocalhostAddr)
var miningRetries = 100
//var miningWaitTime = 100
var cryptoPiece = "00"
var lastBlock = bchainlibs.Packet{}
var randomGen = rand.New( rand.NewSource( time.Now().UnixNano() ) )

// +++++++++ Channels
// For the Miner the Input and Output will be to Router
var input = make(chan string)
var output = make(chan string)
var done = make(chan bool)
var mining = make(chan string)

var waitQueue []string

var transactions map[string]bchainlibs.Packet

// +++++++++ Unverified Blocks MAP with sync
var unverifiedBlocks = bchainlibs.MapBlocks{ make(map[string]bchainlibs.Packet), make(map[string]int64), make(map[string]int64), sync.RWMutex{} }

func toOutput(payload bchainlibs.Packet) {
	log.Debug("Sending Packet with ID " + payload.ID + " to channel output")
	bchainlibs.SendGeneric( output, payload, log )
}

func attendOutputChannel() {
	log.Debug("Starting output channel")
	bchainlibs.SendToNetwork( me.String(), bchainlibs.RouterPort, output, false, log, me)
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

		case bchainlibs.TransactionType:

			if _, ok := transactions[ id ]; !ok {
				transactions[ id ] = payload

				// Check for query completeness
				count := 0
				queryID := payload.Transaction.ID
				data := make([][]byte, 5)
				for _, element := range transactions {
					if queryID == element.Transaction.ID {
						data[count] = []byte(element.Transaction.Data.String())
						count++
					}
				}

				if count == 4 {
					// AssembleMerkleTree ... then put it in unverifiedblocks and call pow or any other mechanism

					tree := gomerkle.NewTree(sha256.New())
					tree.AddData(data...)

					err := tree.Generate()
					if err != nil {
						panic(err)
					}

					merkleTreeRoot := hex.EncodeToString(tree.Root())
					log.Info("Merkle Tree Root = " + merkleTreeRoot)

					if !unverifiedBlocks.Has(merkleTreeRoot) { // If it does not exists then
						log.Debug("New Unverified Block to mine with merkle tree root = " + merkleTreeRoot)

						transactionDatas := make([]bchainlibs.TransactionData, 4)
						for _, element := range transactions {
							if queryID == element.Transaction.ID {
								transactionDatas[count] = *element.Transaction.Data
							}
						}

						newBlock := bchainlibs.CreateBlock(me, queryID, merkleTreeRoot, transactionDatas)

						pow(merkleTreeRoot, newBlock)
					} else {
						log.Info("Redundant Merkle Tree Root = " + merkleTreeRoot)
					}
				}

			} else {
				log.Info("RedundantID = " + id)
			}

		break

		case bchainlibs.LastBlockType:
			merkleTreeRoot := payload.Block.MerkleTreeRoot
			queryID := payload.Block.QueryID

			if unverifiedBlocks.Has(merkleTreeRoot) { // If it does exists then
				unverifiedBlocks.Del(merkleTreeRoot)
			}

			for _, element := range payload.Block.Transactions {
				for key, element2 := range transactions {
					if queryID == element2.Transaction.ID {
						sum1 := sha256.Sum256([]byte( element.String() ))
						sum2 := sha256.Sum256([]byte( element2.Transaction.Data.String() ))

						if sum1 == sum2 {
							delete( transactions, key )
						}
					}
				}
			}

			lastBlock = payload
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

func pow(merkle string, payload bchainlibs.Packet) {
	randNum := randomGen.Intn(10000)
	coin := randNum % 2
	log.Debug("Random Number is " + strconv.Itoa(randNum))
	log.Debug("Coin toss is " + strconv.Itoa(coin))

	unverifiedBlocks.Add(merkle, payload)

	if coin == 0 {
		log.Debug("Mining true for merkle tree root =" + merkle)
		log.Debug("unverifiedBlocks => " + unverifiedBlocks.String())
		go func() {
			mining <- merkle
		}()
	}
}


// Function that handles the buffer channel
func attendMiningChannel() {
	log.Debug("Starting mining channel")
	globalMiningCount := int64(1)

	for {
		j, more := <-mining
		if more {
			// First we take the json, unmarshal it to an object
			if unverifiedBlocks.Has(j) {

				block := unverifiedBlocks.Get(j)
				foundIt := false
				startTime := int64(0)
				validity := true

				for unverifiedBlocks.Has(j) && !foundIt && validity {

					globalMiningCount++
					validity = false

					if ( globalMiningCount % 100 ) == 0 {
						log.Debug("Mining " + block.ID)
					}

					// Time to verify
					// Validation based on the query
					validity = true

					if validity {
						//roaming = false

						hashGeneration := 0
						for i := 0; i < miningRetries ; i++  {
							if !foundIt {
								randString := bchainlibs.RandString(20)

								information := lastBlock.Block.ID + block.Block.MerkleTreeRoot + randString
								checksum := bchainlibs.MyCalculateSHA(information)

								hashGeneration += 1

								//if strings.Contains(string(checksum), cryptoPiece) {
								if strings.HasPrefix(checksum, cryptoPiece) {
									// Myabe????
									//if unverifiedBlocks.Has(j) {
									verified := bchainlibs.BuildBlock(block, lastBlock.Block.ID, randString, checksum, me)
									toOutput(verified)

									startTime = unverifiedBlocks.Del(block.Block.MerkleTreeRoot)

									//}

									log.Debug("Mining_WIN => " + checksum)
									log.Debug("randString => " + randString )
									log.Debug("unverifiedBlocks => " + unverifiedBlocks.String())

									foundIt = true
								}
							}
						}

						unverifiedBlocks.AddHashesCount(block.Block.MerkleTreeRoot, int64(hashGeneration))
					} else {
						waitQueue = append( waitQueue, j )

						if len(waitQueue) > 0 {
							jj := waitQueue[0]
							waitQueue = waitQueue[1:]

							go func() {
								duration := randomGen.Intn(100000) / 100
								time.Sleep( time.Millisecond * time.Duration( 1000 + duration ) )
								log.Debug("RESCHEDULE " + jj + " => " + strconv.Itoa(1000 + duration))
								mining <- jj
							}()
						}
					}

					//if !foundIt && validity {
					//	//log.Debug("Rock and roll then")
					//	go func() {
					//		mining <- block.TID
					//	}()

						//duration := randomGen.Intn(100000) / miningWaitTime
						//log.Debug("Repeat mining! But first waiting for " + strconv.Itoa(duration) + "ms")
						//time.Sleep( time.Millisecond * time.Duration( duration ) )
					//} else

					if foundIt {
						elapsedTimeNs := time.Now().UnixNano() - startTime
						elapsedTimeMs := toMilliseconds( elapsedTimeNs )
						log.Debug("MINER_WIN_TIME_NS=" + strconv.FormatInt(elapsedTimeNs, 10))
						log.Debug("MINER_WIN_TIME_MS=" + strconv.FormatInt(elapsedTimeMs, 10))

						hashesGenerated := unverifiedBlocks.GetDelHashesCount(block.Block.MerkleTreeRoot)
						log.Info("HASHES_GENERATED=" + strconv.FormatInt(hashesGenerated, 10))

						//roaming = true
					}

				}

				if !unverifiedBlocks.Has(j) && !foundIt {
					log.Debug("Unverified block " + j + " is not in the list, moving on!")

					hashesGenerated := unverifiedBlocks.GetDelHashesCount(j)
					log.Info("HASHES_GENERATED=" + strconv.FormatInt(hashesGenerated, 10))
				}

			} else {
				log.Debug("Unverified block " + j + " is not in the list, moving on!")

				hashesGenerated := unverifiedBlocks.GetDelHashesCount(j)
				log.Info("HASHES_GENERATED=" + strconv.FormatInt(hashesGenerated, 10))
			}

		} else {
			log.Debug("closing channel")
			done <- true
			return
		}

	}
}

func toMilliseconds( nano int64 ) int64 {
	return nano / int64(time.Millisecond)
}

//func eqIp( a net.IP, b net.IP ) bool {
//	return treesiplibs.CompareIPs(a, b)
//}

func main() {

    confPath := "/app/conf.yml"
    if len(os.Args[1:]) >= 1 {
		confPath = os.Args[1]
    }
    var c bchainlibs.Conf
    c.GetConf( confPath )

    targetSync := c.TargetSync
    miningRetries = c.MiningRetry
	//miningWaitTime = c.MiningWait
	cryptoPiece = c.CryptoPiece
	logPath := c.LogPath

	// Logger configuration
	f := bchainlibs.PrepareLog( logPath, "miner" )
	defer f.Close()
	backend := logging.NewLogBackend(f, "", 0)
	backendFormatter := logging.NewBackendFormatter(backend, bchainlibs.LogFormat)
	backendLeveled := logging.AddModuleLevel(backendFormatter)
	backendLeveled.SetLevel(logging.DEBUG, "")
	logging.SetBackend( backendLeveled )

    log.Info("")
    log.Info("------------------------------------------------------------------------")
    log.Info("")
    log.Info("Starting Routing process, waiting some time to get my own IP...")

	// Wait for sync
	bchainlibs.WaitForSync( targetSync, log )

    // But first let me take a selfie, in a Go lang program is getting my own IP
    me = treesiplibs.SelfieIP()
    log.Info("Good to go, my ip is " + me.String())

    // Lets prepare a address at any address at port bchainlibs.MinerPort
    ServerAddr,err := net.ResolveUDPAddr(bchainlibs.Protocol, bchainlibs.MinerPort)
    treesiplibs.CheckError(err, log)

    // Now listen at selected port
    ServerConn, err := net.ListenUDP(bchainlibs.Protocol, ServerAddr)
    treesiplibs.CheckError(err, log)
    defer ServerConn.Close()

    // Run the Input!
    go attendInputChannel()
	// THE Miner
    go attendMiningChannel()
	// Run the Output channel! The direct messages to the router layer
	go attendOutputChannel()

    buf := make([]byte, 1024)

    for {
		n,_,err := ServerConn.ReadFromUDP(buf)
		input <- string(buf[0:n])
		treesiplibs.CheckError(err, log)
    }

    close(input)
    close(mining)
    close(output)

    <-done
}

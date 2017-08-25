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
)


// +++++++++++++++++++++++++++
// +++++++++ Go-Logging Conf
// +++++++++++++++++++++++++++
var log = logging.MustGetLogger("miner")

// +++++++++ Global vars
var me net.IP = net.ParseIP(bchainlibs.LocalhostAddr)
var miningRetries = 100
var miningWaitTime = 100
var lastBlock bchainlibs.Packet = bchainlibs.Packet{}

// +++++++++ Channels
// For the Miner the Input and Output will be to Router
var input = make(chan string)
var output = make(chan string)
var done = make(chan bool)

var mining = make(chan string)

var randomGen = rand.New( rand.NewSource(time.Now().UnixNano()) )

// +++++++++ Unverified Blocks MAP with sync
var unverifiedBlocks = bchainlibs.MapBlocks{ make(map[string]bchainlibs.Packet), sync.RWMutex{} }


func toOutput(payload bchainlibs.Packet) {
	log.Debug("Sending Packet with TID " + payload.TID + " to channel output")
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
		tid := payload.TID

		switch payload.Type {

		case bchainlibs.UBlockType:
			if !unverifiedBlocks.Has(tid) { // If it does not exists then
				log.Debug("New Unverified Block to mine")
				unverifiedBlocks.Add(tid, payload)

				randNum := randomGen.Intn(10000)
				coin := randNum % 2
				log.Debug("Random Number is " + strconv.Itoa(randNum))
				log.Debug("Coin toss is " + strconv.Itoa(coin))
				if coin == 0 {
					log.Debug("Mining true for " + tid)
					log.Debug("unverifiedBlocks => " + unverifiedBlocks.String())
					go func() {
						mining <- tid
					}()
				}
			} else {
				log.Info("RedundantTid = " + tid)
			}
		break

		case bchainlibs.LastBlockType:
			if unverifiedBlocks.Has(tid) { // If it does exists then
				unverifiedBlocks.Del(tid)
			}

			lastBlock = payload
		break

		}

	} else {
	    log.Debug("closing channel")
	    done <- true
	    return
	}

    }
}


// Function that handles the buffer channel
func attendMiningChannel() {
	log.Debug("Starting mining channel")
	for {
		j, more := <-mining
		if more {
			// First we take the json, unmarshal it to an object
			block := unverifiedBlocks.Get(j)
			foundIt := false
			log.Debug("Mining " + block.TID)
			for i := 0; i < miningRetries ; i++  {
				if !foundIt {
					h := sha256.New()
					randString := bchainlibs.RandString(20)
					cryptoPuzzle := lastBlock.BID + block.TID + randString
					h.Write([]byte( cryptoPuzzle ))
					checksum := h.Sum(nil)
					if strings.Contains(string(checksum), "00") {
						log.Debug("Mining WIN => " + cryptoPuzzle)
						log.Debug("Checksum => " + string(checksum))

						foundIt = true

						verified := bchainlibs.AssembleVerifiedBlock(block, lastBlock.BID, randString, cryptoPuzzle, me)
						toOutput( verified )

						unverifiedBlocks.Del(block.TID)

						log.Debug("unverifiedBlocks => " + unverifiedBlocks.String())
					}
				}
			}

			if !foundIt {
				log.Debug("Rock and roll then")
				go func() {
					mining <- block.TID
				}()

				duration := randomGen.Intn(100000) / miningWaitTime
				log.Debug("Repeat mining! But first waiting for " + strconv.Itoa(duration) + "ms")
				time.Sleep( time.Millisecond * time.Duration( duration ) )
			}

		} else {
			log.Debug("closing channel")
			done <- true
			return
		}

	}
}

//
//
//func eqIp( a net.IP, b net.IP ) bool {
//    return treesiplibs.CompareIPs(a, b)
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
	miningWaitTime = c.MiningWait

	// Logger configuration
	f := bchainlibs.PrepareLog( "miner" )
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

    // Lets prepare a address at any address at port 10000
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

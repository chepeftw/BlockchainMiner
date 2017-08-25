package main

import (
	"github.com/chepeftw/bchainlibs"
	"net"
	"github.com/chepeftw/treesiplibs"
	"encoding/json"
	"github.com/op/go-logging"
)

var log2 = logging.MustGetLogger("test")

func main() {

	// Logger configuration
	f := bchainlibs.PrepareLog( "test" )
	defer f.Close()
	backend := logging.NewLogBackend(f, "", 0)
	backendFormatter := logging.NewBackendFormatter(backend, bchainlibs.LogFormat)
	backendLeveled := logging.AddModuleLevel(backendFormatter)
	backendLeveled.SetLevel(logging.DEBUG, "")
	logging.SetBackend( backendLeveled )

	var me net.IP = net.ParseIP(bchainlibs.LocalhostAddr)

	packet := bchainlibs.AssembleUnverifiedBlock(me, "data", "function")

	packet2 := bchainlibs.AssembleUnverifiedBlock(me, "data", "function")
	packet2.Type = bchainlibs.InternalVBlockType

	packet3 := bchainlibs.AssembleUnverifiedBlock(me, "data", "function")
	packet3.Type = bchainlibs.UBlockType

	packet4 := bchainlibs.AssembleUnverifiedBlock(me, "data", "function")
	packet4.Type = bchainlibs.VBlockType

	packet5 := bchainlibs.AssembleUnverifiedBlock(me, "data", "function")
	packet5.Type = bchainlibs.LastBlockType

	packet6 := bchainlibs.AssembleUnverifiedBlock(me, "data", "function")
	packet6.Type = bchainlibs.QueryType



	Server,err := net.ResolveUDPAddr(bchainlibs.Protocol, bchainlibs.LocalhostAddr + bchainlibs.MinerPort)
	treesiplibs.CheckError(err, log2)
	Local, err := net.ResolveUDPAddr(bchainlibs.Protocol, me.String() + bchainlibs.LocalPort)
	treesiplibs.CheckError(err, log2)
	Conn, err := net.DialUDP(bchainlibs.Protocol, Local, Server)
	treesiplibs.CheckError(err, log2)
	defer Conn.Close()

	send(packet, Conn)
	send(packet2, Conn)
	send(packet3, Conn)
	send(packet4, Conn)
	send(packet5, Conn)
	send(packet6, Conn)

}

func send(packet bchainlibs.Packet, conn net.Conn) {
	if conn != nil {
		js, err := json.Marshal(packet)
		treesiplibs.CheckError(err, log2)

		buf := []byte(js)
		_,err = conn.Write(buf)
		treesiplibs.CheckError(err, log2)
	}
}
package raft

import (
	"bytes"
	"errors"
	"github.com/hashicorp/go-msgpack/codec"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// PacketConnTransport is a transport that runs using a net.PacketConn
// and copes with fragmenting packets. It assuminges that an address
// translator is supplied, that converts a peer sting into a net.Addr
// Most of the work is done in the PakcetConnRPC struct.
type PacketConnTransport struct {
	DatagramTransport                                     // Embedded DatagramTransport
	peerslock         sync.RWMutex                        // Lock to protect peers
	peers             map[string]net.Addr                 // Translation between peer and net.Addr
	AddrTranslator    func(addr string) (net.Addr, error) // function to translate address strings into net.Addr
}

// PacketConn transport is an implementation of DatagramRPC using the PacketConn interface
type PacketConnRPC struct {
	trans *PacketConnTransport // the underlying datagram transport

	localaddr  string         // local address
	pc         net.PacketConn // the packet connection. We close this once started
	encodeCh   chan ToSend    // channel representing data to be encoded and sent
	tx         chan Packet    // encoded data waiting to be sent
	rx         chan Packet    // encoded data received and waiting to be decoded
	shutdownCh chan struct{}  // shutdown channel - close to indicate it should stop
	wg         sync.WaitGroup // waitgroup for the 4 main worker go-routines

	commslock sync.Mutex            // mutex protecting comms map
	comms     map[string]*pcrpcComm // map of unique IDs to comms structure

	CachedAddrTranslatorFunc func(addr string) (net.Addr, error) // function to translate address strings into net.Addr

	MaxSize uint64 // maximum size of a packet

	CloseFunc func(*PacketConnRPC) // nil or function to replace Close() on p.pc
}

// these are used for debugging only. They are in pairs that should be equal
// unless there is loss in the system. The pairs are related to other pairs
// (for instance one would expect the number of requets and repies, to match,
// and the number of raw datagrams and fragments to match)
type packetConnStats struct {
	txrpcreq int64 // transmitted rpc requests
	rxrpcreq int64 // received rpc requets

	txrpcrep int64 // transmitted rpc replies
	rxrpcrep int64 // received rpc replies

	rpcfragandsend int64 // packets sent to be fragmented
	processcomm    int64 // packets that have been reassembled

	dgtx int64 // raw network datagrams transmitted
	dgrx int64 // raw network datagrams received

	frtx int64 // fragments sent to transmit as raw datagrams
	frrx int64 // fragments received as raw datagrams
}

// instance of internal stats object
var pcstats packetConnStats

// An encoded fragmented packet structure
type Packet struct {
	addr net.Addr // the source or destination address
	data []byte   // the data
}

// A non-encoded non-fragmented message structure
type ToSend struct {
	packetType int      // the type of the packet
	requestId  string   // the packet Id
	addr       net.Addr // the destination
	RPC                 // embedded RPC structure. For replies we reuse the Command field
}

// struct to track a communication and reassemble all fragments
type pcrpcComm struct {
	respCh      chan<- RPCResponse // channel the response should be sent to
	resp        []byte             // the data
	totalLength uint64             // total length of all fragments
	lengthLeft  uint64             // length left to assemble
	fragmap     map[uint64]bool    // map of fragment start offsets, to avoid dupes
	packetType  int                // packet type
	expiry      time.Time          // when the item should be removed from the map
	addr        net.Addr           // address it came from
}

// Decoded header
type packetConnHeader struct {
	RequestId   string   // unique ID of request
	TotalLength uint64   // total length of all fragments
	StartOffset uint64   // start offset of this fragment
	Length      uint64   // length of this fragment
	PacketType  int      // packet type
	_struct     struct{} `codec:",toarray"` // mysterious tag for codec to encode more efficiently
}

// Indicators of the type of request
const (
	PCH_REQUEST = iota // an RPC request
	PCH_REPLY   = iota // an RPC reply
)

// Various errors
var (
	ErrReceiveTimeout           = errors.New("Receive timeout")
	ErrTransmitTimeout          = errors.New("Transmit timeout")
	ErrRequestProcessingTimeout = errors.New("Request processing timeout")
	ErrReplyProcessingTimeout   = errors.New("Reply processing timeout")
	ErrNoResponseChannel        = errors.New("No response channel")
	ErrUnknownPacketType        = errors.New("Unknown packet type")
	ErrMismatchedFragment       = errors.New("Mismatched packet fragment")
	ErrMismatchedReply          = errors.New("Mismatched packet reply")
	ErrTooMuchFragmentData      = errors.New("Too much data in fragment chain")
	ErrCommandEnqueueTimeout    = errors.New("Command enqueue timeout")
)

// Connect is used to connect this transport to another transport for
// a given peer name. This allows for local routing.
func (tr *PacketConnTransport) Connect(peer string, tr2 Transport) {
	addr, err := tr.AddrTranslator(peer)
	if err != nil {
		return
	}
	tr.peerslock.Lock()
	defer tr.peerslock.Unlock()
	tr.peers[peer] = addr
}

// Disconnect is used to remove the ability to route to a given peer.
func (tr *PacketConnTransport) Disconnect(peer string) {
	tr.peerslock.Lock()
	defer tr.peerslock.Unlock()
	delete(tr.peers, peer)
}

// DisconnectAll is used to remove all routes to peers.
func (tr *PacketConnTransport) DisconnectAll() {
	tr.peerslock.Lock()
	defer tr.peerslock.Unlock()
	for peer, _ := range tr.peers {
		delete(tr.peers, peer)
	}
}

// CachedAdddressTranslator reads the address from the cached.
func (tr *PacketConnTransport) CachedAddressTranslator(peer string) (net.Addr, error) {
	tr.peerslock.Lock()
	defer tr.peerslock.Unlock()
	addr, ok := tr.peers[peer]
	if !ok {
		return nil, ErrUnknownPeer
	}
	return addr, nil
}

// Permanently close the transport (and close the RPC layer too)
func (tr *PacketConnTransport) Close() error {
	tr.DisconnectAll()
	return tr.rpc.Close()
}

// Create a new transport with the given address translator and function to create a datagram transport
func NewPacketConnTransport(addrTranslator func(addr string) (net.Addr, error), datagramTransportCreator func() *DatagramTransport) *PacketConnTransport {
	tr := &PacketConnTransport{
		peers:             make(map[string]net.Addr),
		AddrTranslator:    addrTranslator,
		DatagramTransport: *datagramTransportCreator(),
	}
	tr.rpc.(*PacketConnRPC).trans = tr
	tr.rpc.(*PacketConnRPC).CachedAddrTranslatorFunc = tr.CachedAddressTranslator
	return tr
}

// implementation of asynchronous call interface
func (p *PacketConnRPC) CallAsync(transport *DatagramTransport, target string, args interface{}, data io.Reader, responseChan chan<- RPCResponse, timeout <-chan time.Time, shutdown <-chan struct{}) error {
	if p.CachedAddrTranslatorFunc == nil {
		return ErrUnknownPeer
	}
	addr, err := p.CachedAddrTranslatorFunc(target)
	if err != nil {
		return err
	}
	if addr == nil {
		return ErrUnknownPeer
	}
	rpc := ToSend{
		RPC: RPC{
			Command:  args,
			Reader:   data,
			RespChan: responseChan,
		},
		addr:       addr,
		packetType: PCH_REQUEST,
		requestId:  generateUUID(),
	}

	atomic.AddInt64(&pcstats.txrpcreq, 1)

	// Send the RPC over
	select {
	case p.encodeCh <- rpc:
		break
	case <-timeout:
		return ErrCommandEnqueueTimeout
	case <-shutdown:
		return ErrPipelineShutdown
	}
	return nil
}

// translate the local address into a string
func (p *PacketConnRPC) LocalAddr() string {
	return p.localaddr
}

// expire fragments that relate to before the RPC timeout
// an LLRB tree would arguably be more efficient save for the fact
// we're only going to run this once a second
func (p *PacketConnRPC) expireFragments() {
	now := time.Now()
	p.commslock.Lock()
	for k, v := range p.comms {
		if v.expiry.Before(now) {
			p.trans.Logger.Printf("[WARN] raft-packetconn: expiring fragment chain with id=%s", k)
			delete(p.comms, k)
		}
	}
	p.commslock.Unlock()
}

// processcomm process a reassembled inbound packet, either a request or a reply
func (p *PacketConnRPC) processComm(c *pcrpcComm, requestId string) error {
	atomic.AddInt64(&pcstats.processcomm, 1)
	switch c.packetType {
	case PCH_REQUEST:
		rpc, err := p.trans.DecodeRequest(c.resp)
		if err != nil {
			return err
		}
		respChan := make(chan RPCResponse)
		rpc.RespChan = respChan

		go func() {
			// do not select() here, as if shutdownCh is closed we would merely close
			// respChan, which is unhelpful as the sender panics
			if response, ok := <-respChan; ok {
				defer close(respChan) // ends the baby go-routine we are going to launch
				rpctimeout := time.After(p.trans.timeout)
				tosend := ToSend{
					RPC:        RPC{Command: response},
					addr:       c.addr,
					packetType: PCH_REPLY,
					requestId:  requestId,
				}
				select {
				case <-rpctimeout:
					return
				case <-p.shutdownCh:
					return
				case p.encodeCh <- tosend:
					atomic.AddInt64(&pcstats.txrpcrep, 1)
					return
				}
			}
		}()

		timeout := time.After(p.trans.timeout)
		select {
		case <-timeout:
			return ErrRequestProcessingTimeout
		case <-p.shutdownCh:
			return ErrPipelineShutdown
		case p.trans.consumerCh <- *rpc:
			atomic.AddInt64(&pcstats.rxrpcreq, 1)
			return nil
		}
	case PCH_REPLY:
		if c.respCh == nil {
			return ErrNoResponseChannel
		}

		response, err := p.trans.DecodeResponse(c.resp)
		if err != nil {
			return err
		}
		timeout := time.After(p.trans.timeout)
		select {
		case <-timeout:
			return ErrReplyProcessingTimeout
		case <-p.shutdownCh:
			return ErrPipelineShutdown
		case c.respCh <- *response:
			atomic.AddInt64(&pcstats.rxrpcrep, 1)
			return nil
		}

	default:
		return ErrUnknownPacketType
	}
	return nil
}

// receiveFragment associates a fragment with an in-transit communication (or adds a new one)
// if the communication now has all fragments, the communication is processed
func (p *PacketConnRPC) receiveFragment(hdr *packetConnHeader, data []byte, addr net.Addr) error {
	atomic.AddInt64(&pcstats.frrx, 1)
	if hdr.TotalLength == 0 || hdr.Length == 0 || hdr.StartOffset+hdr.Length > hdr.TotalLength {
		p.trans.Logger.Println("[WARN] raft-packetconn: receiveFragment fragment sanity error")
		return nil
	}
	p.commslock.Lock()
	c := p.comms[hdr.RequestId]
	if c == nil {
		if hdr.PacketType == PCH_REPLY {
			p.commslock.Unlock()
			return ErrMismatchedReply
		}
		c = &pcrpcComm{
			totalLength: hdr.TotalLength,
			lengthLeft:  hdr.TotalLength,
			packetType:  hdr.PacketType,
			fragmap:     make(map[uint64]bool),
			resp:        make([]byte, hdr.TotalLength),
			expiry:      time.Now().Add(p.trans.timeout),
		}
		p.comms[hdr.RequestId] = c
	} else if c.totalLength == 0 {
		// put there as we are expecting a reply
		c.totalLength = hdr.TotalLength
		c.packetType = hdr.PacketType
		c.lengthLeft = hdr.TotalLength
		c.fragmap = make(map[uint64]bool)
		c.resp = make([]byte, hdr.TotalLength)
		c.expiry = time.Now().Add(p.trans.timeout)
	} else if hdr.TotalLength != c.totalLength || hdr.PacketType != c.packetType {
		// just ignore this packet, it has something wrong with it
		p.commslock.Unlock()
		return ErrMismatchedFragment
	}
	p.commslock.Unlock()

	c.addr = addr
	if c.fragmap[hdr.StartOffset] {
		return nil // this is a dupe
	}
	c.fragmap[hdr.StartOffset] = true
	copy(c.resp[hdr.StartOffset:hdr.StartOffset+hdr.Length], data)
	c.lengthLeft -= hdr.Length

	if c.lengthLeft < 0 {
		p.commslock.Lock()
		delete(p.comms, hdr.RequestId)
		p.commslock.Unlock()
		return ErrTooMuchFragmentData
	} else if c.lengthLeft == 0 {
		p.commslock.Lock()
		delete(p.comms, hdr.RequestId)
		p.commslock.Unlock()
		return p.processComm(c, hdr.RequestId)
	}

	return nil
}

// decoder is a goroutine that transfers data from the encoded rx channel and processes the result.
// possible processing actions include sending a request to the consumer channel, or sending a reply to the reply channel
// this go-routine also does some housekeeping work expiring fragments and cached names
func (p *PacketConnRPC) decoder() {
	defer p.wg.Done()
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			p.expireFragments()
		case <-p.shutdownCh:
			return
		case packet, ok := <-p.rx:
			if !ok {
				return
			}
			buf := bytes.NewBuffer(packet.data)
			dec := codec.NewDecoder(buf, &codec.MsgpackHandle{})
			var header packetConnHeader
			if err := dec.Decode(&header); err != nil {
				p.trans.Logger.Printf("[WARN] raft-packetconn: decoder header: %s", err)
			} else {
				if err := p.receiveFragment(&header, buf.Bytes(), packet.addr); err != nil {
					p.trans.Logger.Printf("[WARN] raft-packetconn: decoder: receiveFragment %s", err)
				}
			}
		}
	}
}

// fragment a packet and send it to a given address by putting it on the encoded tx channel
func (p *PacketConnRPC) fragmentAndSend(tosend ToSend, buf []byte) error {
	hdr := packetConnHeader{
		RequestId:   tosend.requestId,
		TotalLength: uint64(len(buf)),
		StartOffset: 0,
		PacketType:  tosend.packetType,
	}
	atomic.AddInt64(&pcstats.rpcfragandsend, 1)
	for ; hdr.StartOffset < hdr.TotalLength; hdr.StartOffset += hdr.Length {
		hdr.Length = hdr.TotalLength - hdr.StartOffset
		if hdr.Length > p.MaxSize {
			hdr.Length = p.MaxSize
		}
		pkt := Packet{addr: tosend.addr, data: nil}
		enc := codec.NewEncoderBytes(&pkt.data, &codec.MsgpackHandle{})
		if err := enc.Encode(hdr); err != nil {
			return err
		}
		pkt.data = append(pkt.data, buf[hdr.StartOffset:hdr.StartOffset+hdr.Length]...)
		timeout := time.After(p.trans.timeout)
		select {
		case <-timeout:
			return ErrRequestProcessingTimeout
		case <-p.shutdownCh:
			return ErrPipelineShutdown
		case p.tx <- pkt:
			atomic.AddInt64(&pcstats.frtx, 1)
		}
	}
	return nil
}

// encode is a goroutine that takes the unencoded data from encodeCh, encodes it, and puts it on
// the encoded tx channel
func (p *PacketConnRPC) encoder() {
	defer p.wg.Done()
	for {
		select {
		case <-p.shutdownCh:
			return
		case tosend, ok := <-p.encodeCh:
			if !ok {
				return
			}
			switch tosend.packetType {
			case PCH_REQUEST:
				if buf, err := p.trans.EncodeRequest(&tosend.RPC); err != nil {
					p.trans.Logger.Printf("[WARN] raft-packetconn: encoder %s", err)
					continue
				} else {
					p.commslock.Lock()
					p.comms[tosend.requestId] = &pcrpcComm{
						respCh: tosend.RPC.RespChan,
					}
					p.commslock.Unlock()
					if err := p.fragmentAndSend(tosend, buf); err != nil {
						p.trans.Logger.Printf("[WARN] raft-packetconn: encoder send %s", err)
						continue
					}
				}
			case PCH_REPLY:
				if response, ok := tosend.RPC.Command.(RPCResponse); !ok {
					p.trans.Logger.Printf("[WARN] raft-packetconn: encoder RPC response does not actually contain a response")
					continue
				} else {
					if buf, err := p.trans.EncodeResponse(&response); err != nil {
						p.trans.Logger.Printf("[WARN] raft-packetconn: encoder %s", err)
						continue
					} else if err := p.fragmentAndSend(tosend, buf); err != nil {
						p.trans.Logger.Printf("[WARN] raft-packetconn: encoder Send %s", err)
						continue
					}
				}
			default:
				p.trans.Logger.Printf("[WARN] raft-packetconn: encoder Unknown packet type")
			}

		}
	}
}

// sender() reads from the encoded tx channel and writes to the socket
func (p *PacketConnRPC) sender() {
	defer p.wg.Done()
	for {
		select {
		case <-p.shutdownCh:
			return
		case msg, ok := <-p.tx:
			if !ok {
				return
			}
			atomic.AddInt64(&pcstats.dgtx, 1)
			for cnt := 0; ; cnt++ {
				if _, err := p.pc.WriteTo(msg.data, msg.addr); err != nil {
					// it might just be that the channel is shutdown, in which case we ignore the error
					select {
					case <-p.shutdownCh:
						return
					}
					if e, ok := err.(net.Error); ok {
						if !e.Temporary() {
							p.trans.Logger.Printf("[WARN] raft-packetconn: sender net-perm len=%d: %d %s", cnt, len(msg.data), err)
							break
						}
						p.trans.Logger.Printf("[WARN] raft-packetconn: sender cnt=%d len=%d: %s", cnt, len(msg.data), err)
						time.Sleep(1 * time.Millisecond)
						select {
						case <-p.shutdownCh:
							return
						default:
						}
					} else {
						p.trans.Logger.Printf("[WARN] raft-packetconn: sender non-net len=%d: %d %s", cnt, len(msg.data), err)
						break
					}
				} else {
					break
				}
			}
		}
	}
}

// receiver reads from the socket and writes to the encoded tx channel
func (p *PacketConnRPC) receiver() {
	defer p.wg.Done()
	for {
		select {
		case <-p.shutdownCh:
			return
		default:
		}
		packet := Packet{
			data: make([]byte, p.MaxSize+1024), // extra 1024 for header space - no easy way to find this out in advance
		}
		if n, addr, err := p.pc.ReadFrom(packet.data); err != nil {
			// it might just be that the channel is shutdown, in which case we ignore the error
			select {
			case <-p.shutdownCh:
				return
			}
			if e, ok := err.(net.Error); ok {
				p.trans.Logger.Printf("[WARN] raft-packetconn: receiver %s", err)
				if !e.Temporary() {
					return
				}
				time.Sleep(10 * time.Millisecond)
			} else {
				p.trans.Logger.Printf("[WARN] raft-packetconn non-net: receiver %s", err)
				return
			}
		} else if n != 0 {
			packet.data = packet.data[0:n]
			packet.addr = addr
			// now try to insert it into the rx channel
			for {
				select {
				case <-p.shutdownCh:
					return
				case p.rx <- packet:
					atomic.AddInt64(&pcstats.dgrx, 1)
					goto done
				}
			}
		done:
		}
	}
}

// Close() closes a running packetconn RPC session
func (p *PacketConnRPC) Close() error {
	close(p.shutdownCh)
	if p.CloseFunc == nil {
		p.pc.Close()
	} else {
		p.CloseFunc(p)
	}
	p.wg.Wait()
	close(p.tx)
	close(p.rx)
	close(p.encodeCh)
	return nil
}

// Internal routine to print statistics out. Useful for debugging
func (p *PacketConnRPC) statsPrinter() {
	defer p.wg.Done()
	t := time.NewTicker(250 * time.Millisecond)
	for {
		select {
		case <-t.C:
			p.trans.Logger.Printf("[DEBUG]: raft-PacketConnRPC: req=%d,%d, rep=%d,%d fragandsend=%d processcomm=%d dg=%d,%d fr=%d,%d",
				atomic.LoadInt64(&pcstats.txrpcreq), atomic.LoadInt64(&pcstats.rxrpcreq),
				atomic.LoadInt64(&pcstats.txrpcrep), atomic.LoadInt64(&pcstats.rxrpcrep),
				atomic.LoadInt64(&pcstats.rpcfragandsend), atomic.LoadInt64(&pcstats.processcomm),
				atomic.LoadInt64(&pcstats.dgtx), atomic.LoadInt64(&pcstats.dgrx),
				atomic.LoadInt64(&pcstats.frtx), atomic.LoadInt64(&pcstats.frrx))
		case <-p.shutdownCh:
			return
		}
	}
}

// Build a new PacketConnRPC with the given (open) net.PacketConn and an address translator. Note that once this is called,
// the caller loses responsibility for the net.PacketConn (unless error is returned), i.e. we close it. Closing it is achieved
// by calling Close()
func NewPacketConnRPC(pc net.PacketConn, localaddr string, closeFunc func(*PacketConnRPC)) (*PacketConnRPC, error) {
	p := &PacketConnRPC{
		localaddr:  localaddr,
		tx:         make(chan Packet, 64),
		rx:         make(chan Packet, 64),
		encodeCh:   make(chan ToSend, 64),
		shutdownCh: make(chan struct{}),
		comms:      make(map[string]*pcrpcComm),
		MaxSize:    1024,
		pc:         pc,
		CloseFunc:  closeFunc,
	}
	p.pc.SetDeadline(time.Time{})
	p.wg.Add(4)
	go p.sender()
	go p.receiver()
	go p.decoder()
	go p.encoder()

	return p, nil
}

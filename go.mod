module github.com/pilot-protocol/webhook

go 1.25.10

require github.com/pilot-protocol/handshake v0.2.1

require (
	github.com/pilot-protocol/rendezvous v0.2.4 // indirect
	github.com/pilot-protocol/trustedagents v0.2.3 // indirect
	golang.org/x/sys v0.45.0 // indirect
)

require github.com/pilot-protocol/common v0.4.8

replace github.com/pilot-protocol/pilotprotocol => ../web4

replace github.com/pilot-protocol/common => ../common

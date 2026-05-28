module github.com/pilot-protocol/webhook

go 1.25.10

require github.com/pilot-protocol/handshake v0.1.0

require (
	github.com/TeoSlayer/pilotprotocol v0.0.0 // indirect
	github.com/pilot-protocol/common v0.2.0
)

replace github.com/TeoSlayer/pilotprotocol => ../web4

replace github.com/pilot-protocol/common => ../common

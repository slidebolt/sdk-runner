module github.com/slidebolt/sdk-runner

go 1.25.7

require (
	github.com/nats-io/nats.go v1.49.0
	github.com/slidebolt/sdk-types v0.0.0
)

replace github.com/slidebolt/sdk-types => ../sdk-types

require (
	github.com/klauspost/compress v1.18.2 // indirect
	github.com/nats-io/nkeys v0.4.12 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	golang.org/x/crypto v0.46.0 // indirect
	golang.org/x/sys v0.39.0 // indirect
)

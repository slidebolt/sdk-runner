module github.com/slidebolt/sdk-runner

go 1.25.7

require (
	github.com/nats-io/nats.go v1.49.0
	github.com/robfig/cron/v3 v3.0.1
	github.com/slidebolt/registry v0.0.0-00010101000000-000000000000
	github.com/slidebolt/sdk-types v1.20.3
)

require (
	github.com/klauspost/compress v1.18.4 // indirect
	github.com/nats-io/nkeys v0.4.15 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	golang.org/x/crypto v0.48.0 // indirect
	golang.org/x/sys v0.42.0 // indirect
)

replace github.com/slidebolt/registry => ../registry

replace github.com/slidebolt/sdk-types => ../sdk-types

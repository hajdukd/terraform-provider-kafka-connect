TEST?=./...
GOFMT_FILES?=$$(find . -name '*.go' |grep -v vendor)

default: build

build:
	go install

test:
	go test ./...

install-local:
	./xtest/install-local --arch all

upload-provider:
	./xtest/upload-provider

.PHONY: build test install-local upload-provider

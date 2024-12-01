.PHONY: test vet go_vendor go_mod

all: test build

build:
	go build --tags fts5 -gcflags="all=-trimpath=${PWD}" -asmflags="all=-trimpath=${PWD}" -ldflags "-X 'main.UserName=magnet' -s -w" ./

vet:
	go vet ./

test:
	CGO_ENABLED=1 go test -v -race ./

go_vendor:
	go mod vendor

go_mod:
	go mod tidy


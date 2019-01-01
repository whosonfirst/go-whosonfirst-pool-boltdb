prep:
	if test -d pkg; then rm -rf pkg; fi

self:   prep
	if test -d src; then rm -rf src; fi
	mkdir -p src/github.com/whosonfirst/go-whosonfirst-pool-boltdb
	cp *.go src/github.com/whosonfirst/go-whosonfirst-pool-boltdb/
	cp -r vendor/* src/

rmdeps:
	if test -d src; then rm -rf src; fi 

deps:   prep
	@GOPATH=$(shell pwd) go get "github.com/whosonfirst/go-whosonfirst-pool"
	@GOPATH=$(shell pwd) go get "github.com/boltdb/bolt"

vendor-deps: rmdeps deps
	if test ! -d src; then mkdir src; fi
	if test -d vendor; then rm -rf vendor; fi
	cp -r src vendor
	find vendor -name '.git' -print -type d -exec rm -rf {} +
	rm -rf src
fmt:
	go fmt *.go
	go fmt cmd/*.go

bin: 	self
	@GOPATH=$(shell pwd) go build -o bin/int-pool cmd/int-pool.go

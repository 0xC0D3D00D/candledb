COVERPROFILE = ./coverage.out

all: clean test build

build: 
	CGO_ENABLED=0 go build -o bin/ ./cmd/...

run:
	go run ./cmd/server

migrate:
	dbmate up

test:
	go test -v --race -coverpkg=./... -coverprofile=$(COVERPROFILE) ./...

test-integration:
	go test -v -race -tags integration -coverprofile=coverage.out ./...

test-view-coverage: test
	go tool cover -html=$(COVERPROFILE)

validate:
	go vet ./...

fmt: validate
	gofmt -l -s -w ./
	go run mvdan.cc/gofumpt@v0.4.0 -l -w .
	go run golang.org/x/tools/cmd/goimports@v0.11.0 -l -w .
	go run github.com/daixiang0/gci@v0.10.1 write . --skip-generated

clean: fmt
	go mod tidy

proto-gen:
	cd ./apis && buf lint && buf format -w && buf generate

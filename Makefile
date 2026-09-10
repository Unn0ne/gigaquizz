.PHONY: dev run build test check file-test e2e

dev:
	./scripts/dev.sh

run:
	go run ./cmd/gigaquizz -env "$${GIGAQUIZZ_ENV_FILE:-.env.simple-files}"

build:
	go build -o bin/gigaquizz ./cmd/gigaquizz
	go build -o bin/loadtest ./cmd/loadtest
	go build -o bin/corebench ./cmd/corebench
	go build -o bin/filebench ./cmd/filebench

test:
	go test -race -count=1 ./...

check:
	go vet ./...
	go test -race -count=1 ./...

file-test:
	go test -race -count=1 ./internal/filelog ./internal/filestore ./cmd/filebench

e2e:
	npm run test:e2e

.PHONY: dev run build test integration check lab-up lab-status lab-stop replication-test kafka-up kafka-status kafka-test frame-test file-test capacity-check app-kafka-test e2e

GIGAQUIZZ_ENV_FILE ?= .env.postgres-kafka
export GIGAQUIZZ_ENV_FILE

dev:
	./scripts/dev.sh

run:
	go run ./cmd/gigaquizz -env "$$GIGAQUIZZ_ENV_FILE"

build:
	go build -o bin/gigaquizz ./cmd/gigaquizz
	go build -o bin/loadtest ./cmd/loadtest
	go build -o bin/storagebench ./cmd/storagebench
	go build -o bin/logbench ./cmd/logbench
	go build -o bin/corebench ./cmd/corebench
	go build -o bin/framebench ./cmd/framebench
	go build -o bin/filebench ./cmd/filebench

test:
	go test -race ./...

integration:
	@test -n "$(TEST_DATABASE_URL)" || (echo 'Set TEST_DATABASE_URL to an isolated test database'; exit 1)
	go test -race -count=1 ./internal/postgres

check:
	go vet ./...
	go test -race ./...

lab-up:
	python3 scripts/replication_lab.py up

lab-status:
	python3 scripts/replication_lab.py status

lab-stop:
	python3 scripts/replication_lab.py stop

replication-test:
	GIGAQUIZZ_REPLICATION_TEST=1 go test -race -count=1 -run TestReplicatedDurability ./internal/postgres

kafka-up:
	python3 scripts/kafka_lab.py up

kafka-status:
	python3 scripts/kafka_lab.py status

kafka-test:
	GIGAQUIZZ_KAFKA_TEST=1 go test -race -count=1 ./internal/votelog

app-kafka-test:
	@test -n "$(TEST_DATABASE_URL)" || (echo 'Set TEST_DATABASE_URL for isolated application integration'; exit 1)
	GIGAQUIZZ_APP_KAFKA_TEST=1 go test -race -count=1 -v ./internal/kafkapoll

e2e:
	npm run test:e2e

frame-test:
	GIGAQUIZZ_KAFKA_TEST=1 go test -race -count=1 ./internal/votelog ./cmd/framebench

file-test:
	go test -race -count=1 ./internal/filelog ./cmd/filebench

capacity-check:
	python3 -m unittest discover -s scripts/tests -p 'test_production_capacity.py'

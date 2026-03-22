VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
COMMIT  ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
DATE    ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
PKG      = github.com/lynxbase/lynxdb/internal/buildinfo
LDFLAGS  = -X $(PKG).Version=$(VERSION) -X $(PKG).Commit=$(COMMIT) -X $(PKG).Date=$(DATE)

CUSTOM_GCL = ./custom-gcl

.PHONY: build test test-unit test-e2e test-cli vet clean lint lint-build

build:
	go build -ldflags "$(LDFLAGS)" -o lynxdb ./cmd/lynxdb/

test: test-unit test-e2e test-cli

test-unit:
	go test ./... -count=1 -timeout 120s -race

test-e2e:
	go test -tags e2e -count=1 -timeout 180s ./test/e2e/

test-cli: build
	go test -tags clitest -count=1 -timeout 300s ./test/cli/

vet:
	go vet ./...

lint-build:
	$(shell go env GOPATH)/bin/golangci-lint custom

lint: $(CUSTOM_GCL)
	$(CUSTOM_GCL) run ./...

$(CUSTOM_GCL): .custom-gcl.yml
	$(shell go env GOPATH)/bin/golangci-lint custom

clean:
	rm -f lynxdb custom-gcl

.PHONY: bench-micro bench-fixtures bench-macro

bench-micro:
	@mkdir -p artifacts
	go test -bench=. -benchmem -count=5 -timeout 300s \
		./pkg/engine/pipeline/... \
		./pkg/spl2/... \
		./pkg/ingest/pipeline/... \
		| tee artifacts/bench-micro.txt

bench-fixtures:
	go run scripts/genbench/main.go

bench-macro: build
	@mkdir -p artifacts
	./lynxdb bench --events 1000000 | tee artifacts/bench-macro.txt

.PHONY: webui-install webui webui-dev

webui-install:
	cd web && bun install

webui: webui-install
	cd web && bun run build
	rm -rf internal/webui/dist
	cp -r web/dist internal/webui/dist

webui-dev:
	cd web && bun run dev

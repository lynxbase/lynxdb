VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
COMMIT  ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
DATE    ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
PKG      = github.com/lynxbase/lynxdb/internal/buildinfo
LDFLAGS  = -X $(PKG).Version=$(VERSION) -X $(PKG).Commit=$(COMMIT) -X $(PKG).Date=$(DATE)

CUSTOM_GCL = ./custom-gcl

.PHONY: build test vet clean lint lint-build

build:
	go build -ldflags "$(LDFLAGS)" -o lynxdb ./cmd/lynxdb/

test:
	go test ./... -count=1 -timeout 120s

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

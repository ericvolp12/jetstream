GO_CMD_W_CGO = CGO_ENABLED=1 GOOS=linux go
GO_CMD = CGO_ENABLED=0 GOOS=linux go

# Build Jetstream
.PHONY: build
build:
	@echo "Building Jetstream Go binary..."
	$(GO_CMD) build -o jetstream cmd/jetstream/*.go

# Run Jetstream
.PHONY: run
run:
	@echo "Running Jetstream..."
	$(GO_CMD) run cmd/jetstream/*.go

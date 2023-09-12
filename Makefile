GO_CMD_W_CGO = CGO_ENABLED=1 GOOS=linux go
GO_CMD = CGO_ENABLED=0 GOOS=linux go

# Build Skypipe
.PHONY: build
build:
	@echo "Building Skypipe Go binary..."
	$(GO_CMD) build -o skypipe cmd/skypipe/*.go

# Run Skypipe
.PHONY: run
run:
	@echo "Running Skypipe..."
	$(GO_CMD) run cmd/skypipe/*.go

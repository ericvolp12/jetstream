GO_CMD_W_CGO = CGO_ENABLED=1 GOOS=linux go
GO_CMD = CGO_ENABLED=0 GOOS=linux go

# Build Jetstream
.PHONY: build
build:
	@echo "Building Jetstream Go binary..."
	$(GO_CMD_W_CGO) build -o jetstream cmd/jetstream/*.go

# Run Jetstream
.PHONY: run
run: .env
	@echo "Running Jetstream..."
	$(GO_CMD_W_CGO) run cmd/jetstream/*.go

.PHONY: up
up:
	@echo "Starting Jetstream..."
	docker compose up -d

.PHONY: rebuild
rebuild:
	@echo "Starting Jetstream..."
	docker compose up -d --build

.PHONY: down
down:
	@echo "Stopping Jetstream..."
	docker compose down

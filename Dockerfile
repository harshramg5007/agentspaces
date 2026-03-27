# Build stage
FROM public.ecr.aws/docker/library/golang:1.22-alpine AS builder

WORKDIR /app

# Install dependencies
RUN apk add --no-cache git

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build the server
RUN go build -o bin/agent-server ./cmd/server

# Runtime stage
FROM public.ecr.aws/docker/library/alpine:3.21

RUN apk --no-cache add ca-certificates wget

WORKDIR /app

# Copy the binary
COPY --from=builder /app/bin/agent-server .

EXPOSE 8080

CMD ["./agent-server"]

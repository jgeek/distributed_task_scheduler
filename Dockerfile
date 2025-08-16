# Build stage
FROM golang:1.24.5-alpine AS builder

WORKDIR /app

# Install git (needed for go mod download)
RUN apk add --no-cache git

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the application (main package is in ./main)
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o app ./main

# Final stage
FROM alpine:latest

# Install ca-certificates for HTTPS requests
RUN apk --no-cache add ca-certificates

WORKDIR /root/

# Copy the built binary from builder
COPY --from=builder /app/app .
# Copy Swagger docs so they are available at runtime
COPY --from=builder /app/docs ./docs

# Expose port (change if your app uses a different port)
EXPOSE 8080
EXPOSE 9900

# Run the binary
ENTRYPOINT ["./app"]

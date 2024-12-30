# Stage 1: Build the Go application
FROM golang:1.23 AS builder

# Set environment variables for a static build
ENV CGO_ENABLED=0 GOOS=linux

# Set the working directory inside the container
WORKDIR /app

# Copy Go modules and install dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy the source code
COPY . .

# Build the gRPC server binary
RUN go build -o woolf ./cmd/server

# Stage 2: Use a minimal container to run the application
FROM gcr.io/distroless/static:nonroot

# Set the working directory inside the final container
WORKDIR /app

# Copy the binary from the builder stage
COPY --from=builder /app/woolf .

# Expose the gRPC port
EXPOSE 9180 9181

# Run the server as a non-root user
USER nonroot:nonroot

# Command to run the gRPC server
CMD ["./woolf"]

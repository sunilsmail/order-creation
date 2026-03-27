FROM node:22-alpine AS builder

WORKDIR /app

# Copy package files
COPY package.json package-lock.json ./

# Install dependencies
RUN npm ci

# Copy source code
COPY . .

# Build Angular application
RUN npm run build

# Runtime stage
FROM node:22-alpine

WORKDIR /app

# Install http-server to serve the built Angular app
RUN npm install -g http-server

# Copy built application from builder
COPY --from=builder /app/dist/order-ui ./dist/order-ui

# Expose port
EXPOSE 4200

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD wget --quiet --tries=1 --spider http://localhost:4200/ || exit 1

# Start HTTP server
CMD ["http-server", "dist/order-ui", "-p", "4200", "--gzip"]

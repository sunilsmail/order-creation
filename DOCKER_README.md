# How to Run the Application with Docker

## Prerequisites
- Docker and Docker Compose installed
- At least 4GB of free memory

## Running the Complete Application Stack

### Using Docker Compose (Recommended)
```bash
# Build and start all services
docker-compose up -d

# View logs for all services
docker-compose logs -f

# View logs for a specific service
docker-compose logs -f gateway
docker-compose logs -f order-service
docker-compose logs -f payment-service
docker-compose logs -f frontend

# Stop all services
docker-compose down

# Stop and remove volumes
docker-compose down -v
```

## Service Endpoints

After running `docker-compose up -d`, access the services at:

- **Frontend**: http://localhost:4200
- **Gateway API**: http://localhost:3000
- **Kafka**: localhost:9092
- **Zookeeper**: localhost:2181

## Service Details

### Zookeeper
- Container: `zookeeper`
- Port: 2181
- Role: Manages Kafka cluster coordination

### Kafka
- Container: `kafka`
- External Port: 9092
- Internal Port: 29092 (for service-to-service communication)
- Role: Message broker for event streaming

### Gateway Service
- Container: `order-gateway`
- Language: Node.js
- Port: 3000
- Role: API entry point, manages REST API and WebSocket connections
- Logs: `[GATEWAY-INFO]`, `[GATEWAY-ERROR]`, `[GATEWAY-WARN]`

### Order Service
- Container: `order-service`
- Language: Node.js
- Internal service (no exposed port)
- Role: Validates orders (product, price, location)
- Logs: `[ORDER-SERVICE-INFO]`, `[ORDER-SERVICE-ERROR]`, `[ORDER-SERVICE-WARN]`

### Payment Service
- Container: `payment-service`
- Language: Node.js
- Internal service (no exposed port)
- Role: Processes payment transactions
- Logs: `[PAYMENT-SERVICE-INFO]`, `[PAYMENT-SERVICE-ERROR]`, `[PAYMENT-SERVICE-WARN]`

### Frontend
- Container: `order-frontend`
- Language: Angular 17
- Port: 4200
- Role: Web UI for order creation and status tracking

## Event Flow

1. User places order on frontend
2. Frontend sends request to Gateway API (`POST /api/orders`)
3. Gateway publishes `ORDER_CREATED` event to Kafka
4. Order Service consumes event and triggers validation checks:
   - PRODUCT_CHECK
   - PRICE_CHECK
   - LOCATION_CHECK
5. Order Service publishes validation results (PRODUCT_AVAILABLE, PRICE_CONFIRMED, LOCATION_VALID)
6. Once all checks pass, Order Service publishes PAYMENT_REQUEST
7. Payment Service consumes and processes payment
8. Payment result (PAYMENT_SUCCESS or PAYMENT_FAILED) is published
9. Gateway broadcasts status updates to frontend via WebSocket

## Logging

Each service logs with timestamped messages including:
- Timestamp in ISO format
- Service name prefix
- Log level (INFO, ERROR, WARN)
- Event details in JSON format

Example log output:
```
[2026-03-28T10:30:45.123Z] [GATEWAY-INFO] Gateway listening {"port":3000,"url":"http://localhost:3000"}
[2026-03-28T10:30:46.456Z] [ORDER-SERVICE-INFO] Order Service listening for order events
[2026-03-28T10:30:47.789Z] [PAYMENT-SERVICE-INFO] Payment Service listening for payment requests
```

## Troubleshooting

### Services keep restarting
- Check logs: `docker-compose logs -f`
- Ensure Kafka is healthy: Wait 10-15 seconds after startup

### Port already in use
- Change ports in docker-compose.yml
- Or kill existing processes: `lsof -i :3000` or `lsof -i :4200`

### No logs appearing
- Verify containers are running: `docker-compose ps`
- Check container status: `docker ps -a`
- View full logs: `docker-compose logs`

## Building Images Manually

```bash
# Build backend services
docker build -t order-backend:latest ./backend

# Build frontend
docker build -t order-frontend:latest ./frontend/angular-app

# Run specific service
docker run -e SERVICE=gateway -p 3000:3000 order-backend:latest
```

## Performance Considerations

- Each service runs in its own container
- Services communicate through Kafka on the internal network
- Kafka uses a single partition for order events
- No database persistence (in-memory state only)
- Suitable for development and testing

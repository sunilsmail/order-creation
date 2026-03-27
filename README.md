# Real-Time Order Processing Microservices

A production-ready microservices architecture for order processing with real-time status updates using Node.js, Kafka, and Angular.

**Tech Stack:** Node.js 22 | Express | Kafka | Socket.io | Angular 17 | Docker & Docker Compose

## 📋 Table of Contents

- [Architecture Overview](#architecture-overview)
- [System Layers](#system-layers)
- [Quick Start](#quick-start)
- [Project Structure](#project-structure)
- [Setup Instructions](#setup-instructions)
- [API Documentation](#api-documentation)
- [Event Flow](#event-flow)

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          CLIENT LAYER                                   │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                   Angular Frontend (Port 4200)                  │   │
│  │  ┌──────────────────┐  ┌──────────────────┐                    │   │
│  │  │  Checkout       │  │  Status         │                    │   │
│  │  │  Component      │  │  Component      │                    │   │
│  │  └──────────────────┘  └──────────────────┘                    │   │
│  │           ▲                      ▲                              │   │
│  │           │ HTTP/WS              │ HTTP/WS                       │   │
│  │           └──────────┬───────────┘                              │   │
│  └─────────────────────┼──────────────────────────────────────────┘   │
└────────────────────────┼───────────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                       API/GATEWAY LAYER                                 │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │              Gateway Service (Port 3000)                        │   │
│  │  ┌──────────────────┐  ┌──────────────────┐                    │   │
│  │  │  REST API       │  │  WebSocket       │                    │   │
│  │  │  • /api/token   │  │  Server (Socket) │                    │   │
│  │  │  • /api/orders  │  │  • Real-time     │                    │   │
│  │  │  • Auth         │  │    Updates       │                    │   │
│  │  └──────────────────┘  └──────────────────┘                    │   │
│  │           ▲                      ▲                              │   │
│  └─────────┼──────────────────────┼───────────────────────────────┘   │
└────────────┼──────────────────────┼──────────────────────────────────────┘
             │                      │
             │ Kafka Events         │ Kafka Events
             ▼                      ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                     MESSAGE BROKER LAYER                                │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                    Kafka (Port 9092)                            │   │
│  │  ┌────────────────────────────────────────────────────────┐    │   │
│  │  │  Topic: order-events (Single Partition)              │    │   │
│  │  │  • ORDER_CREATED                                      │    │   │
│  │  │  • PRODUCT_CHECK, PRICE_CHECK, LOCATION_CHECK       │    │   │
│  │  │  • PRODUCT_AVAILABLE, PRICE_CONFIRMED, etc.         │    │   │
│  │  │  • PAYMENT_REQUEST, PAYMENT_SUCCESS, etc.           │    │   │
│  │  └────────────────────────────────────────────────────────┘    │   │
│  │  ┌────────────────────────────────────────────────────────┐    │   │
│  │  │  Zookeeper (Port 2181) - Cluster Coordination        │    │   │
│  │  └────────────────────────────────────────────────────────┘    │   │
│  └─────────────────────────────────────────────────────────────────┘   │
└───┬─────────────────────────────────────────────────────────────┬───────┘
    │                                                             │
    │ Subscribe                                      Subscribe   │
    ▼                                                             ▼
┌──────────────────────────┐  ┌──────────────────────┐  ┌──────────────────────┐
│   ORDER SERVICE          │  │   PAYMENT SERVICE    │  │    GATEWAY           │
│  (Consumer Group)        │  │  (Consumer Group)    │  │   (Consumer Group)   │
│  ┌────────────────────┐  │  │  ┌────────────────┐  │  │  ┌────────────────┐  │
│  │ • Validates order  │  │  │  │ • Processes    │  │  │  │ • Broadcasts   │  │
│  │ • Checks products  │  │  │  │   payments     │  │  │  │   updates to   │  │
│  │ • Verifies pricing │  │  │  │ • Random 80%   │  │  │  │   clients via  │  │
│  │ • Validates region │  │  │  │   success rate │  │  │  │   WebSocket    │  │
│  │ • Publishes result │  │  │  │ • Publishes    │  │  │  │ Events to UI   │  │
│  │   events           │  │  │  │   result       │  │  │  │                │  │
│  └────────────────────┘  │  │  └────────────────┘  │  │  └────────────────┘  │
│  Port: Internal          │  │  Port: Internal      │  │  Port: 3000          │
│  Group: order-service    │  │  Group: payment-svc  │  │  Group: gateway-svc  │
└──────────────────────────┘  └──────────────────────┘  └──────────────────────┘
```

---

## System Layers

### 1️⃣ **Client Layer (Frontend - Angular)**

**Responsibility:** User interface and client-side logic

**Components:**
- **Checkout Component** - Create new orders
  - Displays user information
  - Form to place orders
  - Button to initiate order creation
  
- **Status Component** - Real-time order tracking
  - Displays current order status
  - Live updates via WebSocket
  - Shows order details and timeline

**Services:**
- **ApiService** - HTTP client for REST API calls
  - Creates orders via POST to Gateway
  - Includes JWT authentication
  
- **AuthService** - Authentication management
  - Gets user tokens from Gateway
  - Manages user session (demo-user)
  
- **SocketService** - WebSocket client
  - Connects to Gateway via Socket.io
  - Receives real-time order updates
  - Subscribes to order rooms

**Entry Points:**
- `localhost:4200` - Main application
- Routes: `/` (checkout), `/status/:orderId` (tracking)

---

### 2️⃣ **API/Gateway Layer (Express + Socket.io)**

**Responsibility:** Entry point for all client requests and real-time communication

**Location:** `backend/gateway/server.js`

**Port:** 3000

**Key Features:**
- **REST API Endpoints:**
  - `GET /api/token/:userId` - Generate JWT token
  - `POST /api/orders` - Create new order
  
- **Authentication:** JWT-based Bearer token validation
  
- **WebSocket Server:**
  - Two-way communication with clients
  - Order room subscriptions (`join-order` event)
  - Broadcasting status updates to subscribed clients
  
- **Kafka Integration:**
  - Produces: `ORDER_CREATED` event
  - Consumes: Status events (PRODUCT_AVAILABLE, PAYMENT_SUCCESS, etc.)
  - Broadcasts results to connected clients

**Logging:** `[GATEWAY-INFO]`, `[GATEWAY-ERROR]`, `[GATEWAY-WARN]`

---

### 3️⃣ **Message Broker Layer (Kafka + Zookeeper)**

**Responsibility:** Asynchronous event distribution and service decoupling

**Components:**

- **Zookeeper (Port 2181)**
  - Manages Kafka cluster metadata
  - Maintains consumer group state
  - Provides distributed coordination
  
- **Kafka (Port 9092)**
  - Topic: `order-events` (1 partition)
  - Publish-subscribe messaging
  - Message persistence
  - Consumer group coordination

**Event Types:**

| Event | Producer | Consumer | Purpose |
|-------|----------|----------|---------|
| `ORDER_CREATED` | Gateway | Order Service | New order receives validation tasks |
| `PRODUCT_CHECK` | Order Service | Order Service | Check product availability |
| `PRICE_CHECK` | Order Service | Order Service | Verify pricing |
| `LOCATION_CHECK` | Order Service | Order Service | Validate delivery location |
| `PRODUCT_AVAILABLE` | Order Service | Order Service | Product confirmed |
| `PRICE_CONFIRMED` | Order Service | Order Service | Price verified |
| `LOCATION_VALID` | Order Service | Order Service | Location acceptable |
| `PAYMENT_REQUEST` | Order Service | Payment Service | Ready for payment |
| `PAYMENT_SUCCESS` | Payment Service | Gateway | Payment approved |
| `PAYMENT_FAILED` | Payment Service | Gateway | Payment rejected |
| `ORDER_FAILED` | Order Service | Gateway | Order rejected |

---

### 4️⃣ **Business Logic Layer (Microservices)**

#### **Order Service**
**File:** `backend/order-service/index.js`
**Consumer Group:** `order-service-group`

**Responsibilities:**
- Consumes `ORDER_CREATED` events
- Initiates validation checks (Product, Price, Location)
- Tracks validation state per order
- Publishes validation result events
- Triggers payment when all checks pass
- Publishes `ORDER_FAILED` on validation failure

**Processing Flow:**
```
ORDER_CREATED 
  → Trigger PRODUCT_CHECK
  → Trigger PRICE_CHECK
  → Trigger LOCATION_CHECK
  → Wait for results
  → On success → Publish PAYMENT_REQUEST
  → On failure → Publish ORDER_FAILED
```

**Logging:** `[ORDER-SERVICE-INFO]`, `[ORDER-SERVICE-ERROR]`, `[ORDER-SERVICE-WARN]`

---

#### **Payment Service**
**File:** `backend/payment-service/index.js`
**Consumer Group:** `payment-service-group`

**Responsibilities:**
- Consumes `PAYMENT_REQUEST` events
- Simulates payment processing (1 second delay)
- Random success/failure (80% success rate)
- Publishes payment result (`PAYMENT_SUCCESS` or `PAYMENT_FAILED`)

**Processing Flow:**
```
PAYMENT_REQUEST
  → Wait 1 second
  → Random decision (80% success)
  → Publish PAYMENT_SUCCESS or PAYMENT_FAILED
```

**Logging:** `[PAYMENT-SERVICE-INFO]`, `[PAYMENT-SERVICE-ERROR]`, `[PAYMENT-SERVICE-WARN]`

---

### 5️⃣ **Infrastructure Layer**

**Kafka Client:** `backend/kafka/client.js`
- KafkaJS client initialization
- Producer creation
- Consumer creation with group coordination

**Kafka Topics:** `backend/kafka/topics.js`
- Centralized topic definitions
- Ensures consistent naming

---

## Quick Start

### Option 1: Docker Compose (Recommended)

```bash
# Clone or navigate to project directory
cd order-creation

# Build and start all services
docker-compose up -d

# View logs
docker-compose logs -f

# Stop all services
docker-compose down
```

**Access:**
- Frontend: http://localhost:4200
- Gateway API: http://localhost:3000
- Kafka: localhost:9092

See [DOCKER_README.md](DOCKER_README.md) for detailed Docker instructions.

### Option 2: Local Development

See [Local Setup](#local-setup-without-docker) below.

---

## Project Structure

```
order-creation/
├── docker-compose.yml           # Complete stack orchestration
├── README.md                    # This file
├── DOCKER_README.md            # Docker-specific documentation
│
├── backend/                     # Node.js backend services
│   ├── package.json
│   ├── Dockerfile
│   ├── .dockerignore
│   ├── gateway/
│   │   └── server.js           # API Gateway (Express + Socket.io)
│   ├── order-service/
│   │   └── index.js            # Order validation service
│   ├── payment-service/
│   │   └── index.js            # Payment processing service
│   └── kafka/
│       ├── client.js           # Kafka client setup
│       └── topics.js           # Topic definitions
│
└── frontend/                    # Angular frontend
    └── angular-app/
        ├── package.json
        ├── Dockerfile
        ├── .dockerignore
        ├── angular.json
        ├── tsconfig.json
        └── src/
            ├── main.ts         # Application entry point (with Zone.js)
            ├── index.html
            ├── styles.css
            └── app/
                ├── app.component.ts
                ├── app.routes.ts
                ├── components/
                │   ├── checkout/
                │   │   └── checkout.component.ts
                │   └── status/
                │       └── status.component.ts
                ├── services/
                │   ├── api.service.ts
                │   ├── auth.service.ts
                │   └── socket.service.ts
                └── environments/
                    └── environment.ts
```

---

## Setup Instructions

### Local Setup (Without Docker)

#### Prerequisites
- Node.js 22+
- Kafka installed locally
- npm or yarn

#### 1. Start Kafka & Zookeeper

```bash
# Terminal 1 - Zookeeper
cd ~/kafka
bin/zookeeper-server-start.sh config/zookeeper.properties

# Terminal 2 - Kafka Broker
cd ~/kafka
bin/kafka-server-start.sh config/server.properties

# Terminal 3 - Create Topic
cd ~/kafka
bin/kafka-topics.sh --create \
  --topic order-events \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1

# Verify topic
bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

#### 2. Start Backend Services

```bash
# Terminal 4 - Install dependencies
cd backend
npm install

# Terminal 4a - Gateway Service
npm run start:gateway

# Terminal 4b - Order Service
npm run start:order

# Terminal 4c - Payment Service
npm run start:payment
```

**Expected output:**
```
[2026-03-28T...] [GATEWAY-INFO] Gateway listening {"port":3000}
[2026-03-28T...] [ORDER-SERVICE-INFO] Order Service listening for order events
[2026-03-28T...] [PAYMENT-SERVICE-INFO] Payment Service listening for payment requests
```

#### 3. Start Frontend

```bash
# Terminal 5
cd frontend/angular-app
npm install
npm start
# or: ng serve
```

**Open browser:** http://localhost:4200

---

## API Documentation

### Gateway API Endpoints

#### 1. Get Authentication Token
```http
GET /api/token/{userId}
```

**Response:**
```json
{
  "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."
}
```

**Usage:** Token must be included in subsequent API calls as Bearer token

---

#### 2. Create Order
```http
POST /api/orders
Authorization: Bearer {token}
Content-Type: application/json
```

**Request Body:**
```json
{
  "items": [
    { "sku": "BOOK-1", "qty": 1 }
  ],
  "total": 100,
  "location": "US"
}
```

**Response:**
```json
{
  "orderId": "ord_1711620646123_45678",
  "status": "PROCESSING"
}
```

---

### WebSocket Events

#### Connect
```javascript
const socket = io('http://localhost:3000', {
  auth: { token: 'Bearer ...' }
});
```

#### Join Order Room
```javascript
socket.emit('join-order', { orderId: 'ord_...' });
```

#### Listen for Updates
```javascript
socket.on('order-update', (data) => {
  console.log(data);
  // {
  //   orderId: "ord_...",
  //   userId: "demo-user",
  //   status: "PAYMENT_SUCCESS",
  //   message: "Payment completed for ord_..."
  // }
});
```

---

## Event Flow

### Complete Order Lifecycle

```
1. USER ACTION
   └─> User clicks "Place Order" on frontend

2. AUTHENTICATION
   └─> Frontend calls GET /api/token/demo-user
   └─> Gateway returns JWT token

3. ORDER CREATION
   └─> Frontend calls POST /api/orders with token
   └─> Gateway validates JWT
   └─> Gateway publishes ORDER_CREATED to Kafka
   └─> Returns orderId and PROCESSING status

4. REAL-TIME SUBSCRIPTION
   └─> Frontend connects to WebSocket
   └─> Emits join-order event with orderId
   └─> Gateway adds client to order room

5. ORDER VALIDATION
   └─> Order Service consumes ORDER_CREATED
   └─> Triggers 3 parallel checks:
       ├─> PRODUCT_CHECK
       ├─> PRICE_CHECK
       └─> LOCATION_CHECK
   └─> Each check has 300-1200ms random delay
   └─> Publishes results to Kafka

6. PAYMENT TRIGGER
   └─> Order Service consumes validation results
   └─> If all passed, triggers PAYMENT_REQUEST
   └─> If any failed, triggers ORDER_FAILED

7. PAYMENT PROCESSING
   └─> Payment Service consumes PAYMENT_REQUEST
   └─> Waits 1 second
   └─> 80% success, 20% failure (random)
   └─> Publishes PAYMENT_SUCCESS or PAYMENT_FAILED

8. STATUS BROADCAST
   └─> Gateway consumes payment result
   └─> Broadcasts to all clients in order room
   └─> Clients receive update via WebSocket

9. USER FEEDBACK
   └─> Frontend displays final order status
   └─> Order complete (success) or failed
```

---

## Logging

All services include comprehensive logging with timestamps.

**Format:**
```
[ISO-8601 Timestamp] [SERVICE-LEVEL] Message {"data": "in JSON"}
```

**Example Logs:**
```
[2026-03-28T10:30:45.123Z] [GATEWAY-INFO] Token generated {"userId":"demo-user"}
[2026-03-28T10:30:46.456Z] [GATEWAY-INFO] Order creation initiated {"orderId":"ord_1711...","userId":"demo-user"}
[2026-03-28T10:30:46.789Z] [ORDER-SERVICE-INFO] Processing ORDER_CREATED {"orderId":"ord_1711..."}
[2026-03-28T10:30:46.945Z] [PAYMENT-SERVICE-INFO] Payment request received {"orderId":"ord_1711..."}
[2026-03-28T10:30:48.123Z] [PAYMENT-SERVICE-INFO] Processing payment {"orderId":"ord_1711...","resultType":"PAYMENT_SUCCESS"}
[2026-03-28T10:30:48.456Z] [GATEWAY-INFO] Emitting order update to clients {"orderId":"ord_1711...","status":"PAYMENT_SUCCESS"}
```

**Log Prefixes by Service:**
- Gateway: `[GATEWAY-INFO]`, `[GATEWAY-ERROR]`, `[GATEWAY-WARN]`
- Order Service: `[ORDER-SERVICE-INFO]`, `[ORDER-SERVICE-ERROR]`, `[ORDER-SERVICE-WARN]`
- Payment Service: `[PAYMENT-SERVICE-INFO]`, `[PAYMENT-SERVICE-ERROR]`, `[PAYMENT-SERVICE-WARN]`

---

## Performance & Scalability

### Current Architecture
- **Single Kafka partition** - Maintains order processing sequence
- **In-memory state** - No database persistence (suitable for demo/testing)
- **80% payment success rate** - Simulated realistic scenarios

### To Scale to Production
1. **Database Layer** - Add PostgreSQL/MongoDB for persistence
2. **Multiple Partitions** - Distribute orders across Kafka partitions
3. **Load Balancing** - Multiple Gateway instances
4. **Distributed State** - Redis for shared session management
5. **Monitoring** - ELK stack for centralized logging
6. **Distributed Tracing** - Jaeger for end-to-end request tracking

---

## Troubleshooting

### Services won't start
- Ensure Kafka (or Docker containers) are running
- Check port availability: 3000 (Gateway), 4200 (Frontend), 9092 (Kafka)
- View logs: `npm run start:*` or `docker-compose logs -f`

### WebSocket connection fails
- Ensure Gateway is running on port 3000
- Check CORS configuration in gateway
- Browser console for detailed errors

### Orders not processing
- Verify Kafka is healthy: `docker-compose logs kafka`
- Check consumer group status in Kafka
- Review service logs for specific errors

### Port already in use
- Kill existing process: `lsof -i :3000` then `kill -9 <PID>`
- Or change port in environment/config

---

## License

MIT
bin/zookeeper-server-start.sh config/zookeeper.properties

# Terminal 2 - Kafka broker
cd ~/kafka
bin/kafka-server-start.sh config/server.properties

# Terminal 3 - Create topic
cd ~/kafka
bin/kafka-topics.sh --create --topic order-events --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

# Verify topic exists
bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

## Local Run Instructions

### 1) Backend

```bash
cd backend
npm install
```

Run each service in a separate terminal:

```bash
# Terminal A
node gateway/server.js

# Terminal B
node order-service/index.js

# Terminal C
node payment-service/index.js
```

### 2) Frontend (Angular)

```bash
cd frontend/angular-app
npm install
npm run start
```

Open browser at `http://localhost:4200`.

## JWT Authentication

The gateway provides a simple token helper endpoint:

```bash
curl http://localhost:3000/api/token/demo-user
```

Use the returned token as `Authorization: Bearer <token>` for REST calls and as Socket.IO auth token.

## Test Flow

1. Start Zookeeper and Kafka.
2. Create topic `order-events`.
3. Start gateway, order-service, and payment-service.
4. Start Angular app.
5. Open `http://localhost:4200`.
6. Click **Place Order**.
7. Checkout page creates order and joins order room.
8. Status page shows live events via WebSocket `order-update`:
   - ORDER_CREATED
   - PRODUCT_AVAILABLE / PRICE_CONFIRMED / LOCATION_VALID
   - PAYMENT_SUCCESS or PAYMENT_FAILED


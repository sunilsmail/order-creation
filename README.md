# Real-Time Order Processing Microservices (Node + Kafka + Angular)

## Folder Structure

```text
backend/
  package.json
  gateway/
    server.js
  order-service/
    index.js
  payment-service/
    index.js
  kafka/
    client.js
    topics.js

frontend/
  angular-app/
    package.json
    angular.json
    tsconfig.json
    tsconfig.app.json
    src/
      index.html
      main.ts
      styles.css
      app/
        app.component.ts
        app.routes.ts
        services/
          api.service.ts
          socket.service.ts
          auth.service.ts
        components/
          checkout/
            checkout.component.ts
          status/
            status.component.ts
      environments/
        environment.ts
```

## Kafka Setup (No Docker)

> Assumes Kafka is installed locally in `~/kafka`.

```bash
# Terminal 1 - Zookeeper
cd ~/kafka
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


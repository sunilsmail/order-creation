FROM node:22-alpine

WORKDIR /app

# Copy package.json and install dependencies
COPY package.json package-lock.json ./
RUN npm ci

# Copy all backend source code
COPY . .

# Accept SERVICE as build argument with gateway as default
ARG SERVICE=gateway
ENV SERVICE=${SERVICE}

# Expose ports for all services
EXPOSE 3000 3001 3002

# Start the service based on SERVICE variable
CMD npm run start:${SERVICE}

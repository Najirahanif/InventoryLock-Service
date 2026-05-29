# Use official Node.js image with build tools
FROM node:20-alpine

# Install build dependencies needed for native modules
RUN apk add --no-cache python3 make g++

# Create app directory
WORKDIR /app

# Copy package.json and package-lock.json (if available)
COPY package*.json ./

# Install dependencies (including dev dependencies for building)
RUN npm ci || npm install

# Copy application source code
COPY . .

# Create non-root user for security
RUN addgroup -g 1001 -S nodejs && \
    adduser -S nodejs -u 1001 && \
    chown -R nodejs:nodejs /app

# Switch to non-root user
USER nodejs

# Command to run your application
CMD ["node", "order-service/producer.js"]
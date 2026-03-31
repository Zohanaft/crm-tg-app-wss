FROM node:24-trixie-slim

WORKDIR /app

COPY package*.json ./
RUN npm install

COPY . .

EXPOSE 3002

# Run node directly so Docker SIGTERM does not produce noisy "npm error" logs (npm is not PID 1).
CMD ["node", "src/index.js"]

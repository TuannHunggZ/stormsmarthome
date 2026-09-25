const express = require('express');
const http = require('http');
const path = require('path');
const { WebSocketServer } = require('ws');
const { createClient } = require('redis');

const app = express();
const server = http.createServer(app);
const webSocketServer = new WebSocketServer({ server });

const port = Number.parseInt(process.env.PORT || '3000', 10);
const redisHost = process.env.REDIS_HOST || 'redis';
const redisPort = Number.parseInt(process.env.REDIS_PORT || '6379', 10);
const redisUrl = process.env.REDIS_URL || `redis://${redisHost}:${redisPort}`;
const plugChannel = process.env.REDIS_PLUG_CHANNEL || 'anomaly:plug';
const houseChannel = process.env.REDIS_HOUSE_CHANNEL || 'anomaly:house';

const recentAlerts = [];

app.use(express.static(path.join(__dirname, 'public')));

function pushRecentAlert(alert) {
  recentAlerts.unshift(alert);

  if (recentAlerts.length > 30) {
    recentAlerts.length = 30;
  }
}

function broadcastAlert(alert) {
  const payload = JSON.stringify({
    ...alert,
    receivedAt: new Date().toISOString()
  });

  for (const client of webSocketServer.clients) {
    if (client.readyState === 1) {
      client.send(payload);
    }
  }
}

function parseRedisAlert(channel, payload) {
  const alert = JSON.parse(payload);
  alert.channel = channel;
  return alert;
}

async function startRedisSubscriber() {
  const subscriber = createClient({ url: redisUrl });

  subscriber.on('connect', () => {
    console.log(`[webapp] connecting to Redis at ${redisUrl}`);
  });

  subscriber.on('ready', () => {
    console.log(`[webapp] Redis subscriber ready on ${redisUrl}`);
  });

  subscriber.on('reconnecting', () => {
    console.log('[webapp] Redis subscriber reconnecting');
  });

  subscriber.on('end', () => {
    console.log('[webapp] Redis subscriber disconnected');
  });

  subscriber.on('error', (error) => {
    console.error('[webapp] Redis subscriber error', error);
  });

  await subscriber.connect();

  await subscriber.subscribe(plugChannel, (message) => {
    try {
      const alert = parseRedisAlert(plugChannel, message);
      pushRecentAlert(alert);
      broadcastAlert(alert);
      console.log(`[webapp] broadcast plug alert from ${plugChannel}`);
    } catch (error) {
      console.error('[webapp] failed to process plug alert', error);
    }
  });

  await subscriber.subscribe(houseChannel, (message) => {
    try {
      const alert = parseRedisAlert(houseChannel, message);
      pushRecentAlert(alert);
      broadcastAlert(alert);
      console.log(`[webapp] broadcast house alert from ${houseChannel}`);
    } catch (error) {
      console.error('[webapp] failed to process house alert', error);
    }
  });

  console.log(`[webapp] subscribed to ${plugChannel} and ${houseChannel}`);
}

webSocketServer.on('connection', (socket) => {
  console.log('[webapp] browser connected');
  socket.send(JSON.stringify({ type: 'history', alerts: recentAlerts }));

  socket.on('close', () => {
    console.log('[webapp] browser disconnected');
  });

  socket.on('error', (error) => {
    console.error('[webapp] websocket error', error);
  });
});

server.listen(port, () => {
  console.log(`[webapp] listening on ${port}`);
});

startRedisSubscriber().catch((error) => {
  console.error('[webapp] unable to start Redis subscriber', error);
});
import 'dotenv/config';
import Fastify from 'fastify';
import websocket from '@fastify/websocket';
import { Redis } from 'ioredis';
import type { WebSocket } from 'ws';
import { LiveStateStore } from './live/live-state-store.js';
import { LiveConsumer } from './messaging/live-consumer.js';
import { LiveConnectionRegistry } from './websocket/live-connection-registry.js';

const port = Number(process.env.LIVE_HTTP_PORT ?? 3002);
const redisUrl = process.env.LIVE_REDIS_URL;
const rabbitUrl = process.env.RABBITMQ_URL;

if (!redisUrl) {
  throw new Error('LIVE_REDIS_URL é obrigatório.');
}

if (!rabbitUrl) {
  throw new Error('RABBITMQ_URL é obrigatório.');
}

const app = Fastify({
  logger: true,
});

await app.register(websocket);

const redis = new Redis(redisUrl);
const store = new LiveStateStore(redis);
const registry = new LiveConnectionRegistry();

const consumer = new LiveConsumer(
  rabbitUrl,
  store,
  registry,
  process.env.RABBITMQ_EXCHANGE ?? 'powergrid.events',
);

app.get('/health', async () => ({
  status: 'ok',
  service: 'live-service',
  version: '0.1.0',
}));

app.get<{ Params: { orderId: string } }>(
  '/live/orders/:orderId',
  async (request, reply) => {
    const state = await store.findByOrderId(request.params.orderId);

    if (state === null) {
      return reply.code(404).send({
        error: 'LIVE_STATE_NOT_FOUND',
        message: 'Estado Live da ordem não encontrado.',
      });
    }

    return state;
  },
);

app.get(
  '/ws',
  { websocket: true },
  (socket: WebSocket): void => {
    registry.add(socket);

    app.log.info(
      { websocketClients: registry.size() },
      'cliente WebSocket conectado',
    );

    socket.on('close', () => {
      registry.remove(socket);

      app.log.info(
        { websocketClients: registry.size() },
        'cliente WebSocket desconectado',
      );
    });

    socket.on('error', () => {
      registry.remove(socket);
    });
  },
);

const shutdown = async (signal: string): Promise<void> => {
  app.log.info({ signal }, 'encerrando live-service');

  await app.close();
  await consumer.close();
  await redis.quit();

  process.exit(0);
};

process.on('SIGINT', () => {
  void shutdown('SIGINT');
});

process.on('SIGTERM', () => {
  void shutdown('SIGTERM');
});

try {
  await redis.ping();
  await consumer.start();
  await app.listen({ host: '0.0.0.0', port });
} catch (error) {
  app.log.error(error);
  await consumer.close();
  await redis.quit();
  process.exit(1);
}

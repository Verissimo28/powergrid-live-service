import 'dotenv/config';
import Fastify from 'fastify';

const port = Number(process.env.LIVE_HTTP_PORT ?? 3002);

const app = Fastify({
  logger: true,
});

app.get('/health', async () => {
  return {
    status: 'ok',
    service: 'live-service',
    version: '0.1.0',
  };
});

const start = async (): Promise<void> => {
  try {
    await app.listen({ host: '0.0.0.0', port });
  } catch (error) {
    app.log.error(error);
    process.exit(1);
  }
};

const shutdown = async (signal: string): Promise<void> => {
  app.log.info({ signal }, 'encerrando live-service');
  await app.close();
  process.exit(0);
};

process.on('SIGINT', () => {
  void shutdown('SIGINT');
});

process.on('SIGTERM', () => {
  void shutdown('SIGTERM');
});

void start();
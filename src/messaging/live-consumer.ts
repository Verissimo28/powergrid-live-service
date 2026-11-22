import amqp, {
  type Channel,
  type ChannelModel,
  type ConsumeMessage,
} from 'amqplib';
import type Redis from 'ioredis';
import { LiveStateStore } from '../live/live-state-store.js';
import {
  LiveConnectionRegistry,
  type LiveWebSocketMessage,
} from '../websocket/live-connection-registry.js';

type IncomingEvent = {
  eventId: string;
  eventType: string;
  aggregateId: string;
  aggregateVersion: number;
  occurredAt?: string;
  correlationId?: string;
  payload?: Record<string, unknown>;
  [key: string]: unknown;
};

const MAX_RETRIES = 3;

function parseEvent(message: ConsumeMessage): IncomingEvent {
  const parsed: unknown = JSON.parse(message.content.toString('utf8'));

  if (typeof parsed !== 'object' || parsed === null) {
    throw new Error('Evento RabbitMQ inválido.');
  }

  const event = parsed as Partial<IncomingEvent>;

  if (
    typeof event.eventId !== 'string' ||
    typeof event.eventType !== 'string' ||
    typeof event.aggregateId !== 'string' ||
    typeof event.aggregateVersion !== 'number'
  ) {
    throw new Error('Evento sem envelope obrigatório.');
  }

  return event as IncomingEvent;
}

function retryCount(message: ConsumeMessage): number {
  const value = message.properties.headers?.['x-retry-count'];

  return typeof value === 'number' ? value : 0;
}

export class LiveConsumer {
  private connection: ChannelModel | undefined;
  private channel: Channel | undefined;
  private stopping = false;
  private reconnecting = false;

  public constructor(
    private readonly rabbitUrl: string,
    private readonly store: LiveStateStore,
    private readonly registry: LiveConnectionRegistry,
    private readonly exchange = 'powergrid.events',
    private readonly queue = 'live.events',
  ) {}

  public async start(): Promise<void> {
    this.stopping = false;
    await this.connect();

    console.log(
      JSON.stringify({
        service: 'live-service',
        action: 'rabbit-connected',
      }),
    );
  }

  private async connect(): Promise<void> {
    const connection = await amqp.connect(this.rabbitUrl);
    const channel = await connection.createChannel();

    this.connection = connection;
    this.channel = channel;

    connection.on('close', () => {
      if (!this.stopping) {
        this.scheduleReconnect();
      }
    });

    connection.on('error', (error) => {
      if (!this.stopping) {
        console.error('Conexão RabbitMQ do Live apresentou erro', error);
      }
    });

    await channel.assertExchange(this.exchange, 'topic', {
      durable: true,
    });

    await channel.assertExchange('powergrid.retry', 'topic', {
      durable: true,
    });

    await channel.assertExchange('powergrid.dlx', 'topic', {
      durable: true,
    });

    await channel.assertQueue(this.queue, {
      durable: true,
    });

    await channel.bindQueue(this.queue, this.exchange, 'order.#');
    await channel.prefetch(10);

    await channel.consume(this.queue, (message) => {
      if (message === null) {
        return;
      }

      void this.handle(message);
    });
  }

  private scheduleReconnect(): void {
    if (this.reconnecting || this.stopping) {
      return;
    }

    this.reconnecting = true;

    void (async () => {
      let delay = 1000;

      while (!this.stopping) {
        try {
          await this.connect();

          console.log(
            JSON.stringify({
              service: 'live-service',
              action: 'rabbit-reconnected',
            }),
          );

          this.reconnecting = false;
          return;
        } catch (error) {
          console.error(
            JSON.stringify({
              service: 'live-service',
              action: 'rabbit-reconnect-failed',
              error: error instanceof Error ? error.message : error,
              retryInMs: delay,
            }),
          );

          await new Promise((resolve) => setTimeout(resolve, delay));
          delay = Math.min(delay * 2, 30000);
        }
      }

      this.reconnecting = false;
    })();
  }

  private async publishRetry(
    message: ConsumeMessage,
    count: number,
  ): Promise<void> {
    const channel = this.channel;

    if (channel === undefined) {
      throw new Error('Canal RabbitMQ indisponível.');
    }

    const routingKey = message.fields.routingKey || 'order.unknown';

    channel.publish('powergrid.retry', routingKey, message.content, {
      persistent: true,
      contentType: message.properties.contentType ?? 'application/json',
      headers: {
        ...message.properties.headers,
        'x-retry-count': count,
      },
    });
  }

  private async publishDeadLetter(
    message: ConsumeMessage,
    error: unknown,
  ): Promise<void> {
    const channel = this.channel;

    if (channel === undefined) {
      throw new Error('Canal RabbitMQ indisponível.');
    }

    const routingKey = message.fields.routingKey || 'order.unknown';
    const errorMessage =
      error instanceof Error ? error.message : 'Erro desconhecido';

    channel.publish('powergrid.dlx', routingKey, message.content, {
      persistent: true,
      contentType: message.properties.contentType ?? 'application/json',
      headers: {
        ...message.properties.headers,
        'x-dead-letter-reason': errorMessage,
        'x-live-retry-count': retryCount(message),
      },
    });
  }

  private async handle(message: ConsumeMessage): Promise<void> {
    const channel = this.channel;

    if (channel === undefined) {
      return;
    }

    try {
      const event = parseEvent(message);

      const payload =
        event.payload !== undefined && typeof event.payload === 'object'
          ? event.payload
          : event;

      const applied = await this.store.apply({
        eventId: event.eventId,
        eventType: event.eventType,
        aggregateId: event.aggregateId,
        aggregateVersion: event.aggregateVersion,
        occurredAt:
          typeof event.occurredAt === 'string' ? event.occurredAt : null,
        correlationId:
          typeof event.correlationId === 'string'
            ? event.correlationId
            : null,
        payload,
      });

      if (applied) {
        const websocketMessage: LiveWebSocketMessage = {
          type: 'order.updated',
          eventId: event.eventId,
          eventType: event.eventType,
          aggregateId: event.aggregateId,
          aggregateVersion: event.aggregateVersion,
          occurredAt:
            typeof event.occurredAt === 'string' ? event.occurredAt : null,
          correlationId:
            typeof event.correlationId === 'string'
              ? event.correlationId
              : null,
          payload,
        };

        this.registry.broadcast(websocketMessage);
      }

      console.log(
        JSON.stringify({
          eventId: event.eventId,
          eventType: event.eventType,
          aggregateId: event.aggregateId,
          applied,
          websocketClients: this.registry.size(),
        }),
      );

      channel.ack(message);
    } catch (error) {
      const count = retryCount(message);

      try {
        if (count < MAX_RETRIES) {
          await this.publishRetry(message, count + 1);

          console.error(
            JSON.stringify({
              action: 'retry',
              retryCount: count + 1,
              error: error instanceof Error ? error.message : error,
            }),
          );
        } else {
          await this.publishDeadLetter(message, error);

          console.error(
            JSON.stringify({
              action: 'dead-letter',
              retryCount: count,
              error: error instanceof Error ? error.message : error,
            }),
          );
        }

        channel.ack(message);
      } catch (publishError) {
        console.error('Falha ao encaminhar mensagem para retry/DLQ', publishError);
        channel.nack(message, false, false);
      }
    }
  }

  public async close(): Promise<void> {
    this.stopping = true;
    this.reconnecting = false;

    await this.channel?.close().catch(() => undefined);
    await this.connection?.close().catch(() => undefined);

    this.channel = undefined;
    this.connection = undefined;
  }
}

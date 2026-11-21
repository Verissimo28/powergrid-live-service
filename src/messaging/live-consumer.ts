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

export class LiveConsumer {
  private connection: ChannelModel | undefined;
  private channel: Channel | undefined;

  public constructor(
    private readonly rabbitUrl: string,
    private readonly store: LiveStateStore,
    private readonly registry: LiveConnectionRegistry,
    private readonly exchange = 'powergrid.events',
    private readonly queue = 'live.events',
  ) {}

  public async start(): Promise<void> {
    this.connection = await amqp.connect(this.rabbitUrl);
    this.channel = await this.connection.createChannel();

    await this.channel.assertExchange(this.exchange, 'topic', {
      durable: true,
    });

    await this.channel.assertQueue(this.queue, {
      durable: true,
    });

    await this.channel.bindQueue(this.queue, this.exchange, 'order.#');
    await this.channel.prefetch(10);

    await this.channel.consume(this.queue, (message) => {
      if (message === null) {
        return;
      }

      void this.handle(message);
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
      console.error('Falha ao processar evento do Live Service', error);
      channel.nack(message, false, true);
    }
  }

  public async close(): Promise<void> {
    await this.channel?.close();
    await this.connection?.close();

    this.channel = undefined;
    this.connection = undefined;
  }
}

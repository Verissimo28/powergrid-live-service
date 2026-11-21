import type { Redis } from 'ioredis';

export type LiveOrderState = {
  eventId: string;
  eventType: string;
  aggregateId: string;
  aggregateVersion: number;
  occurredAt: string | null;
  correlationId: string | null;
  payload: Record<string, unknown>;
};

export class LiveStateStore {
  public constructor(private readonly redis: Redis) {}

  private stateKey(aggregateId: string): string {
    return `live:order:${aggregateId}`;
  }

  private dedupeKey(eventId: string): string {
    return `live:dedupe:${eventId}`;
  }

  public async apply(event: LiveOrderState): Promise<boolean> {
    const dedupeResult = await this.redis.set(
      this.dedupeKey(event.eventId),
      '1',
      'EX',
      86400,
      'NX',
    );

    if (dedupeResult === null) {
      return false;
    }

    const currentRaw = await this.redis.get(this.stateKey(event.aggregateId));

    if (currentRaw !== null) {
      const current = JSON.parse(currentRaw) as LiveOrderState;

      if (current.aggregateVersion >= event.aggregateVersion) {
        return false;
      }
    }

    await this.redis.set(
      this.stateKey(event.aggregateId),
      JSON.stringify(event),
    );

    return true;
  }

  public async findByOrderId(
    aggregateId: string,
  ): Promise<LiveOrderState | null> {
    const raw = await this.redis.get(this.stateKey(aggregateId));

    if (raw === null) {
      return null;
    }

    return JSON.parse(raw) as LiveOrderState;
  }
}

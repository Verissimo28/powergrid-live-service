import assert from 'node:assert/strict';
import test from 'node:test';
import type { Redis } from 'ioredis';
import {
  LiveStateStore,
  type LiveOrderState,
} from '../../src/live/live-state-store.js';

class FakeRedis {
  private readonly values = new Map<string, string>();

  public async set(
    key: string,
    value: string,
    ...args: Array<string | number>
  ): Promise<'OK' | null> {
    const nx = args.includes('NX');

    if (nx && this.values.has(key)) {
      return null;
    }

    this.values.set(key, value);
    return 'OK';
  }

  public async get(key: string): Promise<string | null> {
    return this.values.get(key) ?? null;
  }
}

function createEvent(
  overrides: Partial<LiveOrderState> = {},
): LiveOrderState {
  return {
    eventId: 'event-001',
    eventType: 'order.started.v1',
    aggregateId: 'order-001',
    aggregateVersion: 2,
    occurredAt: '2026-11-21T00:00:00.000Z',
    correlationId: 'correlation-001',
    payload: {
      status: 'IN_PROGRESS',
    },
    ...overrides,
  };
}

test('aplica o primeiro evento e salva o estado da ordem', async () => {
  const redis = new FakeRedis();
  const store = new LiveStateStore(redis as unknown as Redis);

  const applied = await store.apply(createEvent());

  assert.equal(applied, true);
  assert.deepEqual(await store.findByOrderId('order-001'), createEvent());
});

test('ignora o mesmo eventId duas vezes', async () => {
  const redis = new FakeRedis();
  const store = new LiveStateStore(redis as unknown as Redis);
  const event = createEvent();

  const first = await store.apply(event);
  const second = await store.apply(event);

  assert.equal(first, true);
  assert.equal(second, false);
  assert.deepEqual(await store.findByOrderId('order-001'), event);
});

test('ignora evento com versão menor ou igual à versão atual', async () => {
  const redis = new FakeRedis();
  const store = new LiveStateStore(redis as unknown as Redis);

  await store.apply(createEvent());

  const olderEvent = createEvent({
    eventId: 'event-000',
    aggregateVersion: 1,
    eventType: 'order.created.v1',
    payload: {
      status: 'OPEN',
    },
  });

  const applied = await store.apply(olderEvent);

  assert.equal(applied, false);
  assert.equal(
    (await store.findByOrderId('order-001'))?.aggregateVersion,
    2,
  );
});

test('aplica evento mais novo e substitui o estado anterior', async () => {
  const redis = new FakeRedis();
  const store = new LiveStateStore(redis as unknown as Redis);

  await store.apply(createEvent());

  const newerEvent = createEvent({
    eventId: 'event-002',
    eventType: 'order.closed.v1',
    aggregateVersion: 3,
    payload: {
      status: 'COMPLETED',
    },
  });

  const applied = await store.apply(newerEvent);

  assert.equal(applied, true);
  assert.deepEqual(
    await store.findByOrderId('order-001'),
    newerEvent,
  );
});

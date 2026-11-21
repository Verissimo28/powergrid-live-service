import type { WebSocket } from 'ws';

export type LiveWebSocketMessage = {
  type: 'order.updated';
  eventId: string;
  eventType: string;
  aggregateId: string;
  aggregateVersion: number;
  occurredAt: string | null;
  correlationId: string | null;
  payload: Record<string, unknown>;
};

export class LiveConnectionRegistry {
  private readonly connections = new Set<WebSocket>();

  public add(socket: WebSocket): void {
    this.connections.add(socket);
  }

  public remove(socket: WebSocket): void {
    this.connections.delete(socket);
  }

  public broadcast(message: LiveWebSocketMessage): void {
    const serialized = JSON.stringify(message);

    for (const socket of this.connections) {
      if (socket.readyState !== socket.OPEN) {
        this.connections.delete(socket);
        continue;
      }

      socket.send(serialized);
    }
  }

  public size(): number {
    return this.connections.size;
  }
}

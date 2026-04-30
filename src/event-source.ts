import {
  EventSourceMessage,
  EventStreamContentType,
  fetchEventSource,
} from "@microsoft/fetch-event-source";
import { deserializeString } from "@utiliread/json";
import { HttpError } from "@utiliread/http";

type Fetch = typeof fetch;
type EventType<T = any> = (new (...args: any[]) => T) & { eventName: string };
type EventHandler<T = any> = (event: T) => void;
type TimeoutHandle = ReturnType<typeof setTimeout>;
type Subscription = { dispose: () => void };

type Logger = {
  debug: (message: string, ...rest: any[]) => void;
  info: (message: string, ...rest: any[]) => void;
  warn: (message: string, ...rest: any[]) => void;
  error: (message: string, ...rest: any[]) => void;
};

export interface Options {
  fetch: Fetch;
  logger: Logger;
  reconnectDelay: number;
}

export interface BatchOptions {
  /** Time window in milliseconds to batch events together.
   * If specified, events will be delayed from the first received event until this duration has elapsed.
   * */
  windowTime?: number;
  /** Idle time in milliseconds to batch events together.
   * If specified, events will be delayed until there is a pause in events for at least this duration.
   * */
  idleTime?: number;
  /** Maximum number of messages to batch together. */
  messageLimit?: number;
}

export class EventSource {
  private created = false;
  private subscriptions = new Map<string, Map<EventHandler, EventType>>();
  private options: Options;

  public isConnected: boolean = false;

  constructor(
    private streamUrl: string,
    options?: Partial<Options>,
  ) {
    this.options = {
      fetch: fetch,
      reconnectDelay: 3000,
      logger: console,
      ...options,
    };
  }

  async connect(signal?: AbortSignal) {
    if (this.created) {
      return;
    }

    await this.createEventSource(signal);
  }

  private async createEventSource(signal?: AbortSignal) {
    return new Promise<void>((resolve, reject) => {
      fetchEventSource(this.streamUrl, {
        fetch: this.options.fetch,
        onopen: (response) => this.onOpen(response).then(resolve).catch(reject),
        onerror: this.onError.bind(this),
        onmessage: this.onMessage.bind(this),
        signal: signal,
        openWhenHidden: true, // Keep connection open even when the page is not visible
      });

      this.created = true;
    });
  }

  private async onOpen(response: Response) {
    if (
      response.ok &&
      response.headers.get("content-type") === EventStreamContentType
    ) {
      this.isConnected = true;
      this.options.logger.info(`Connected to event stream ${this.streamUrl}`);
    } else {
      this.options.logger.error(
        `Failed to connect to event stream ${this.streamUrl}. Status: ${response.status}`,
      );
      throw new HttpError(response.status);
    }
  }

  private onError(error: any) {
    this.isConnected = false;
    this.options.logger.warn(
      "Event stream connection lost. Attempting to reconnect...",
      error,
    );

    // do nothing to automatically retry or return a specific retry interval here.
    return this.options.reconnectDelay;
  }

  private onMessage(message: EventSourceMessage) {
    if (message.event) {
      const handlers = this.subscriptions.get(message.event);
      this.options.logger.debug(
        `Received event '${message.event}' message, dispatching to ${handlers?.size || 0} handlers.`,
      );
      if (handlers) {
        handlers.forEach((eventType, handler) => {
          const data = deserializeString(message.data, eventType);
          handler(data);
        });
      }
    } else {
      const comment = JSON.parse(message.data);
      this.options.logger.debug(`Received comment '${comment}' message.`);
    }
  }

  /**
   * Subscribe to a specific event type.
   * Returns an unsubscribe function.
   */
  subscribe<T>(
    eventType: EventType<T>,
    handler: EventHandler<T>,
  ): Subscription {
    const eventName = eventType.eventName;

    this.options.logger.debug(`Subscribing to event '${eventName}'`);

    // Add handler to our map
    if (!this.subscriptions.has(eventName)) {
      this.subscriptions.set(eventName, new Map());
    }
    this.subscriptions.get(eventName)!.set(handler, eventType);

    // Return unsubscribe function
    return {
      dispose: () => {
        const handlers = this.subscriptions.get(eventName);
        if (handlers) {
          handlers.delete(handler);
          if (handlers.size === 0) {
            this.subscriptions.delete(eventName);
          }
        }
      },
    };
  }

  batchSubscribe<T>(
    eventType: EventType<T>,
    handler: EventHandler<T[]>,
    options?: BatchOptions,
  ): Subscription {
    const windowTime =
      !options?.windowTime && !options?.idleTime ? 1_000 : options?.windowTime; // Default to 1 second if no batching time options are provided
    const idleTime = options?.idleTime;
    const messageLimit = options?.messageLimit;

    const queuedEvents: T[] = [];
    let windowTimer: TimeoutHandle | null = null;
    let idleTimer: TimeoutHandle | null = null;

    const clearWindowTimer = () => {
      if (windowTimer) {
        clearTimeout(windowTimer);
        windowTimer = null;
      }
    };

    const clearIdleTimer = () => {
      if (idleTimer) {
        clearTimeout(idleTimer);
        idleTimer = null;
      }
    };

    const flush = () => {
      clearWindowTimer();
      clearIdleTimer();

      if (queuedEvents.length === 0) {
        return;
      }

      const batch = queuedEvents.splice(0, queuedEvents.length);
      handler(batch);
    };

    const subscription = this.subscribe(eventType, (event) => {
      queuedEvents.push(event);

      if (
        messageLimit !== undefined &&
        messageLimit > 0 &&
        queuedEvents.length >= messageLimit
      ) {
        flush();
        return;
      }

      if (windowTime !== undefined && windowTime > 0 && !windowTimer) {
        windowTimer = setTimeout(flush, windowTime);
      }

      if (idleTime !== undefined && idleTime > 0) {
        clearIdleTimer();
        idleTimer = setTimeout(flush, idleTime);
      }
    });

    return {
      dispose: () => {
        subscription.dispose();
        clearWindowTimer();
        clearIdleTimer();
        queuedEvents.length = 0;
      },
    };
  }

  /**
   * Disconnect and clean up
   */
  disconnect(): void {
    this.isConnected = false;
    this.subscriptions.clear();
  }
}

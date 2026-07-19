// Bounded queues (SPEC-011 SDK-046).
//
// Unbounded queues are prohibited, and the reason is that they do not fail —
// they degrade. A client that cannot keep up with a busy subscription grows
// its backlog until the tab dies, and the symptom (memory) is nowhere near
// the cause (a slow callback). A bounded queue turns that into either
// backpressure or a typed error, both of which point at what happened.

/** The queue filled and backpressure could not be applied in time. */
export class QueueOverflowError extends Error {
  readonly capacity: number;
  constructor(name: string, capacity: number, timeoutMs: number) {
    super(
      `${name} queue overflowed: ${capacity} items and no room within ${timeoutMs}ms. ` +
        `The consumer is not keeping up.`,
    );
    this.name = 'QueueOverflowError';
    this.capacity = capacity;
  }
}

export interface BoundedQueueOptions {
  /** Documented capacity (SDK-046 requires one). */
  capacity: number;
  /** How long a producer waits for room before the connection is failed. */
  timeoutMs?: number;
  /** Used in the overflow message. */
  name?: string;
}

interface Waiter<T> {
  resolve: (value: T) => void;
  reject: (err: Error) => void;
  timer: ReturnType<typeof setTimeout> | null;
}

/**
 * A FIFO queue with a hard capacity.
 *
 * `push` resolves once the item is queued, which is immediately while there is
 * room and only after a consumer drains one when there is not. Awaiting it is
 * how backpressure reaches the transport: a caller that stops reading the
 * socket while this promise is pending is doing exactly what SDK-046 asks for,
 * rather than buffering messages nobody has asked for yet.
 */
export class BoundedQueue<T> {
  readonly #items: T[] = [];
  readonly #capacity: number;
  readonly #timeoutMs: number;
  readonly #name: string;

  /** Producers waiting for room. */
  readonly #producers: (Waiter<void> & { item: T })[] = [];
  /** Consumers waiting for an item. */
  readonly #consumers: Waiter<T>[] = [];

  #closed: Error | null = null;

  constructor(options: BoundedQueueOptions) {
    if (options.capacity < 1) throw new RangeError('queue capacity must be at least 1');
    this.#capacity = options.capacity;
    this.#timeoutMs = options.timeoutMs ?? 30_000;
    this.#name = options.name ?? 'bounded';
  }

  /** Items currently queued. */
  get length(): number {
    return this.#items.length;
  }

  /** The documented capacity (SDK-046). */
  get capacity(): number {
    return this.#capacity;
  }

  /** True once the queue is at capacity — the backpressure signal. */
  get full(): boolean {
    return this.#items.length >= this.#capacity;
  }

  /** Producers currently blocked waiting for room. */
  get waitingProducers(): number {
    return this.#producers.length;
  }

  /**
   * Enqueue, awaiting room if the queue is full.
   *
   * Rejects with {@link QueueOverflowError} if no room appears within the
   * timeout: at that point the consumer is not merely slow but stopped, and
   * failing the connection loudly beats dropping messages silently.
   */
  push(item: T): Promise<void> {
    if (this.#closed !== null) return Promise.reject(this.#closed);

    // Hand straight to a waiting consumer: queueing it first would let a
    // later item overtake it once the consumer resumes.
    const consumer = this.#consumers.shift();
    if (consumer !== undefined) {
      if (consumer.timer !== null) clearTimeout(consumer.timer);
      consumer.resolve(item);
      return Promise.resolve();
    }

    if (!this.full) {
      this.#items.push(item);
      return Promise.resolve();
    }

    return new Promise<void>((resolve, reject) => {
      const waiter: Waiter<void> & { item: T } = {
        item,
        resolve,
        reject,
        timer: null,
      };
      waiter.timer = setTimeout(() => {
        const index = this.#producers.indexOf(waiter);
        if (index >= 0) this.#producers.splice(index, 1);
        reject(new QueueOverflowError(this.#name, this.#capacity, this.#timeoutMs));
      }, this.#timeoutMs);
      this.#producers.push(waiter);
    });
  }

  /** Take the next item, awaiting one if the queue is empty. */
  shift(): Promise<T> {
    if (this.#items.length > 0) {
      // Non-null: length was just checked, and nothing runs in between.
      const item = this.#items.shift() as T;
      this.#admitProducer();
      return Promise.resolve(item);
    }
    if (this.#closed !== null) return Promise.reject(this.#closed);

    return new Promise<T>((resolve, reject) => {
      this.#consumers.push({ resolve, reject, timer: null });
    });
  }

  /** Take the next item if one is queued, without waiting. */
  tryShift(): T | undefined {
    if (this.#items.length === 0) return undefined;
    const item = this.#items.shift() as T;
    this.#admitProducer();
    return item;
  }

  /**
   * Fail every pending producer and consumer.
   *
   * Callers awaiting this queue would otherwise hang forever on a connection
   * that is already gone — the failure mode a bounded queue exists to avoid.
   */
  close(reason: Error): void {
    if (this.#closed !== null) return;
    this.#closed = reason;
    for (const producer of this.#producers.splice(0)) {
      if (producer.timer !== null) clearTimeout(producer.timer);
      producer.reject(reason);
    }
    for (const consumer of this.#consumers.splice(0)) {
      if (consumer.timer !== null) clearTimeout(consumer.timer);
      consumer.reject(reason);
    }
  }

  /** Let one blocked producer through, now that a slot opened. */
  #admitProducer(): void {
    const producer = this.#producers.shift();
    if (producer === undefined) return;
    if (producer.timer !== null) clearTimeout(producer.timer);
    this.#items.push(producer.item);
    producer.resolve();
  }
}

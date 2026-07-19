// Bounded queues and reconnect (SDK-046, SDK-047, acceptance 13).
import assert from 'node:assert/strict';
import { test } from 'node:test';

import { BoundedQueue, QueueOverflowError } from '../src/queue.ts';
import { backoffDelay, reconnect, ReconnectFailedError } from '../src/reconnect.ts';
import type { ReconnectHandlers } from '../src/reconnect.ts';

// --- Bounded queues (SDK-046) ----------------------------------------------

test('a queue accepts up to its capacity without blocking', async () => {
  const q = new BoundedQueue<number>({ capacity: 2 });
  await q.push(1);
  await q.push(2);
  assert.equal(q.length, 2);
  assert.equal(q.full, true);
});

test('a full queue applies backpressure instead of growing', async () => {
  // The producer's push stays pending. A caller awaiting it is not reading
  // the socket, which is exactly the backpressure SDK-046 asks for — as
  // opposed to buffering messages nobody has asked for.
  const q = new BoundedQueue<number>({ capacity: 1 });
  await q.push(1);

  let admitted = false;
  const blocked = q.push(2).then(() => {
    admitted = true;
  });

  await new Promise((resolve) => setTimeout(resolve, 5));
  assert.equal(admitted, false, 'the producer is held, not accepted');
  assert.equal(q.length, 1, 'the queue did not grow past capacity');
  assert.equal(q.waitingProducers, 1);

  assert.equal(await q.shift(), 1);
  await blocked;
  assert.equal(admitted, true, 'draining one admits the waiting producer');
  assert.equal(q.length, 1);
});

test('no message is dropped under backpressure', async () => {
  // The property that matters more than any timing detail: everything the
  // producer sent comes out, in order.
  const q = new BoundedQueue<number>({ capacity: 2 });
  const sent = [1, 2, 3, 4, 5];
  const producer = (async () => {
    for (const n of sent) await q.push(n);
  })();

  const received: number[] = [];
  for (let i = 0; i < sent.length; i += 1) received.push(await q.shift());
  await producer;

  assert.deepEqual(received, sent);
});

test('a stopped consumer overflows with a typed error rather than silence', async () => {
  // SDK-046: if backpressure cannot be applied within the timeout, fail the
  // connection loudly. Silent loss is the one outcome that is never allowed.
  const q = new BoundedQueue<number>({ capacity: 1, timeoutMs: 10, name: 'inbound' });
  await q.push(1);

  await assert.rejects(q.push(2), (err: unknown) => {
    assert.ok(err instanceof QueueOverflowError);
    assert.equal(err.capacity, 1);
    assert.match(err.message, /not keeping up/);
    return true;
  });
});

test('an item handed to a waiting consumer keeps its order', async () => {
  const q = new BoundedQueue<number>({ capacity: 4 });
  const first = q.shift();
  await q.push(1);
  await q.push(2);
  assert.equal(await first, 1, 'the waiting consumer got the first item');
  assert.equal(await q.shift(), 2);
});

test('closing fails everyone waiting instead of hanging them', async () => {
  const q = new BoundedQueue<number>({ capacity: 1 });
  await q.push(1);
  const blockedProducer = q.push(2);

  const q2 = new BoundedQueue<number>({ capacity: 1 });
  const blockedConsumer = q2.shift();

  q.close(new Error('connection lost'));
  q2.close(new Error('connection lost'));

  await assert.rejects(blockedProducer, /connection lost/);
  await assert.rejects(blockedConsumer, /connection lost/);
});

test('tryShift drains without waiting', () => {
  const q = new BoundedQueue<number>({ capacity: 2 });
  assert.equal(q.tryShift(), undefined);
  void q.push(1);
  assert.equal(q.tryShift(), 1);
  assert.equal(q.tryShift(), undefined);
});

test('a capacity below one is rejected at construction', () => {
  assert.throws(() => new BoundedQueue<number>({ capacity: 0 }), RangeError);
});

// --- Backoff (SDK-047) ------------------------------------------------------

test('backoff grows exponentially and stops at the ceiling', () => {
  const opts = { initialMs: 100, factor: 2, maxMs: 1000, jitter: 0 };
  assert.equal(backoffDelay(0, opts), 100);
  assert.equal(backoffDelay(1, opts), 200);
  assert.equal(backoffDelay(2, opts), 400);
  assert.equal(backoffDelay(10, opts), 1000, 'clamped, not unbounded');
});

test('jitter spreads the delay without going negative', () => {
  // Without jitter every client knocked off by one server restart returns on
  // the same schedule and recreates the load that took it down.
  const opts = { initialMs: 100, factor: 2, maxMs: 1000, jitter: 0.5 };
  const samples = Array.from({ length: 200 }, () => backoffDelay(0, opts));
  assert.ok(Math.min(...samples) >= 50, 'never below the jitter floor');
  assert.ok(Math.max(...samples) <= 150, 'never above the jitter ceiling');
  assert.ok(new Set(samples).size > 1, 'actually varies');
});

// --- Reconnect loop (SDK-047, acceptance 13) --------------------------------

function handlers(overrides: Partial<ReconnectHandlers> = {}): {
  handlers: ReconnectHandlers;
  order: string[];
} {
  const order: string[] = [];
  const track =
    (name: string, override?: () => Promise<void>) =>
    async (): Promise<void> => {
      order.push(name);
      if (override) await override();
    };
  return {
    order,
    handlers: {
      connect: track('connect', overrides.connect),
      authenticate: track('authenticate', overrides.authenticate),
      resubscribe: track('resubscribe', overrides.resubscribe),
      reconcile: track('reconcile', overrides.reconcile),
    },
  };
}

test('a successful reconnect runs connect, auth, resubscribe, reconcile in order', async () => {
  // The order is load-bearing: reconciling before resubscribing would compare
  // the cache against an InitialData missing the application's queries, and
  // delete every row it could not see.
  const { handlers: h, order } = handlers();
  const attempts = await reconnect(h, { sleep: async () => {} });

  assert.equal(attempts, 1);
  assert.deepEqual(order, ['connect', 'authenticate', 'resubscribe', 'reconcile']);
});

test('a failing connect is retried with growing delays', async () => {
  const delays: number[] = [];
  let failures = 2;
  const { handlers: h } = handlers({
    connect: async () => {
      if (failures-- > 0) throw new Error('ECONNREFUSED');
    },
  });

  const attempts = await reconnect(h, {
    sleep: async (ms) => {
      delays.push(ms);
    },
    initialMs: 100,
    factor: 2,
    jitter: 0,
  });

  assert.equal(attempts, 3, 'succeeded on the third attempt');
  assert.deepEqual(delays, [100, 200], 'no delay before the first try, then growth');
});

test('giving up surfaces the last failure, not a generic one', async () => {
  const { handlers: h } = handlers({
    connect: async () => {
      throw new Error('DNS lookup failed');
    },
  });

  await assert.rejects(reconnect(h, { sleep: async () => {}, maxAttempts: 3 }), (err: unknown) => {
    assert.ok(err instanceof ReconnectFailedError);
    assert.equal(err.attempts, 3);
    assert.match(err.last.message, /DNS lookup failed/);
    return true;
  });
});

test('a failure after authenticating restarts the whole sequence', async () => {
  // A half-rebuilt session is worse than none: resubscribing onto a
  // connection whose auth succeeded but whose subscribe failed would leave
  // the cache reconciling against partial data.
  let resubFailures = 1;
  const { handlers: h, order } = handlers({
    resubscribe: async () => {
      if (resubFailures-- > 0) throw new Error('subscribe rejected');
    },
  });

  await reconnect(h, { sleep: async () => {} });

  assert.deepEqual(order, [
    'connect',
    'authenticate',
    'resubscribe',
    'connect',
    'authenticate',
    'resubscribe',
    'reconcile',
  ]);
});

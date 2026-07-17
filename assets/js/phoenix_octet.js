// PhoenixOctet client: binary ingress over a Phoenix channel with
// credit-based stop-and-wait flow control.
//
// The transfer protocol frames each upload as begin -> chunks -> commit on an
// "octet:<sink>" channel. One chunk is in flight at a time: awaiting the
// server's per-chunk reply is the credit for the next, so consumer
// backpressure reaches this sender. Reliability and ordering come from the
// underlying transport; errors and timeouts abort (confirmed via "abort").
//
// `createQueue()` provides the serialized executor most apps want around
// transfers: strictly FIFO per owner, and poisoned if a cancellation cannot
// be confirmed — so later uploads can never interleave with an undead
// transfer.

export const DEFAULT_CHUNK_BYTES = 1024 * 1024

/** Joins (or reuses) the octet channel for a sink id on a Phoenix Socket. */
export function joinOctetChannel(socket, sinkId) {
  return new Promise((resolve, reject) => {
    const chan = socket.channel(`octet:${sinkId}`, {})
    chan
      .join()
      .receive("ok", () => resolve(chan))
      .receive("error", (e) => reject(new Error(`octet join failed: ${reason(e)}`)))
      .receive("timeout", () => reject(new Error("octet join timed out")))
  })
}

/**
 * Streams `bytes` (Uint8Array/ArrayBuffer/Blob parts already materialized)
 * through the channel. Resolves when the server has committed the binary.
 */
export async function transfer(channel, id, bytes, opts = {}) {
  const chunkBytes = opts.chunkBytes || DEFAULT_CHUNK_BYTES
  const timeout = opts.timeout || 30000
  const u8 = bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes)
  await push(channel, "begin", { id, size: u8.byteLength }, timeout)
  for (let offset = 0; offset < u8.byteLength; offset += chunkBytes) {
    const end = Math.min(offset + chunkBytes, u8.byteLength)
    // stop-and-wait: awaiting the reply is the credit for the next chunk
    await push(channel, "chunk", u8.buffer.slice(u8.byteOffset + offset, u8.byteOffset + end), timeout)
  }
  return push(channel, "commit", { id }, timeout)
}

/** Confirmed, idempotent cancellation of an in-flight transfer. */
export function abort(channel, id, timeout = 10000) {
  return push(channel, "abort", { id }, timeout)
}

/**
 * A serialized upload executor: `enqueue(owner, task)` runs tasks strictly
 * FIFO per owner. A task rejection carrying `cancellationUnconfirmed = true`
 * poisons the owner's queue; subsequent tasks reject immediately.
 */
export function createQueue() {
  const queues = new WeakMap()

  return function enqueue(owner, task) {
    let state = queues.get(owner)
    if (state && state.poisoned) return Promise.reject(poisonedError(state))
    if (!state) {
      state = { tail: Promise.resolve(), poisoned: false, reason: null }
      queues.set(owner, state)
    }
    const prior = state.tail
    const run = prior.then(
      () => task(),
      () => {
        if (state.poisoned) throw poisonedError(state)
        return task()
      },
    )
    const settled = run.then(
      () => undefined,
      (error) => {
        if (error && error.cancellationUnconfirmed) {
          state.poisoned = true
          state.reason = error.message
        }
        if (state.poisoned) throw poisonedError(state)
        return undefined
      },
    )
    state.tail = settled
    settled.then(
      () => {
        if (queues.get(owner) === state && state.tail === settled) queues.delete(owner)
      },
      () => undefined,
    )
    return run
  }
}

function poisonedError(state) {
  const error = new Error(`octet queue blocked: ${state.reason || "cancellation was not confirmed"}`)
  error.octetQueueBlocked = true
  return error
}

function push(channel, event, payload, timeout) {
  return new Promise((resolve, reject) => {
    channel
      .push(event, payload, timeout)
      .receive("ok", resolve)
      .receive("error", (e) => reject(new Error(`octet ${event} failed: ${reason(e)}`)))
      .receive("timeout", () => reject(new Error(`octet ${event} timed out`)))
  })
}

function reason(e) {
  return String((e && (e.reason || e.message)) || JSON.stringify(e))
}

// PhoenixOctet client: binary ingress over a Phoenix channel.
//
// An upload is ONE binary frame — a length-prefixed id followed by the
// payload — and the push reply is the acknowledgment. Ordering and
// reliability are the transport's job; pacing beyond one-reply-per-upload
// belongs to the caller (`createQueue()` serializes uploads per owner).
//
// Configure your server socket's max_frame_size to fit your uploads: a
// frame is a whole upload.

/** Joins the octet channel for a sink id on a connected Phoenix Socket. */
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
 * Uploads `bytes` (Uint8Array or ArrayBuffer) under `id` in a single frame.
 * Resolves with the server reply (`{ bytes }`) once acknowledged.
 */
export function upload(channel, id, bytes, timeout = 30000) {
  return new Promise((resolve, reject) => {
    channel
      .push("upload", encodeFrame(id, bytes), timeout)
      .receive("ok", resolve)
      .receive("error", (e) => reject(new Error(`octet upload failed: ${reason(e)}`)))
      .receive("timeout", () => reject(new Error("octet upload timed out")))
  })
}

/**
 * Pushes a stateless cancellation for `id`. Ordered after any upload pushed
 * on the same channel before it, so server-side delivery of the cancel
 * follows delivery of the upload it chases.
 */
export function cancel(channel, id, timeout = 10000) {
  return new Promise((resolve, reject) => {
    channel
      .push("cancel", { id }, timeout)
      .receive("ok", resolve)
      .receive("error", (e) => reject(new Error(`octet cancel failed: ${reason(e)}`)))
      .receive("timeout", () => reject(new Error("octet cancel timed out")))
  })
}

/** The frame layout: <<id byte length::8, id utf8, payload>>. */
export function encodeFrame(id, bytes) {
  const idBytes = new TextEncoder().encode(id)
  if (idBytes.byteLength === 0 || idBytes.byteLength > 255) {
    throw new Error("octet upload id must encode to 1..255 bytes")
  }
  const u8 = bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes)
  const frame = new Uint8Array(1 + idBytes.byteLength + u8.byteLength)
  frame[0] = idBytes.byteLength
  frame.set(idBytes, 1)
  frame.set(u8, 1 + idBytes.byteLength)
  return frame.buffer
}

/**
 * A serialized upload executor: `enqueue(owner, task)` runs tasks strictly
 * FIFO per owner. One upload in flight at a time is the flow control most
 * apps need on a reliable local transport.
 */
export function createQueue() {
  const queues = new WeakMap()

  return function enqueue(owner, task) {
    let state = queues.get(owner)
    if (!state) {
      state = { tail: Promise.resolve() }
      queues.set(owner, state)
    }
    const run = state.tail.then(task, task)
    const settled = run.then(
      () => undefined,
      () => undefined,
    )
    state.tail = settled
    settled.then(() => {
      if (queues.get(owner) === state && state.tail === settled) queues.delete(owner)
    })
    return run
  }
}

function reason(e) {
  return String((e && (e.reason || e.message)) || JSON.stringify(e))
}

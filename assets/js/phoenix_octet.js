// PhoenixOctet client: binary ingress over a Phoenix channel.
//
// An upload is ONE binary frame — a length-prefixed id followed by the
// payload — and the push reply is the acknowledgment. Ordering and
// reliability are the transport's job: pushes on one channel arrive and are
// processed in push order, so concurrent uploads need no client-side
// pacing on the reliable local transports this targets.
//
// Configure your server socket's max_frame_size to fit your uploads: a
// frame is a whole upload.

/**
 * Joins the octet channel for a sink id once the Phoenix Socket is open.
 *
 * `Socket.connect()` starts a connection asynchronously. Calling
 * `Channel.join()` before that connection opens starts Phoenix's join timeout
 * while the join is only buffered locally. Waiting for `onOpen` keeps that
 * timeout scoped to the server join itself, including while the Socket is
 * reconnecting after a server restart. The separate socket-open wait is
 * bounded because Phoenix's channel-join timeout does not start until
 * `Channel.join()` is called.
 *
 * Pass `{ openTimeout, signal, closedGraceMs }` to customize that wait.
 * `openTimeout` defaults to 10 seconds; `signal` can abort while the socket
 * is still unopened. Once channel joining starts, Phoenix owns that phase's
 * timeout.
 *
 * A socket whose `connectionState()` reports `"closed"` has no dial in
 * flight — waiting the full `openTimeout` on it can only succeed if some
 * other actor revives the transport, and hosts that memoize sockets turn
 * that wait into a constant full-deadline stall on every join. If the socket
 * still reports `"closed"` after `closedGraceMs` (default 1 second — long
 * enough for a reconnect backoff to flip it back to `"connecting"`), the
 * join rejects early with `octet socket closed` so the caller can dial a
 * fresh socket. A socket without `connectionState` keeps the plain
 * `openTimeout` wait.
 */
export function joinOctetChannel(socket, sinkId, options = {}) {
  const { openTimeout = 10000, signal, closedGraceMs = 1000 } = options
  if (!Number.isFinite(openTimeout) || openTimeout < 0) {
    throw new TypeError("octet socket open timeout must be a finite non-negative number")
  }
  if (!Number.isFinite(closedGraceMs) || closedGraceMs < 0) {
    throw new TypeError("octet socket closed grace must be a finite non-negative number")
  }

  return new Promise((resolve, reject) => {
    let openRef = null
    let openTimer = null
    let closedGraceTimer = null
    let joining = false
    let settled = false

    const removeOpenListener = () => {
      if (openRef === null) return
      socket.off([openRef])
      openRef = null
    }

    const abortOpenWait = () => {
      rejectOpenWait(abortError(signal))
    }

    const cleanupOpenWait = () => {
      removeOpenListener()
      if (openTimer !== null) {
        clearTimeout(openTimer)
        openTimer = null
      }
      if (closedGraceTimer !== null) {
        clearTimeout(closedGraceTimer)
        closedGraceTimer = null
      }
      signal?.removeEventListener("abort", abortOpenWait)
    }

    const rejectOpenWait = (error) => {
      if (joining || settled) return
      settled = true
      cleanupOpenWait()
      reject(error)
    }

    const join = () => {
      if (joining || settled) return
      joining = true
      cleanupOpenWait()

      const chan = socket.channel(`octet:${sinkId}`, {})

      const resolveJoin = () => {
        if (settled) return
        settled = true
        resolve(chan)
      }

      const rejectJoin = (error) => {
        if (settled) return
        settled = true
        try {
          chan.leave()
        } catch (_) {
          // Preserve the join failure. The channel is already unusable to the
          // caller, and Phoenix may throw if transport teardown won the race.
        }
        reject(error)
      }

      chan
        .join()
        .receive("ok", resolveJoin)
        .receive("error", (e) => rejectJoin(new Error(`octet join failed: ${reason(e)}`)))
        .receive("timeout", () => rejectJoin(new Error("octet join timed out")))
    }

    if (socket.isConnected()) {
      join()
    } else {
      if (signal?.aborted) {
        rejectOpenWait(abortError(signal))
        return
      }

      signal?.addEventListener("abort", abortOpenWait, { once: true })
      openTimer = setTimeout(
        () => rejectOpenWait(new Error("octet socket open timed out")),
        openTimeout,
      )
      openRef = socket.onOpen(join)

      // Do not miss an open transition between the first state check and
      // registering the callback. `onOpen` implementations may also invoke
      // the callback synchronously, so clean up the returned ref afterward.
      if (joining || settled) cleanupOpenWait()
      else if (signal?.aborted) rejectOpenWait(abortError(signal))
      else if (socket.isConnected()) join()
      else if (
        typeof socket.connectionState === "function" &&
        socket.connectionState() === "closed" &&
        closedGraceMs < openTimeout
      ) {
        closedGraceTimer = setTimeout(() => {
          closedGraceTimer = null
          if (joining || settled) return
          if (socket.connectionState() === "closed") {
            rejectOpenWait(new Error("octet socket closed"))
          }
          // "connecting"/"closing": a dial is in flight again; the open
          // wait's own timeout keeps governing.
        }, closedGraceMs)
      }
    }
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

function reason(e) {
  return String((e && (e.reason || e.message)) || JSON.stringify(e))
}

function abortError(signal) {
  const error = new Error("octet socket open aborted")
  error.name = "AbortError"
  if (signal?.reason !== undefined) error.cause = signal.reason
  return error
}

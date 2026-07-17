import assert from "node:assert/strict"
import test from "node:test"

import { cancel, joinOctetChannel, upload } from "./phoenix_octet.js"

class FakeJoinPush {
  hooks = new Map()

  receive(status, callback) {
    this.hooks.set(status, callback)
    return this
  }

  trigger(status, payload = {}) {
    this.hooks.get(status)?.(payload)
  }
}

class FakeChannel {
  joinCalls = 0
  joinPush = new FakeJoinPush()
  leaveCalls = 0

  constructor(onLeave = () => {}) {
    this.onLeave = onLeave
  }

  join() {
    this.joinCalls += 1
    return this.joinPush
  }

  leave() {
    this.leaveCalls += 1
    this.onLeave()
  }
}

class FakeSocket {
  activeChannels = new Set()
  channelCalls = []
  connected = false
  listeners = new Map()
  nextRef = 0

  isConnected() {
    return this.connected
  }

  onOpen(callback) {
    const ref = ++this.nextRef
    this.listeners.set(ref, callback)
    return ref
  }

  off(refs) {
    for (const ref of refs) this.listeners.delete(ref)
  }

  channel(topic, params) {
    let channel
    channel = new FakeChannel(() => this.activeChannels.delete(channel))
    this.activeChannels.add(channel)
    this.channelCalls.push({ topic, params, channel })
    return channel
  }

  open() {
    this.connected = true
    for (const callback of [...this.listeners.values()]) callback()
  }
}

test("defers channel creation and join until a reconnecting socket opens", async () => {
  const socket = new FakeSocket()
  const joined = joinOctetChannel(socket, "sink-1")

  assert.equal(socket.channelCalls.length, 0)
  assert.equal(socket.listeners.size, 1)

  socket.open()

  assert.equal(socket.channelCalls.length, 1)
  const [{ topic, params, channel }] = socket.channelCalls
  assert.equal(topic, "octet:sink-1")
  assert.deepEqual(params, {})
  assert.equal(channel.joinCalls, 1)
  assert.equal(socket.listeners.size, 0)

  channel.joinPush.trigger("ok")
  assert.equal(await joined, channel)
})

test("joins immediately when the socket is already open", async () => {
  const socket = new FakeSocket()
  socket.connected = true

  const joined = joinOctetChannel(socket, "sink-2")

  assert.equal(socket.channelCalls.length, 1)
  assert.equal(socket.listeners.size, 0)
  const [{ channel }] = socket.channelCalls
  assert.equal(channel.joinCalls, 1)

  channel.joinPush.trigger("ok")
  assert.equal(await joined, channel)
})

test("does not miss an open transition while registering its listener", async () => {
  const socket = new FakeSocket()
  socket.onOpen = function (callback) {
    const ref = FakeSocket.prototype.onOpen.call(this, callback)
    this.connected = true
    return ref
  }

  const joined = joinOctetChannel(socket, "sink-3")

  assert.equal(socket.channelCalls.length, 1)
  assert.equal(socket.listeners.size, 0)
  const [{ channel }] = socket.channelCalls
  assert.equal(channel.joinCalls, 1)

  channel.joinPush.trigger("ok")
  assert.equal(await joined, channel)
})

test("rejects and removes its listener when a socket never opens", async () => {
  const socket = new FakeSocket()
  const joined = joinOctetChannel(socket, "sink-never-opens", { openTimeout: 0 })

  assert.equal(socket.channelCalls.length, 0)
  assert.equal(socket.listeners.size, 1)

  await assert.rejects(joined, /octet socket open timed out/)
  assert.equal(socket.listeners.size, 0)
  assert.equal(socket.channelCalls.length, 0)

  socket.open()
  assert.equal(socket.channelCalls.length, 0)
})

test("can abort the socket-open wait without creating a channel", async () => {
  const socket = new FakeSocket()
  const controller = new AbortController()
  const joined = joinOctetChannel(socket, "sink-aborted", {
    openTimeout: 10000,
    signal: controller.signal,
  })

  assert.equal(socket.listeners.size, 1)
  controller.abort("owner stopped")

  await assert.rejects(joined, (error) => {
    assert.equal(error.name, "AbortError")
    assert.match(error.message, /octet socket open aborted/)
    assert.equal(error.cause, "owner stopped")
    return true
  })
  assert.equal(socket.listeners.size, 0)
  assert.equal(socket.channelCalls.length, 0)
})

for (const [status, payload, message] of [
  ["timeout", {}, /octet join timed out/],
  ["error", { reason: "denied" }, /octet join failed: denied/],
]) {
  test(`leaves once before rejecting a join ${status}`, async () => {
    const socket = new FakeSocket()
    socket.connected = true

    const joined = joinOctetChannel(socket, `sink-${status}`)
    const [{ channel }] = socket.channelCalls
    channel.joinPush.trigger(status, payload)
    channel.joinPush.trigger(status, payload)
    channel.joinPush.trigger("ok")

    assert.equal(channel.leaveCalls, 1)
    assert.equal(socket.activeChannels.size, 0)
    await assert.rejects(joined, message)
    assert.equal(channel.leaveCalls, 1)
  })
}

test("uploads one binary frame and resolves with the server byte reply", async () => {
  const push = new FakeJoinPush()
  const calls = []
  const channel = {
    push(event, payload, timeout) {
      calls.push({ event, payload, timeout })
      return push
    },
  }
  const bytes = new Uint8Array([11, 22, 33])

  const uploaded = upload(channel, "upload-1", bytes, 4321)

  assert.equal(calls.length, 1)
  const [{ event, payload, timeout }] = calls
  assert.equal(event, "upload")
  assert.equal(timeout, 4321)

  const frame = new Uint8Array(payload)
  const idLength = frame[0]
  assert.equal(new TextDecoder().decode(frame.slice(1, 1 + idLength)), "upload-1")
  assert.deepEqual(frame.slice(1 + idLength), bytes)

  push.trigger("ok", { bytes: bytes.byteLength })
  assert.deepEqual(await uploaded, { bytes: 3 })
})

for (const failure of [
  {
    name: "upload error",
    id: "upload-error",
    event: "upload",
    timeout: 4101,
    status: "error",
    reply: { reason: "upload denied" },
    message: "octet upload failed: upload denied",
    invoke: (channel) => upload(channel, "upload-error", new Uint8Array([1, 2]), 4101),
  },
  {
    name: "upload timeout",
    id: "upload-timeout",
    event: "upload",
    timeout: 4102,
    status: "timeout",
    reply: {},
    message: "octet upload timed out",
    invoke: (channel) => upload(channel, "upload-timeout", new Uint8Array([3, 4]), 4102),
  },
  {
    name: "cancel error",
    id: "cancel-error",
    event: "cancel",
    timeout: 5101,
    status: "error",
    reply: { reason: "cancel denied" },
    message: "octet cancel failed: cancel denied",
    invoke: (channel) => cancel(channel, "cancel-error", 5101),
  },
  {
    name: "cancel timeout",
    id: "cancel-timeout",
    event: "cancel",
    timeout: 5102,
    status: "timeout",
    reply: {},
    message: "octet cancel timed out",
    invoke: (channel) => cancel(channel, "cancel-timeout", 5102),
  },
]) {
  test(`reports the exact ${failure.name} and forwards its deadline`, async () => {
    const push = new FakeJoinPush()
    const calls = []
    const channel = {
      push(event, payload, timeout) {
        calls.push({ event, payload, timeout })
        return push
      },
    }

    const pending = failure.invoke(channel)

    assert.equal(calls.length, 1)
    assert.equal(calls[0].event, failure.event)
    assert.equal(calls[0].timeout, failure.timeout)

    if (failure.event === "cancel") {
      assert.deepEqual(calls[0].payload, { id: failure.id })
    } else {
      assert.ok(calls[0].payload instanceof ArrayBuffer)
    }

    push.trigger(failure.status, failure.reply)

    await assert.rejects(pending, (error) => {
      assert.equal(error.message, failure.message)
      return true
    })
  })
}

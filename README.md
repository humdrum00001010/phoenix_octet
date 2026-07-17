# PhoenixOctet

Binary ingress over Phoenix Channels with credit-based flow control — the
missing piece between raw channels and LiveView uploads.

## Why

Phoenix Channels carry binary frames natively, but ship no upload semantics:
no framing, no size enforcement, and no flow control (the channel mailbox is
unbounded and the transport reads eagerly, so TCP backpressure never reaches
your process). LiveView's uploads provide all three — anchored, deliberately,
to a `live_file_input` in the DOM, because they are UI machinery: pickers,
previews, progress bars.

Programmatic byte producers — an in-browser editor engine exporting document
state, canvas blobs, recorded audio — need the upload semantics without the
DOM anchor. Asked about inputless uploads, LiveView maintainers point at
custom channels. This library is that custom channel, written once:

* **Framing**: `begin` (id + declared size) → binary `chunk` frames →
  `commit`; `abort` is a confirmed, idempotent cancellation.
* **Flow control**: credit-based stop-and-wait, window size one — the reply
  to each chunk is the credit for the next, so consumer backpressure reaches
  the sender. Ordering and reliability stay where they belong (the
  transport); the ack exists purely for pacing.
* **Size enforcement**: declared up front, checked per chunk, `commit`
  accepted only when received == declared.
* **Lifetime-scoped memory**: chunks accumulate as iodata in the channel
  process and die with it; delivery to the receiver is a refc-binary
  reference transfer. No temp files, no sweeper.

## Server

```elixir
defmodule MyAppWeb.OctetChannel do
  use PhoenixOctet.Channel, max_upload_bytes: 64 * 1024 * 1024

  @impl PhoenixOctet.Channel
  def handle_octet(sink_id, id, bytes, _socket) do
    PhoenixOctet.Sink.deliver(MyApp.PubSub, sink_id, id, bytes)
  end
end

defmodule MyAppWeb.OctetSocket do
  use Phoenix.Socket
  channel "octet:*", MyAppWeb.OctetChannel
  def connect(_params, socket, _info), do: {:ok, socket}
  def id(_socket), do: nil
end

# endpoint.ex
socket "/octet", MyAppWeb.OctetSocket, websocket: true, longpoll: false
```

The receiving process (typically a LiveView) mints an unguessable sink id and
subscribes; committed binaries arrive as messages:

```elixir
def mount(_params, _session, socket) do
  {sink_id, :ok} = PhoenixOctet.Sink.subscribe(MyApp.PubSub, connected?(socket))
  {:ok, assign(socket, :octet_sink_id, sink_id)}
end

def handle_info({:octet_upload, id, bytes}, socket) do
  # stash/persist; ack your client if your protocol wants ordering
  {:noreply, socket}
end
```

Render the sink id (`data-octet-sink={@octet_sink_id}`) for the client to
join on. The id is a bearer secret: whoever knows it can feed your sink, and
nobody else can.

## Client

```js
import { Socket } from "phoenix"
import { joinOctetChannel, transfer, abort, createQueue } from "phoenix_octet"

const socket = new Socket("/octet")
socket.connect()
const enqueue = createQueue()

async function uploadBytes(owner, sinkId, id, u8) {
  return enqueue(owner, async () => {
    const channel = await joinOctetChannel(socket, sinkId)
    await transfer(channel, id, u8) // stop-and-wait chunking inside
  })
}
```

`createQueue()` serializes uploads per owner and poisons the queue when a
cancellation cannot be confirmed (reject with `cancellationUnconfirmed =
true`), so later uploads can never interleave with an undead transfer.

One sharp edge worth knowing when your bytes come from WASM: fetch/WebSocket
payloads must not alias shared or growable WASM memory — copy first
(`u8.slice()`).

## Provenance

Extracted from [ecrits](https://github.com/humdrum00001010), a local-first
document editor whose browser WASM engines (LibreOffice, an HWP engine) ship
multi-megabyte document exports through this lane on idle-checkpoint and
save. The design history — including a working inputless-uploads patch to
LiveView itself and the maintainer guidance that led here — is in
[phoenixframework/phoenix_live_view#4333](https://github.com/phoenixframework/phoenix_live_view/pull/4333).

## License

MIT

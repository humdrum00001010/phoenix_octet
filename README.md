# PhoenixOctet

Binary ingress over Phoenix Channels — the missing piece between raw
channels and LiveView uploads, kept as small as the problem actually is.

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

* **Framing**: one binary frame per upload — a length-prefixed id followed
  by the payload. The push reply is the acknowledgment; an upload either
  fully happens or fully doesn't, so there is no transfer state and nothing
  to abort.
* **Flow control**: one reply per upload; serialize uploads with
  `createQueue()` for strictly-FIFO-per-owner pacing. Ordering and
  reliability stay where they belong: the transport.
* **Size enforcement**: checked server-side against `:max_upload_bytes`;
  set your socket's `max_frame_size` to match, since a frame is a whole
  upload.
* **Lifetime-scoped memory**: delivery to the receiver is a refc-binary
  reference transfer; anything unconsumed dies with its owner. No temp
  files, no sweeper.

Earlier versions carried a chunked begin/commit protocol with per-chunk
credit acknowledgments; on a reliable ordered transport that reintroduced
upload-machinery complexity (transfer state, abort, commit/abort races) for
problems — WAN backpressure, frame limits, progress — most deployments do
not have. If yours does, chunking belongs in a fork or a future opt-in, not
in everyone's hot path.

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
import { joinOctetChannel, upload, createQueue } from "phoenix_octet"

const socket = new Socket("/octet")
socket.connect()
const enqueue = createQueue()

async function uploadBytes(owner, sinkId, id, u8) {
  return enqueue(owner, async () => {
    const channel = await joinOctetChannel(socket, sinkId)
    await upload(channel, id, u8) // one frame; the reply is the ack
  })
}
```

`createQueue()` runs uploads strictly FIFO per owner — one in flight at a
time is the flow control most apps need on a reliable local transport.

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

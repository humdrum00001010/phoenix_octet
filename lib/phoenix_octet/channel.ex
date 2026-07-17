defmodule PhoenixOctet.Channel do
  @moduledoc """
  A Phoenix channel behaviour for binary ingress (`octet`, as in
  application/octet-stream).

  Phoenix Channels carry binary frames natively but ship no upload semantics:
  no framing, no size enforcement, and — because the channel mailbox is
  unbounded and the transport reads eagerly — no flow control. LiveView's
  uploads provide all three, but are deliberately DOM-anchored UI machinery.
  This module is the missing piece between them: upload semantics for raw
  binaries as a plain channel, nothing else.

  ## Protocol

  A client transfer is framed as three events on one channel:

    * `"begin"` — `%{"id" => id, "size" => declared_size}`
    * `"chunk"` — N binary frames, each individually acknowledged
    * `"commit"` — `%{"id" => id}`; accepted only when received == declared

  Flow control is credit-based stop-and-wait, window size one: the reply to
  each `"chunk"` is the credit for the next, so consumer backpressure reaches
  the sender. Reliability and ordering are the transport's job (WebSocket over
  TCP); the ack exists purely for pacing, so there is no retransmit path —
  errors and timeouts abort. `"abort"` (`%{"id" => id}`) is a confirmed,
  idempotent cancellation.

  Accumulated chunks live in the channel process as an iodata list and die
  with it: no temp files, no sweeper.

  ## Usage

      defmodule MyAppWeb.OctetChannel do
        use PhoenixOctet.Channel, max_upload_bytes: 64 * 1024 * 1024

        @impl PhoenixOctet.Channel
        def handle_octet(sink_id, id, bytes, _socket) do
          PhoenixOctet.Sink.deliver(MyApp.PubSub, sink_id, id, bytes)
        end
      end

      # in your socket module
      channel "octet:*", MyAppWeb.OctetChannel

  `handle_octet/4` receives every committed binary. `PhoenixOctet.Sink`
  implements the common LiveView hand-off (an unguessable per-process sink
  topic), but any delivery works.

  Override `authorize_join/3` to gate joins; the default accepts any non-empty
  sink id, which is appropriate when sink ids are unguessable bearer tokens
  minted by the receiving process.
  """

  @callback handle_octet(
              sink_id :: String.t(),
              id :: String.t(),
              bytes :: binary(),
              socket :: Phoenix.Socket.t()
            ) :: :ok

  @callback authorize_join(
              sink_id :: String.t(),
              params :: map(),
              socket :: Phoenix.Socket.t()
            ) :: :ok | {:error, term()}

  @doc """
  Invoked for every `"abort"`, after the in-flight transfer (if any) is
  forgotten. Because it runs in the channel process, anything it publishes is
  ordered AFTER a commit that raced just ahead of the abort — deliver a
  terminal cancellation here (see `PhoenixOctet.Sink.deliver_cancel/3`) so
  receivers can drop an already-delivered binary instead of stranding it.
  """
  @callback handle_octet_cancelled(
              sink_id :: String.t(),
              id :: String.t(),
              socket :: Phoenix.Socket.t()
            ) :: :ok

  @optional_callbacks authorize_join: 3, handle_octet_cancelled: 3

  defmacro __using__(opts) do
    quote do
      use Phoenix.Channel

      @behaviour PhoenixOctet.Channel
      @phoenix_octet_opts unquote(opts)

      @impl Phoenix.Channel
      def join("octet:" <> sink_id, params, socket) do
        PhoenixOctet.Protocol.join(__MODULE__, sink_id, params, socket)
      end

      @impl Phoenix.Channel
      def handle_in(event, payload, socket) do
        PhoenixOctet.Protocol.handle_in(
          __MODULE__,
          @phoenix_octet_opts,
          event,
          payload,
          socket
        )
      end

      @impl PhoenixOctet.Channel
      def authorize_join(sink_id, _params, _socket) when byte_size(sink_id) > 0, do: :ok
      def authorize_join(_sink_id, _params, _socket), do: {:error, :invalid_sink}

      @impl PhoenixOctet.Channel
      def handle_octet_cancelled(_sink_id, _id, _socket), do: :ok

      defoverridable authorize_join: 3, handle_octet_cancelled: 3
    end
  end
end

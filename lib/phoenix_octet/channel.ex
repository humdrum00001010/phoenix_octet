defmodule PhoenixOctet.Channel do
  @moduledoc """
  A Phoenix channel behaviour for binary ingress (`octet`, as in
  application/octet-stream).

  Phoenix Channels carry binary frames natively but ship no upload semantics,
  while LiveView's uploads are deliberately DOM-anchored UI machinery. This
  is the missing piece between them, kept as small as the problem actually
  is: an upload is **one binary frame** (length-prefixed id + payload), and
  the push reply is the acknowledgment. Ordering and reliability are the
  transport's job; pacing beyond one-reply-per-upload belongs to the caller
  (the JS client's queue serializes uploads per owner).

  Size is enforced against `:max_upload_bytes` (default 256 MiB). Configure
  your socket's `max_frame_size` to match — a frame is a whole upload.

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

  `handle_octet/4` receives every upload. `PhoenixOctet.Sink` implements the
  common LiveView hand-off (an unguessable per-process sink topic), but any
  delivery works.

  Override `authorize_join/3` to gate joins; the default accepts any
  non-empty sink id, which is appropriate when sink ids are unguessable
  bearer tokens minted by the receiving process.
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

  @optional_callbacks authorize_join: 3

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

      defoverridable authorize_join: 3
    end
  end
end

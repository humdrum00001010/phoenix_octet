defmodule PhoenixOctet.Sink do
  @moduledoc """
  The common receiving side: a per-process sink for committed binaries.

  A receiving process (typically a LiveView) mints an unguessable sink id,
  subscribes, and renders the id for the client to join `"octet:<sink-id>"`:

      def mount(_params, _session, socket) do
        {sink_id, :ok} = PhoenixOctet.Sink.subscribe(MyApp.PubSub, connected?(socket))
        {:ok, assign(socket, :octet_sink_id, sink_id)}
      end

      # committed binaries arrive as messages:
      def handle_info({:octet_upload, id, bytes}, socket), do: ...

  The channel delivers with `deliver/4` from `handle_octet/4`. Binaries over
  64 bytes are refc-counted on the BEAM, so delivery is a reference transfer,
  not a copy. Anything the receiver never consumes dies with it.
  """

  @doc """
  Mints a sink id and subscribes the calling process to its topic.

  Pass `subscribe?: false` (e.g. a disconnected LiveView render) to mint
  without subscribing.
  """
  def subscribe(pubsub, subscribe? \\ true) do
    sink_id = generate_sink_id()

    result =
      if subscribe? do
        Phoenix.PubSub.subscribe(pubsub, topic(sink_id))
      else
        :ok
      end

    {sink_id, result}
  end

  @doc "Delivers a committed binary to the sink's subscriber."
  def deliver(pubsub, sink_id, id, bytes)
      when is_binary(sink_id) and is_binary(id) and is_binary(bytes) do
    Phoenix.PubSub.broadcast(pubsub, topic(sink_id), {:octet_upload, id, bytes})
  end

  @doc "The PubSub topic for a sink id."
  def topic(sink_id), do: "octet_sink:" <> sink_id

  defp generate_sink_id do
    Base.url_encode64(:crypto.strong_rand_bytes(24), padding: false)
  end
end

defmodule PhoenixOctet.Protocol do
  @moduledoc """
  The single-message transfer behind `PhoenixOctet.Channel`.

  An upload is one binary frame: a length-prefixed id followed by the payload
  (see `encode_frame/2`/`decode_frame/1`). The transport already provides
  ordering and reliability, so there is no chunk protocol, no transfer state,
  and no abort — the push reply is the acknowledgment, and an upload either
  fully happens or fully doesn't.
  """

  import Phoenix.Socket, only: [assign: 3]

  @default_max_upload_bytes 256 * 1024 * 1024

  @doc "Client-side frame layout, documented for non-JS clients."
  def encode_frame(id, bytes) when is_binary(id) and byte_size(id) < 256 and is_binary(bytes) do
    <<byte_size(id)::8, id::binary, bytes::binary>>
  end

  def decode_frame(<<id_size::8, id::binary-size(id_size), bytes::binary>>) when id_size > 0 do
    {:ok, id, bytes}
  end

  def decode_frame(_other), do: :error

  def join(channel, sink_id, params, socket) do
    case channel.authorize_join(sink_id, params, socket) do
      :ok -> {:ok, assign(socket, :octet_sink_id, sink_id)}
      {:error, reason} -> {:error, %{reason: to_string(reason)}}
    end
  end

  def handle_in(channel, opts, "upload", {:binary, frame}, socket) do
    with {:ok, id, bytes} <- decode_frame(frame),
         :ok <- check_size(bytes, opts) do
      :ok = channel.handle_octet(socket.assigns.octet_sink_id, id, bytes, socket)
      {:reply, {:ok, %{bytes: byte_size(bytes)}}, socket}
    else
      :error -> {:reply, {:error, %{reason: "invalid octet frame"}}, socket}
      {:error, reason} -> {:reply, {:error, %{reason: reason}}, socket}
    end
  end

  # Stateless cancellation relay: nothing to clear (an upload is atomic), but
  # same-channel ordering means this runs after any upload pushed before it,
  # so the callback's delivery is ordered after that upload's delivery.
  def handle_in(channel, _opts, "cancel", %{"id" => id}, socket) when is_binary(id) do
    :ok = channel.handle_octet_cancelled(socket.assigns.octet_sink_id, id, socket)
    {:reply, :ok, socket}
  end

  def handle_in(_channel, _opts, _event, _payload, socket) do
    {:reply, {:error, %{reason: "unknown octet event"}}, socket}
  end

  defp check_size(bytes, opts) do
    if byte_size(bytes) > Keyword.get(opts, :max_upload_bytes, @default_max_upload_bytes) do
      {:error, "upload too large"}
    else
      :ok
    end
  end
end

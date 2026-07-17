defmodule PhoenixOctet.Protocol do
  @moduledoc """
  The begin → chunk → commit / abort state machine behind
  `PhoenixOctet.Channel`. One transfer in flight per channel; each chunk is
  individually acknowledged (the stop-and-wait credit).
  """

  import Phoenix.Socket, only: [assign: 3]

  @default_max_upload_bytes 256 * 1024 * 1024

  def join(channel, sink_id, params, socket) do
    case channel.authorize_join(sink_id, params, socket) do
      :ok -> {:ok, assign(socket, :octet, %{sink_id: sink_id, upload: nil})}
      {:error, reason} -> {:error, %{reason: to_string(reason)}}
    end
  end

  def handle_in(_channel, opts, "begin", %{"id" => id, "size" => size}, socket)
      when is_binary(id) and is_integer(size) do
    %{upload: upload} = socket.assigns.octet

    cond do
      upload != nil ->
        {:reply, {:error, %{reason: "upload already in progress"}}, socket}

      size < 0 or size > max_upload_bytes(opts) ->
        {:reply, {:error, %{reason: "upload too large"}}, socket}

      true ->
        {:reply, :ok, put_upload(socket, %{id: id, size: size, acc: [], received: 0})}
    end
  end

  def handle_in(_channel, opts, "chunk", {:binary, chunk}, socket) do
    case socket.assigns.octet.upload do
      nil ->
        {:reply, {:error, %{reason: "no upload in progress"}}, socket}

      %{received: received} = upload ->
        received = received + byte_size(chunk)

        if received > max_upload_bytes(opts) or received > upload.size do
          {:reply, {:error, %{reason: "upload exceeded declared size"}},
           put_upload(socket, nil)}
        else
          upload = %{upload | acc: [upload.acc | chunk], received: received}
          {:reply, :ok, put_upload(socket, upload)}
        end
    end
  end

  # Confirmed cancellation. Deliberately idempotent — "nothing in flight" is
  # also a confirmed state, so a client deadline miss can never be left
  # unconfirmable by a benign race. The cancellation callback runs from this
  # same channel process, so whatever it publishes is ordered after a commit
  # that won the race just ahead of the abort.
  def handle_in(channel, _opts, "abort", %{"id" => id}, socket) do
    socket =
      case socket.assigns.octet.upload do
        %{id: ^id} -> put_upload(socket, nil)
        _other -> socket
      end

    :ok = channel.handle_octet_cancelled(socket.assigns.octet.sink_id, id, socket)
    {:reply, :ok, socket}
  end

  def handle_in(channel, _opts, "commit", %{"id" => id}, socket) do
    case socket.assigns.octet.upload do
      %{id: ^id, size: size, acc: acc, received: received} when received == size ->
        bytes = IO.iodata_to_binary(acc)
        :ok = channel.handle_octet(socket.assigns.octet.sink_id, id, bytes, socket)
        {:reply, {:ok, %{bytes: received}}, put_upload(socket, nil)}

      %{id: ^id} ->
        {:reply, {:error, %{reason: "upload incomplete"}}, put_upload(socket, nil)}

      _other ->
        {:reply, {:error, %{reason: "no matching upload"}}, socket}
    end
  end

  def handle_in(_channel, _opts, _event, _payload, socket) do
    {:reply, {:error, %{reason: "unknown octet event"}}, socket}
  end

  defp put_upload(socket, upload) do
    assign(socket, :octet, %{socket.assigns.octet | upload: upload})
  end

  defp max_upload_bytes(opts) do
    Keyword.get(opts, :max_upload_bytes, @default_max_upload_bytes)
  end
end

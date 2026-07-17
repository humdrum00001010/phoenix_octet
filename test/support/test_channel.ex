defmodule PhoenixOctet.TestChannel do
  @moduledoc false
  use PhoenixOctet.Channel, max_upload_bytes: 1024

  @impl PhoenixOctet.Channel
  def handle_octet(sink_id, id, bytes, _socket) do
    Phoenix.PubSub.broadcast(
      PhoenixOctet.TestPubSub,
      "octet_test:" <> sink_id,
      {:octet_upload, id, bytes}
    )
  end

  @impl PhoenixOctet.Channel
  def handle_octet_cancelled(sink_id, id, _socket) do
    Phoenix.PubSub.broadcast(
      PhoenixOctet.TestPubSub,
      "octet_test:" <> sink_id,
      {:octet_cancelled, id}
    )
  end
end

defmodule PhoenixOctet.TestSocket do
  @moduledoc false
  use Phoenix.Socket

  channel "octet:*", PhoenixOctet.TestChannel

  @impl true
  def connect(_params, socket, _connect_info), do: {:ok, socket}

  @impl true
  def id(_socket), do: nil
end

defmodule PhoenixOctet.TestEndpoint do
  @moduledoc false
  use Phoenix.Endpoint, otp_app: :phoenix_octet

  socket "/octet", PhoenixOctet.TestSocket, websocket: true, longpoll: false
end

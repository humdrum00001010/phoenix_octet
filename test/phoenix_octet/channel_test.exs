defmodule PhoenixOctet.ChannelTest do
  use ExUnit.Case, async: true

  import Phoenix.ChannelTest

  alias PhoenixOctet.Protocol

  @endpoint PhoenixOctet.TestEndpoint

  setup do
    {:ok, socket} = connect(PhoenixOctet.TestSocket, %{})
    sink_id = Base.url_encode64(:crypto.strong_rand_bytes(24), padding: false)
    {:ok, _reply, socket} = subscribe_and_join(socket, "octet:" <> sink_id, %{})
    :ok = Phoenix.PubSub.subscribe(PhoenixOctet.TestPubSub, "octet_test:" <> sink_id)
    %{socket: socket, sink_id: sink_id}
  end

  test "one frame uploads, acknowledges, and delivers via handle_octet", %{socket: socket} do
    bytes = :crypto.strong_rand_bytes(24)

    ref = push(socket, "upload", {:binary, Protocol.encode_frame("up-1", bytes)})
    assert_reply ref, :ok, %{bytes: 24}

    assert_receive {:octet_upload, "up-1", ^bytes}
  end

  test "enforces the configured max_upload_bytes", %{socket: socket} do
    # TestChannel is configured with max_upload_bytes: 1024
    frame = Protocol.encode_frame("huge", :crypto.strong_rand_bytes(2048))
    ref = push(socket, "upload", {:binary, frame})
    assert_reply ref, :error, %{reason: "upload too large"}

    refute_receive {:octet_upload, _id, _bytes}
  end

  test "refuses malformed frames and unknown events", %{socket: socket} do
    ref = push(socket, "upload", {:binary, <<>>})
    assert_reply ref, :error, %{reason: "invalid octet frame"}

    ref = push(socket, "mystery", %{})
    assert_reply ref, :error, %{reason: "unknown octet event"}
  end

  test "join rejects an empty sink id" do
    {:ok, socket} = connect(PhoenixOctet.TestSocket, %{})
    assert {:error, %{reason: "invalid_sink"}} = subscribe_and_join(socket, "octet:", %{})
  end

  test "frame codec round-trips" do
    assert {:ok, "id", "payload"} = Protocol.decode_frame(Protocol.encode_frame("id", "payload"))
    assert :error = Protocol.decode_frame(<<0, "no-id">>)
  end
end

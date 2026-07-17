defmodule PhoenixOctet.ChannelTest do
  use ExUnit.Case, async: true

  import Phoenix.ChannelTest

  @endpoint PhoenixOctet.TestEndpoint

  setup do
    {:ok, socket} = connect(PhoenixOctet.TestSocket, %{})
    sink_id = Base.url_encode64(:crypto.strong_rand_bytes(24), padding: false)
    {:ok, _reply, socket} = subscribe_and_join(socket, "octet:" <> sink_id, %{})
    :ok = Phoenix.PubSub.subscribe(PhoenixOctet.TestPubSub, "octet_test:" <> sink_id)
    %{socket: socket, sink_id: sink_id}
  end

  test "streams begin -> chunks -> commit and delivers via handle_octet", %{socket: socket} do
    part_one = :crypto.strong_rand_bytes(16)
    part_two = :crypto.strong_rand_bytes(8)
    size = byte_size(part_one) + byte_size(part_two)

    ref = push(socket, "begin", %{"id" => "up-1", "size" => size})
    assert_reply ref, :ok

    # stop-and-wait: every chunk is individually acknowledged (the credit)
    ref = push(socket, "chunk", {:binary, part_one})
    assert_reply ref, :ok
    ref = push(socket, "chunk", {:binary, part_two})
    assert_reply ref, :ok

    ref = push(socket, "commit", %{"id" => "up-1"})
    assert_reply ref, :ok, %{bytes: ^size}

    expected = part_one <> part_two
    assert_receive {:octet_upload, "up-1", ^expected}
  end

  test "rejects a commit whose received bytes miss the declared size", %{socket: socket} do
    ref = push(socket, "begin", %{"id" => "up-short", "size" => 100})
    assert_reply ref, :ok

    ref = push(socket, "chunk", {:binary, :crypto.strong_rand_bytes(10)})
    assert_reply ref, :ok

    ref = push(socket, "commit", %{"id" => "up-short"})
    assert_reply ref, :error, %{reason: "upload incomplete"}

    refute_receive {:octet_upload, _id, _bytes}
  end

  test "rejects chunks beyond the declared size and forgets the transfer", %{socket: socket} do
    ref = push(socket, "begin", %{"id" => "up-over", "size" => 4})
    assert_reply ref, :ok

    ref = push(socket, "chunk", {:binary, :crypto.strong_rand_bytes(5)})
    assert_reply ref, :error, %{reason: "upload exceeded declared size"}

    ref = push(socket, "begin", %{"id" => "up-retry", "size" => 1})
    assert_reply ref, :ok
  end

  test "enforces the configured max_upload_bytes and single transfer", %{socket: socket} do
    # TestChannel is configured with max_upload_bytes: 1024
    ref = push(socket, "begin", %{"id" => "huge", "size" => 2048})
    assert_reply ref, :error, %{reason: "upload too large"}

    ref = push(socket, "begin", %{"id" => "a", "size" => 1})
    assert_reply ref, :ok

    ref = push(socket, "begin", %{"id" => "b", "size" => 1})
    assert_reply ref, :error, %{reason: "upload already in progress"}
  end

  test "abort forgets the in-flight transfer and is idempotent", %{socket: socket} do
    ref = push(socket, "begin", %{"id" => "up-abort", "size" => 10})
    assert_reply ref, :ok

    ref = push(socket, "abort", %{"id" => "up-abort"})
    assert_reply ref, :ok

    ref = push(socket, "abort", %{"id" => "up-abort"})
    assert_reply ref, :ok

    ref = push(socket, "commit", %{"id" => "up-abort"})
    assert_reply ref, :error, %{reason: "no matching upload"}
  end

  test "abort orders a terminal cancellation after an already committed upload", %{
    socket: socket
  } do
    id = "up-commit-abort-race"
    bytes = :crypto.strong_rand_bytes(12)

    ref = push(socket, "begin", %{"id" => id, "size" => byte_size(bytes)})
    assert_reply ref, :ok
    ref = push(socket, "chunk", {:binary, bytes})
    assert_reply ref, :ok
    ref = push(socket, "commit", %{"id" => id})
    assert_reply ref, :ok, %{bytes: 12}
    ref = push(socket, "abort", %{"id" => id})
    assert_reply ref, :ok

    assert_receive {:octet_upload, ^id, ^bytes}
    assert_receive {:octet_cancelled, ^id}
  end

  test "chunks without a begin and unknown events are refused", %{socket: socket} do
    ref = push(socket, "chunk", {:binary, "stray"})
    assert_reply ref, :error, %{reason: "no upload in progress"}

    ref = push(socket, "mystery", %{})
    assert_reply ref, :error, %{reason: "unknown octet event"}
  end

  test "join rejects an empty sink id" do
    {:ok, socket} = connect(PhoenixOctet.TestSocket, %{})
    assert {:error, %{reason: "invalid_sink"}} = subscribe_and_join(socket, "octet:", %{})
  end
end

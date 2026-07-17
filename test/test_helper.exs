{:ok, _} = Supervisor.start_link(
  [
    {Phoenix.PubSub, name: PhoenixOctet.TestPubSub},
    PhoenixOctet.TestEndpoint
  ],
  strategy: :one_for_one
)

ExUnit.start()

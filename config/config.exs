import Config

if config_env() == :test do
  config :phoenix_octet, PhoenixOctet.TestEndpoint,
    secret_key_base: String.duplicate("octet", 13),
    server: false,
    pubsub_server: PhoenixOctet.TestPubSub

  config :logger, level: :warning
end

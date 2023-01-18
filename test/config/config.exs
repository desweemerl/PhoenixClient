use Mix.Config

config :phoenix, :json_library, Jason

config :phoenix_client_test, PhoenixClientTestWeb.Endpoint,
  http: [
    port: 4000,
    otp_app: :phoenix_client_test,
  ],
  url: [
    host: "127.0.0.1"
  ],
  secret_key_base: "3VDjdGVMuf/5tgzzlWRSK4y2MOAZ6r7enuWExM9p3eiN2Sv34gojG+kdfOt7/Jq8",
  pubsub_server: PhoenixClientTest.PubSub
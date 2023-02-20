defmodule PhoenixClientTestWeb.Endpoint do
  use Phoenix.Endpoint, otp_app: :phoenix_client_test

  socket "/socket", PhoenixClientTestWeb.Socket, websocket: [connect_info: [:x_headers]], longpoll: false
end
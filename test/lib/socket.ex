defmodule PhoenixClientTestWeb.Socket do
  use Phoenix.Socket

  channel "test:*", PhoenixClientTestWeb.Channel

  def connect(%{"token" => "user1234"}, socket) do
    {:ok, assign(socket, :user_id, "user1234")}
  end

  def connect(_params, _socket) do
    :error
  end

  def id(socket), do: "socket:#{socket.assigns.user_id}"
end
defmodule PhoenixClientTestWeb.Channel do
  use Phoenix.Channel

  def join("test:" <> id, _payload, socket), do: {:ok, socket}

  def handle_in("hello", %{"name" => name}, socket) do
    {:reply, {:ok, %{message: "hello #{name}"}}, socket}
  end
end
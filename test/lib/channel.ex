defmodule PhoenixClientTestWeb.Channel do
  use Phoenix.Channel

  def join("test:" <> id, _payload, socket), do: {:ok, socket}

  def handle_in("close_socket", _payload, socket) do
    PhoenixClientTestWeb.Endpoint.broadcast("socket:#{socket.assigns.user_id}", "disconnect", %{})
    {:noreply, socket}
  end

  def handle_in("crash_channel", _payload, socket) do
    raise "crash channel"
    {:noreply, socket}
  end

  def handle_in("hello", %{"name" => name}, socket) do
    {:reply, {:ok, %{message: "hello #{name}"}}, socket}
  end
end
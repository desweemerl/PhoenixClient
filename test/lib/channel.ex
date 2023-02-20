defmodule PhoenixClientTestWeb.Channel do
  use Phoenix.Channel

  def join("test:" <> id, _payload, socket), do: {:ok, socket}

  def handle_in("close_socket", _payload, socket) do
    PhoenixClientTestWeb.Endpoint.broadcast("socket:#{socket.assigns.user_id}", "disconnect", %{})
    {:noreply, socket}
  end

  def handle_in("get_headers", _payload, socket) do
    headers = %{
      header1: Map.get(socket.assigns, :header1),
      header2: Map.get(socket.assigns, :header2),
    }
    {:reply, {:ok, headers}, socket}
  end

  def handle_in("crash_channel", _payload, socket) do
    raise "crash channel"
    {:noreply, socket}
  end

  def handle_in("wrong_request", _payload, socket) do
    :ok = :error
    {:reply, {:ok, "please don't retry"}, socket}
  end

  def handle_in("hello", %{"name" => name}, socket) do
    {:reply, {:ok, %{message: "hello #{name}"}}, socket}
  end

  defp build_reply_object(prefix \\ "") do
    %{
      value_string: "test1234#{prefix}",
      value_number: -1234.5678,
      value_boolean: true,
    }
  end

  def handle_in("deserialize_object", payload, socket) do
    {:reply, {:ok, build_reply_object()}, socket}
  end

  def handle_in("deserialize_list_failed", payload, socket) do
    response = ["_1", "_2"] |> Enum.map(&build_reply_object/1) |> IO.inspect

    {:reply, {:ok, response}, socket}
  end

  def handle_in("deserialize_list", payload, socket) do
    response = ["_1", "_2"] |> Enum.map(&build_reply_object/1) |> IO.inspect

    {:reply, {:ok, %{list: response}}, socket}
  end

  def handle_in("deserialize_event", _payload, socket) do
    push(socket, "test_event", build_reply_object())

    {:noreply, socket}
  end
end

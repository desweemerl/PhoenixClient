defmodule PhoenixClientTestWeb.Socket do
  use Phoenix.Socket

  channel "test:*", PhoenixClientTestWeb.Channel

  defp get_header(headers, name, default \\ nil) do
    headers
      |> Enum.filter(fn {n, _v} -> n == name end)
      |> Enum.map(fn {_n, v} -> v end)
      |> List.first()
  end

  def connect(%{"token" => "user1234"}, socket, %{"x_headers": headers}) do
    {
      :ok,
      assign(socket,
        user_id: "user1234",
        header1: get_header(headers, "x-header1"),
        header2: get_header(headers, "x-header2"),
      )
    }
  end

  def connect(_params, _socket, _connect_info) do
    :error
  end

  def id(socket), do: "socket:#{socket.assigns.user_id}"
end

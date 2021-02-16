
defmodule Acceptor do
  defstruct ballot_num: { nil, nil, nil }, accepted: MapSet.new()

  def start(config) do
    data = %Acceptor{}
    next(data, config)
  end

  defp next(data, config) do
    receive do
      {:p1a, lambda, b} ->
        new_ballot_num = if b > data.ballot_num do b else data.ballot_num end
        new_data = %{ data | ballot_num: new_ballot_num }
        send lambda, {:p1b, self(), new_ballot_num, data.accepted}
        next(new_data, config)

      {:p2a, lambda, {b, s, c}} ->
        new_accepted =
          case b == data.ballot_num do
            true -> MapSet.put(data.accepted, {b, s, c})
            false -> data.accepted
          end

        send lambda, {:p2b, self(), data.ballot_num}
        new_data = %{ data | accepted: new_accepted }
        next(new_data, config)
    end
  end

end

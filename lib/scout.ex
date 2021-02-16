
defmodule Scout do
  defstruct lambda: nil, acceptors: [], waitfor: MapSet.new(), b: nil, pvalues: MapSet.new(), id: 0

  def start(lambda, acceptors, b, id, config) do
    send config.monitor, {:SCOUT_SPAWNED, id}
    data = %Scout{lambda: lambda, acceptors: acceptors, waitfor: MapSet.new(acceptors), b: b, id: id}

    for alpha <- acceptors, do: send alpha, {:p1a, self(), b}

    next(data, config)
  end

  def next(data, config) do
    receive do
      {:p1b, alpha, b, accepted} ->
        if b == data.b and MapSet.member?(data.waitfor, alpha) do
          pvalues = MapSet.union(data.pvalues, accepted)
          waitfor = MapSet.delete(data.waitfor, alpha)

          if MapSet.size(waitfor) < (length(data.acceptors) / 2) do
            send data.lambda, {:adopted, b, pvalues}
            send config.monitor, { :SCOUT_FINISHED, data.id }
          else
            next(%{ data | waitfor: waitfor, pvalues: pvalues }, config)
          end

        else
          send data.lambda, {:preempted, b}
          send config.monitor, { :SCOUT_FINISHED, data.id }
        end
    end
  end

end


defmodule Commander do
  defstruct lambda: nil, acceptors: [], replicas: [], waitfor: MapSet.new(), b: nil, s: nil, c: nil, id: 0

  def start(lambda, acceptors, replicas, {b, s, c}, id, config) do
    send config.monitor, { :COMMANDER_SPAWNED, id }

    data = %Commander{lambda: lambda, acceptors: acceptors, waitfor: MapSet.new(acceptors), replicas: replicas, b: b, s: s, c: c, id: id}

    for alpha <- acceptors, do: send alpha, {:p2a, self(), {b, s, c}}

    next(data, config)
  end

  defp next(data, config) do
    receive do
      {:p2b, alpha, b_prime} ->
        if b_prime == data.b  do
          new_waitfor = MapSet.delete(data.waitfor, alpha)
          if MapSet.size(new_waitfor) < (length(data.acceptors) / 2) do
            for rho <- data.replicas, do: send rho, {:decision, data.s, data.c}
            send config.monitor, { :COMMANDER_FINISHED, data.id }
          else
            next(%{ data | waitfor: new_waitfor }, config)
          end

        else
          send data.lambda, {:preempted, b_prime}
          send config.monitor, { :COMMANDER_FINISHED, data.id }
        end
    end

  end

end

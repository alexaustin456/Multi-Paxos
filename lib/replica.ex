
defmodule Replica do
  defstruct leaders: [], state: nil, slot_in: 1, slot_out: 1, requests: [], proposals: Map.new(), decisions: Map.new(), database: nil, id: 0

  def start(config, database, id) do
    receive do
      {:BIND, leaders, initial_state} ->
        data = %Replica{leaders: leaders, state: initial_state, database: database, id: id}
        next(data, 5, config)
    end
  end

  defp next(data, window, config) do
    receive do
      {:CLIENT_REQUEST, c} ->
        send config.monitor, { :CLIENT_REQUEST, data.id }
        %{ data | requests: [ c | data.requests] }
        next(propose(data, window, config), window, config)

      {:decision, s, c} ->
        decisions = Map.put(data.decisions, s, c)
        new_data = %{data | decisions: decisions}
        updated_data = next2(new_data, window, config)
        next(propose(updated_data, window, config), window, config)
    end

  end

  defp next2(data, window, config) do
    command_dec = Map.get(data.decisions, data.slot_out, nil)
    if command_dec !== nil do
      command_prop = Map.get(data.proposals, data.slot_out, nil)
      new_data = if command_prop !== nil do
                    proposals = Map.delete(data.proposals, data.slot_out)
                    %{ data | proposals: proposals, requests: if !compareCommands(command_dec, command_prop) do data.requests else data.requests ++ [command_prop] end }
                 else
                    data
                 end
      {k, cid, op} = command_dec
      next2(perform(new_data, window, config, k, cid, op), window, config)
    else
      data
    end
  end

  # returns new data struct
  defp propose(data, window, config) do
    if data.slot_in < data.slot_out + window and length(data.requests) !== 0 do
      # ignored if statement - as said on Piazza can assume never a reconfig command

      command = Map.get(data.decisions, data.slot_in, nil)

      new_data = if command == nil do
                  {c, requests} = List.pop_at(data.requests, 0)

                  proposals = Map.put(data.proposals, data.slot_in, c)
                  send config.monitor, {:PROPOSAL_MADE, data.id}

                  Enum.each(data.leaders, fn(lambda) ->
                    send lambda, {:propose, data.slot_in, c}
                  end)

                  %{ data | proposals: proposals, requests: requests }
                 else
                  data
                 end
      propose(%{ new_data | slot_in: new_data.slot_in + 1 }, window, config)
    else
      data
    end
  end

  # returns updated data struct
  defp perform(data, _window, _config, k, cid, op) do
    # ignored reconfig op as cannot be used in our coursework
    if find_in_decisions(data.slot_out, k, cid, op, data.decisions) do
      %{ data | slot_out: data.slot_out + 1 }
    else
      # IO.puts "Replica_#{data.id} is performing slot number #{data.slot_in} and #{data.slot_out} with command #{inspect({k, cid, op})}"
      {new_data, result} = perform_op(data, op)
      send k, {:CLIENT_REPLY, cid, result}
      %{ new_data | slot_out: new_data.slot_out+1 }
    end
  end

  defp find_in_decisions(slot_out, k, cid, op, decisions) do
    find_in_dec(slot_out, k, cid, op, decisions, 1)
  end

  defp find_in_dec(slot_out, k, cid, op, decisions, s) do
    if s < slot_out do
      val = Map.get(decisions, s, nil)
      if val == nil do
        find_in_dec(slot_out, k, cid, op, decisions, s+1)
      else
        {k_prime, cid_prime, op_prime} = val
        if k === k_prime and cid === cid_prime and op === op_prime do
          true
        else
          find_in_dec(slot_out, k, cid, op, decisions, s+1)
        end
      end
    else
      false
    end
  end

  defp perform_op(data, transaction) do
    send data.database, {:EXECUTE, transaction}
    {data, true}
  end

  defp compareCommands({k1, cid1, { :MOVE, amount1, account1a, account1b }}, {k2, cid2, { :MOVE, amount2, account2a, account2b }}) do
    k1 == k2 and cid1 == cid2 and amount1 == amount2 and account1a == account2a and account1b == account2b
  end

end

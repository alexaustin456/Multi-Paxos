
defmodule Leader do
  defstruct acceptors: [], replicas: [], ballot_num: nil, active: false, proposals: Map.new(), id: 0, timeout: 50

  def start(config, id) do
    receive do
      {:BIND, acceptors, replicas} ->
        data = %Leader{acceptors: acceptors, replicas: replicas, ballot_num: {0, id, self()}, id: id}

        spawn(Scout, :start, [self(), acceptors, data.ballot_num, id, config])

        next(data, config)
    end
  end

  defp next(data, config) do
    #IO.puts "BALLOT NUMBER: #{inspect(data.ballot_num)} but my id is: #{data.id}"
    receive do
      {:propose, s, c} ->
        #send config.monitor, {:PROPOSAL_RECIEVED, data.id}
        if !Map.has_key?(data.proposals, s) do
          proposals = Map.put(data.proposals, s, c)
          if data.active do
            spawn(Commander, :start, [self(), data.acceptors, data.replicas, {data.ballot_num, s, c}, data.id, config])
          end
          next(%{ data | proposals: proposals }, config)
        else
          next(data, config)
        end

      {:adopted, ballot_num, accepted} ->
          new_proposals = update(data.proposals, pmax(MapSet.to_list(accepted)))
          for {s, c} <- Map.to_list(new_proposals) do
            spawn(Commander, :start, [self(), data.acceptors, data.replicas, {ballot_num, s, c}, data.id, config])
          end
          next(%{ data | active: true, proposals: new_proposals, timeout: data.timeout - 1, ballot_num: ballot_num }, config)

      {:preempted, b} ->
        if b > data.ballot_num do
          send config.monitor, {:PREEMPTION, data.id}

          timeout = data.timeout + 1
          new_data = %{ data | timeout: timeout}

          {rnd, id, lambda} = b
          {_, _, _} = ping_pong(lambda, id, new_data.timeout, new_data.ballot_num, config)


          ballot_num = {rnd + 1, new_data.id, self()}
          spawn(Scout, :start, [self(), data.acceptors, ballot_num, data.id, config])
          next(%{ new_data | active: false, ballot_num: ballot_num, timeout: data.timeout + 1 }, config)
        else
          next(data, config)
        end

      {:ping, lambda} ->
        send lambda, {:pong, data.ballot_num}
        next(data, config)
    end
  end

  def ping_pong(lambda, id, timeout, ballot_num, config) do
    send lambda, {:ping, self()}
    # IO.puts "SENDING PING FROM #{data.id} TO #{id}"
    receive do
      {:pong, b} ->
        # IO.puts "HEARTBEAT from #{id} to #{data.id}"
        ping_pong(lambda, id, max(0, timeout-1), b, config)
      after timeout ->
        # IO.puts "SLOW RESPONSE #{id} in #{data.id}"
        ballot_num
    end
  end


  # defp update(max, proposals, accepted) do
  #   # pmax_accepted = pmax(accepted)
  #   update_helper(max, proposals, MapSet.to_list(accepted))
  # end



  # defp update_helper(_, proposals, []), do: proposals
  # defp update_helper(max, proposals, [key | keys]) do
  #   {b, slot_number, c} = key
  #   bn = Map.get(max, slot_number, nil)
  #   if bn == nil or compareBallots(bn, b) == -1 do
  #     new_max = Map.put(max, slot_number, b)
  #     new_proposals = Map.put(proposals, slot_number, c)
  #     update_helper(new_max, new_proposals, keys)
  #   else
  #     update_helper(max, proposals, keys)
  #   end
  # end

  defp pmax pvalues do
    max_ballot_number =
      Enum.map(pvalues, fn ({ b, _, _ }) -> b end)
      |> Enum.max(fn -> -1 end)

    max = Enum.filter(pvalues, fn ({ b, _, _ }) -> b == max_ballot_number end)
    |> Enum.map(fn ({ _, s, c }) -> { s, c } end)
    |> Map.new

    max
  end

  defp update x, y do
    Map.merge y, x, fn _, c, _ -> c end
  end

end

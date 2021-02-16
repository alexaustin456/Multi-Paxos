
# distributed algorithms, n.dulay 29 jan 2021
# coursework, paxos made moderately complex

defmodule Monitor do

# setters for Monitor state variables
def clock(state, v), do:
  Map.put(state, :clock, v)

def requests(state, i, v), do:
  Map.put(state, :requests, Map.put(state.requests, i, v))

def updates(state, i, v), do:
  Map.put(state, :updates,  Map.put(state.updates, i, v))

def transactions(state, v), do:
  Map.put(state, :transactions, v)

def commanders_spawned(state, i, v), do:
  Map.put(state, :commanders_spawned,  Map.put(state.commanders_spawned, i, v))

def commanders_finished(state, i, v), do:
  Map.put(state, :commanders_finished,  Map.put(state.commanders_finished, i, v))

def scouts_spawned(state, i, v), do:
  Map.put(state, :scouts_spawned, Map.put(state.scouts_spawned, i, v))

def scouts_finished(state, i, v), do:
  Map.put(state, :scouts_finished, Map.put(state.scouts_finished, i, v))

def proposal_made(state, i, v), do:
  Map.put(state, :proposals, Map.put(state.proposals, i, v))

def proposal_recieved(state, i, v), do:
  Map.put(state, :proposals_recieved, Map.put(state.proposals, i, v))

def preemption_happened(state, i, v), do:
  Map.put(state, :preemptions, Map.put(state.preemptions, i, v))

def adoptions(state, i, v), do:
  Map.put(state, :adoptions, Map.put(state.adoptions, i, v))

def client_replies(state, i, v), do:
  Map.put(state, :client_replies, Map.put(state.client_replies, i, v))

def start_print_timeout(duration), do:
  Process.send_after(self(), { :PRINT }, duration)

def start(config) do
  state = %{
    clock:               0,
    requests:            Map.new,
    updates:             Map.new,
    transactions:        Map.new,
    scouts_spawned:      Map.new,
    scouts_finished:     Map.new,
    commanders_spawned:  Map.new,
    commanders_finished: Map.new,
    # our new data
    proposals:           Map.new,
    proposals_recieved:  Map.new,
    preemptions:         Map.new,
    adoptions:           Map.new,
    client_replies:      Map.new,
  }
  Monitor.start_print_timeout(config.print_after)
  Monitor.next(config, state)
end # start

def next(config, state) do
  receive do
  { :DB_UPDATE, db, seqnum, transaction } ->
    { :MOVE, amount, from, to } = transaction
    done = Map.get(state.updates, db, 0)

    if seqnum != done + 1, do:
      Util.halt "  ** error db #{db}: seq #{seqnum} expecting #{done+1}"

    transactions =
      case Map.get(state.transactions, seqnum) do
      nil -> # IO.puts "db #{db} seq #{seqnum} = #{done+1}"
        Map.put(state.transactions, seqnum, %{amount: amount, from: from, to: to})
        #IO.inspect(state.transactions)

      t ->  #already logged - check transaction
        if amount != t.amount or from != t.from or to != t.to do
  	      Util.halt " ** error db #{db}.#{done} [#{amount},#{from},#{to}] "
             <>
          "= log #{done}/#{map_size(state.transactions)} [#{t.amount},#{t.from},#{t.to}]"
        end
        state.transactions
      end # case

    state = Monitor.transactions(state, transactions)
    state = Monitor.updates(state, db, seqnum)
    Monitor.next(config, state)

  { :CLIENT_REQUEST, server_num } ->  # client requests seen by replicas
    value = Map.get(state.requests, server_num, 0)
    state = Monitor.requests(state, server_num, value + 1)
    Monitor.next(config, state)

  { :SCOUT_SPAWNED, server_num } ->
    value = Map.get(state.scouts_spawned, server_num, 0)
    state = Monitor.scouts_spawned(state, server_num, value + 1)
    Monitor.next(config, state)

  { :SCOUT_FINISHED, server_num } ->
    value = Map.get(state.scouts_finished, server_num, 0)
    state = Monitor.scouts_finished(state, server_num, value + 1)
    Monitor.next(config, state)

  { :COMMANDER_SPAWNED, server_num } ->
    value = Map.get(state.commanders_spawned, server_num, 0)
    state = Monitor.commanders_spawned(state, server_num, value + 1)
    Monitor.next(config, state)

  { :COMMANDER_FINISHED, server_num } ->
    value = Map.get(state.commanders_finished, server_num, 0)
    state = Monitor.commanders_finished(state, server_num, value + 1)
    Monitor.next(config, state)

  { :PRINT } ->
    clock  = state.clock + config.print_after
    state  = Monitor.clock(state, clock)

    sorted = state.updates  |> Map.to_list |> List.keysort(0)
    IO.puts "time = #{clock}      db updates done = #{inspect sorted}"
    sorted = state.requests |> Map.to_list |> List.keysort(0)
    IO.puts "time = #{clock} client requests seen = #{inspect sorted}"

    if config.debug_level == 0 do
      # min_done   = state.updates  |> Map.values |> Enum.min(fn -> 0 end)
      # n_requests = state.requests |> Map.values |> Enum.sum
      # IO.puts "time = #{clock}           total seen = #{n_requests} max lag = #{n_requests-min_done}"

      sorted = state.proposals |> Map.to_list |> List.keysort(0)
      IO.puts "time = #{clock}            proposals = #{inspect sorted}"
      sorted = state.proposals_recieved |> Map.to_list |> List.keysort(0)
      IO.puts "time = #{clock}   proposals_recieved = #{inspect sorted}"

      sorted = state.preemptions |> Map.to_list |> List.keysort(0)
      IO.puts "time = #{clock}          preemptions = #{inspect sorted}"

      # sorted = state.client_replies |> Map.to_list |> List.keysort(0)
      # IO.puts "time = #{clock}       client_replies = #{inspect sorted}"

      sorted = state.scouts_spawned |> Map.to_list |> List.keysort(0)
      IO.puts "time = #{clock}            scouts up = #{inspect sorted}"
      sorted = state.scouts_finished |> Map.to_list |> List.keysort(0)
      IO.puts "time = #{clock}          scouts down = #{inspect sorted}"

      sorted = state.commanders_spawned |> Map.to_list |> List.keysort(0)
      IO.puts "time = #{clock}        commanders up = #{inspect sorted}"
      sorted = state.commanders_finished |> Map.to_list |> List.keysort(0)
      IO.puts "time = #{clock}      commanders down = #{inspect sorted}"
    end

    IO.puts ""
    Monitor.start_print_timeout(config.print_after)
    Monitor.next(config, state)

  # ** ADD ADDITIONAL MONITORING MESSAGES OF YOUR OWN HERE

  { :PROPOSAL_MADE, server_num } ->
    value = Map.get(state.proposals, server_num, 0)
    state = Monitor.proposal_made(state, server_num, value + 1)
    Monitor.next(config, state)

  { :PROPOSAL_RECIEVED, server_num } ->
    value = Map.get(state.proposals_recieved, server_num, 0)
    state = Monitor.proposal_recieved(state, server_num, value + 1)
    Monitor.next(config, state)

  { :PREEMPTION, server_num } ->
    # IO.puts "Ballot being preempted on: #{inspect(b)}"
    value = Map.get(state.preemptions, server_num, 0)
    state = Monitor.preemption_happened(state, server_num, value + 1)
    Monitor.next(config, state)

  { :ADOPTION, server_num } ->
    value = Map.get(state.adoptions, server_num, 0)
    state = Monitor.adoptions(state, server_num, value + 1)
    Monitor.next(config, state)

  { :CLIENT_REPLY, server_num } ->
    value = Map.get(state.client_replies, server_num, 0)
    state = Monitor.client_replies(state, server_num, value + 1)
    Monitor.next(config, state)

  unexpected ->
    Util.halt "monitor: unexpected message #{inspect unexpected}"
  end # receive
end # next

end # Monitor

use "collections"
use "random"
use "time"

trait Actor
  be add_neighbor(neighbor: Actor tag)
  be receive_rumor()
  be receive_pair(s: F64, w: F64)
  be simulate_failure(failure_prob: F64)
  be report_convergence(main: Main tag)
  fun print(msg: String)

actor GossipActor is Actor
  let _id: USize
  let _neighbors: Array[Actor tag] = Array[Actor tag]
  var _rumor_count: USize = 0
  var _active: Bool = true
  var _failed: Bool = false
  var _main: (Main tag | None) = None
  let _env: Env

  new create(id: USize, main: Main tag, env: Env) =>
    _id = id
    _main = main
    _env = env

  be add_neighbor(neighbor: Actor tag) =>
    _neighbors.push(neighbor)

  be start_gossip() =>
    receive_rumor()

  be receive_rumor() =>
    if not _failed then
      _rumor_count = _rumor_count + 1
      print("Actor " + _id.string() + " received rumor. Count: " + _rumor_count.string())
      if _rumor_count >= 10 then
        _active = false
        try (_main as Main tag).report_convergence() end
      elseif _active and (_neighbors.size() > 0) then
        let rand = Rand(Time.nanos().u64())
        try
          let neighbor = _neighbors(rand.int(_neighbors.size().u64()).usize())?
          neighbor.receive_rumor()
        end
      end
    end

  be receive_pair(s: F64, w: F64) =>
    None // Do nothing for GossipActor

  be simulate_failure(failure_prob: F64) =>
    let rand = Rand(Time.nanos().u64())
    if rand.real() < failure_prob then
      _failed = true
    end

  be report_convergence(main: Main tag) =>
    if not _active and not _failed then
      main.report_convergence()
    end

  fun print(msg: String) =>
    _env.out.print(msg)

actor PushSumActor is Actor
  let _id: USize
  let _neighbors: Array[Actor tag] = Array[Actor tag]
  var _s: F64
  var _w: F64 = 1.0
  var _ratio: F64
  var _unchanged_count: USize = 0
  var _active: Bool = true
  var _failed: Bool = false
  var _main: (Main tag | None) = None
  let _env: Env

  new create(id: USize, main: Main tag, env: Env) =>
    _id = id
    _s = id.f64()
    _ratio = _s / _w
    _main = main
    _env = env

  be add_neighbor(neighbor: Actor tag) =>
    _neighbors.push(neighbor)

  be start_push_sum() =>
    send_pair()

  be receive_rumor() =>
    None // Do nothing for PushSumActor

  be receive_pair(s_received: F64, w_received: F64) =>
    if not _failed then
      print("Actor " + _id.string() + " received pair: s=" + s_received.string() + ", w=" + w_received.string())
      let old_ratio = _ratio
      _s = _s + s_received
      _w = _w + w_received
      _ratio = _s / _w

      if (_ratio - old_ratio).abs() < 1e-10 then
        _unchanged_count = _unchanged_count + 1
      else
        _unchanged_count = 0
      end

      if _unchanged_count >= 3 then
        _active = false
        try (_main as Main tag).report_convergence() end
      elseif _active then
        send_pair()
      end
    end

  fun ref send_pair() =>
    if _neighbors.size() > 0 then
      let rand = Rand(Time.nanos().u64())
      try
        let neighbor = _neighbors(rand.int(_neighbors.size().u64()).usize())?
        _s = _s / 2
        _w = _w / 2
        neighbor.receive_pair(_s, _w)
      end
    end

  be simulate_failure(failure_prob: F64) =>
    let rand = Rand(Time.nanos().u64())
    if rand.real() < failure_prob then
      _failed = true
    end

  be report_convergence(main: Main tag) =>
    if not _active and not _failed then
      main.report_convergence()
    end

  fun print(msg: String) =>
    _env.out.print(msg)

actor Main
  let env: Env
  let nodes: Array[Actor tag]
  let start_time: U64
  var converged_count: USize
  var total_nodes: USize
  var failure_prob: F64
  let final_ratios: Array[F64] = Array[F64]
  let _timers: Timers = Timers

  new create(env': Env) =>
    env = env'
    nodes = Array[Actor tag]
    start_time = Time.nanos()
    converged_count = 0
    total_nodes = 0
    failure_prob = 0.0

    env.out.print("Starting program")

    if env.args.size() != 5 then
      env.out.print("Usage: project2 numNodes topology algorithm failureProb")
      return
    end

    let num_nodes = try env.args(1)?.usize()? else 10 end
    let topology = try env.args(2)? else "full" end
    let algorithm = try env.args(3)? else "gossip" end
    failure_prob = try env.args(4)?.f64()? else 0.0 end

    total_nodes = num_nodes

    // Create nodes
    for i in Range(0, num_nodes) do
      if algorithm == "gossip" then
        nodes.push(GossipActor(i, this, env))
      else
        nodes.push(PushSumActor(i, this, env))
      end
    end

    // Build topology
    match topology
    | "full" => build_full_network(nodes)
    | "3D" => build_3d_grid(nodes)
    | "line" => build_line(nodes)
    | "imp3D" => build_imperfect_3d_grid(nodes)
    else
      env.out.print("Invalid topology")
      return
    end

    // Simulate failures
    for node in nodes.values() do
      node.simulate_failure(failure_prob)
    end

    // Start algorithm
    let rand = Rand(Time.nanos().u64())
    let starter: USize = rand.int(num_nodes.u64()).usize()

    match algorithm
    | "gossip" =>
      try 
        (nodes(starter)? as GossipActor).start_gossip()
      end
    | "push-sum" =>
      try 
        (nodes(starter)? as PushSumActor).start_push_sum()
      end
    else
      env.out.print("Invalid algorithm")
      return
    end

    env.out.print("Finished setup, starting algorithm")

    // Set a timeout
    let timeout_timer = Timer(TimeoutNotify(this), 60_000_000_000) // 60 seconds timeout
    _timers(consume timeout_timer)

  be report_convergence() =>
    converged_count = converged_count + 1
    env.out.print("Node converged. Total: " + converged_count.string() + "/" + total_nodes.string())
    let active_nodes = nodes.size() - (failure_prob * nodes.size().f64()).usize()
    if converged_count == active_nodes then
      let end_time = Time.nanos()
      let convergence_time = end_time - start_time
      env.out.print("Convergence time: " + (convergence_time.f64() / 1_000_000.0).string() + " milliseconds")
      env.out.print("Failure probability: " + failure_prob.string())
      env.out.print("Active nodes: " + active_nodes.string() + "/" + nodes.size().string())
      print_final_ratios()
    end

  be timeout() =>
    env.out.print("Timeout reached. Program terminating.")
    env.out.print("Converged nodes: " + converged_count.string() + "/" + total_nodes.string())

  fun print_final_ratios() =>
    env.out.print("Final ratios:")
    var sum: F64 = 0.0
    for ratio in final_ratios.values() do
      env.out.print(ratio.string())
      sum = sum + ratio
    end
    let avg_ratio = if final_ratios.size() > 0 then
      sum / final_ratios.size().f64()
    else
      0.0
    end
    env.out.print("Average ratio: " + avg_ratio.string())

  fun build_full_network(network: Array[Actor tag]) =>
    for i in Range(0, network.size()) do
      try
        let node = network(i)?
        for j in Range(0, network.size()) do
          if i != j then
            try node.add_neighbor(network(j)?) end
          end
        end
      end
    end

  fun build_3d_grid(network: Array[Actor tag]) =>
    let size = (network.size().f64().pow(1/3).ceil()).usize()
    for i in Range(0, network.size()) do
      try
        let node = network(i)?
        let x = i % size
        let y = (i / size) % size
        let z = i / (size * size)
        if x > 0 then node.add_neighbor(network(i-1)?) end
        if x < (size-1) then node.add_neighbor(network(i+1)?) end
        if y > 0 then node.add_neighbor(network(i-size)?) end
        if y < (size-1) then node.add_neighbor(network(i+size)?) end
        if z > 0 then node.add_neighbor(network(i-(size*size))?) end
        if z < (size-1) then node.add_neighbor(network(i+(size*size))?) end
      end
    end

  fun build_line(network: Array[Actor tag]) =>
    for i in Range(0, network.size()) do
      try
        let node = network(i)?
        if i > 0 then node.add_neighbor(network(i-1)?) end
        if i < (network.size()-1) then node.add_neighbor(network(i+1)?) end
      end
    end

  fun build_imperfect_3d_grid(network: Array[Actor tag]) =>
    build_3d_grid(network)
    let rand = Rand(Time.nanos().u64())
    for i in Range(0, network.size()) do
      try
        let node = network(i)?
        let random_neighbor = rand.int(network.size().u64()).usize()
        if random_neighbor != i then
          node.add_neighbor(network(random_neighbor)?)
        end
      end
    end

class TimeoutNotify is TimerNotify
  let _main: Main

  new iso create(main: Main) =>
    _main = main

  fun ref apply(timer: Timer, count: U64): Bool =>
    _main.timeout()
    false
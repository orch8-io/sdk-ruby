# frozen_string_literal: true

require_relative "test_helper"
require_relative "support/fake_engine"

class WorkerTest < Minitest::Test
  def setup
    @engine = FakeEngine.new
    @workers = []
  end

  def teardown
    @workers.each { |w| w.stop(timeout: 1) }
    @engine.close
  end

  def worker(**opts)
    w = Orch8::Worker.new(base_url: @engine.base_url, api_key: "k", tenant_id: "t", worker_id: "w-test",
                          concurrency: 4, poll_interval: 0.02, logger: NullLogger.new, **opts)
    @workers << w
    w
  end

  def test_echo_completes_with_claim_epoch_and_worker_id
    task = @engine.add_task("echo", params: { "n" => 7 }, claim_epoch: 5)
    w = worker
    w.register("echo") { |t| { "echo" => t.params, "attempt" => t.attempt, "epoch" => t.claim_epoch } }
    w.start
    @engine.wait_for { @engine.acks(task["id"]).any? }
    ack = @engine.acks(task["id"]).first
    assert_equal "/workers/tasks/#{task['id']}/complete", ack.path
    assert_equal({ "worker_id" => "w-test", "claim_epoch" => 6,
                   "output" => { "echo" => { "n" => 7 }, "attempt" => 0, "epoch" => 6 } }, ack.body)
    poll = @engine.requests(:poll).first
    assert_equal "/workers/tasks/poll", poll.path
    assert_equal "w-test", poll.body["worker_id"]
    assert_operator poll.body["limit"], :<=, 4
    refute poll.body.key?("queue_name")
    assert_equal "k", poll.headers["x-api-key"]
  end

  def test_nil_output_is_sent_as_empty_object
    task = @engine.add_task("noop")
    worker.register("noop") { nil }.start
    @engine.wait_for { @engine.acks(task["id"]).any? }
    assert_equal({}, @engine.acks(task["id"]).first.body["output"])
  end

  def test_error_classification
    r = @engine.add_task("retry")
    p = @engine.add_task("perm")
    c = @engine.add_task("crash")
    n = @engine.add_task("notimpl")
    w = worker
    w.register("retry") { raise Orch8::RetryableError, "boom" }
    w.register("perm") { raise Orch8::PermanentError, "fatal" }
    w.register("crash") { raise ArgumentError, "crash" }
    w.register("notimpl") { raise NotImplementedError }
    w.start
    @engine.wait_for { [r, p, c, n].all? { |t| @engine.acks(t["id"]).any? } }
    got = [r, p, c, n].map { |t| @engine.acks(t["id"]).first.body.values_at("retryable", "message") }
    assert_equal [[true, "boom"], [false, "fatal"], [true, "crash"], [true, "NotImplementedError"]], got
    [r, p, c, n].each { |t| assert_match(%r{/fail\z}, @engine.acks(t["id"]).first.path) }
  end

  def test_unknown_handler_fails_permanently_via_claim
    task = @engine.add_task("ghost", queue: "q")
    w = worker(queue: "q")
    w.register("echo") { {} }
    w.start(poll: false)
    res = w.claim(handler_name: "ghost", queue_name: "q", limit: 1)
    assert_equal 1, res["tasks"].size
    @engine.wait_for { @engine.acks(task["id"]).any? }
    body = @engine.acks(task["id"]).first.body
    refute body["retryable"]
    assert_match(/no handler/, body["message"])
    assert_equal "/workers/tasks/poll/queue", @engine.requests(:poll).first.path
  end

  def test_checkpoint_resumes_and_tracks_cas_sequence
    task = @engine.add_task("cp", params: { "steps" => 3 }, resume_checkpoint: { "step" => 1 }, checkpoint_seq: 5)
    seqs = []
    worker.register("cp") do |t|
      start = t.resume_checkpoint["step"]
      ((start + 1)..3).each { |i| seqs << t.checkpoint("step" => i) }
      { "from" => start, "seq" => t.checkpoint_seq }
    end.start
    @engine.wait_for { @engine.acks(task["id"]).any? }
    cps = @engine.requests("heartbeat", task["id"]).select { |r| r.body.key?("checkpoint") }
    assert_equal [[{ "step" => 2 }, 5], [{ "step" => 3 }, 6]], cps.map { |r| r.body.values_at("checkpoint", "checkpoint_seq") }
    assert_equal [6, 7], seqs
    assert_equal({ "from" => 1, "seq" => 7 }, @engine.acks(task["id"]).first.body["output"])
  end

  def test_heartbeats_at_server_hint
    @engine.heartbeat_interval_secs = 0.2
    task = @engine.add_task("slow")
    worker(heartbeat_interval: 15).register("slow") { |t| t.sleep(0.75) && { "ok" => true } }.start
    @engine.wait_for { @engine.acks(task["id"]).any? }
    hbs = @engine.requests("heartbeat", task["id"])
    assert_operator hbs.size, :>=, 2
    hbs.each { |h| refute h.body.key?("checkpoint") }
    assert_equal({ "ok" => true }, @engine.acks(task["id"]).first.body["output"])
  end

  def test_lease_loss_stops_heartbeats_cancels_and_skips_ack
    @engine.heartbeat_interval_secs = 0.1
    task = @engine.add_task("slow")
    after = @engine.add_task("echo")
    lost_at = nil
    @engine.hooks[:heartbeat] = lambda do |req|
      next unless req.params["id"] == task["id"]

      lost_at ||= Process.clock_gettime(Process::CLOCK_MONOTONIC)
      [409, error_body("conflict", "ownership changed")]
    end
    cancelled = Queue.new
    w = worker
    w.register("slow") do |t|
      t.sleep(5)
      cancelled << [t.cancelled?, t.cancel_reason]
      raise "handler error after cancellation must not be reported"
    end
    w.register("echo") { |t| t.params }
    w.start
    assert_equal [true, :lease_lost], pop_within(cancelled, 3)
    @engine.wait_for { @engine.acks(after["id"]).any? }
    sleep 0.3
    assert_empty @engine.acks(task["id"])
    late = @engine.requests("heartbeat", task["id"]).select { |r| r.at > lost_at + 0.05 }
    assert_empty late
  end

  def test_checkpoint_after_lease_loss_raises_lease_lost
    task = @engine.add_task("cp")
    @engine.hooks[:heartbeat] = ->(_req) { [404, error_body("not_found", "gone")] }
    raised = Queue.new
    worker.register("cp") do |t|
      t.checkpoint("x" => 1)
    rescue Orch8::LeaseLostError => e
      raised << e
      raise
    end.start
    assert_kind_of Orch8::LeaseLostError, pop_within(raised, 3)
    sleep 0.2
    assert_empty @engine.acks(task["id"])
  end

  def test_complete_retried_with_identical_body_after_503_but_not_after_409
    flaky = @engine.add_task("echo", params: { "w" => "flaky" })
    stolen = @engine.add_task("echo", params: { "w" => "stolen" })
    calls = Hash.new(0)
    @engine.hooks[:complete] = lambda do |req|
      id = req.params["id"]
      calls[id] += 1
      next [503, error_body("unavailable", "down")] if id == flaky["id"] && calls[id] == 1
      next [409, error_body("conflict", "lease changed")] if id == stolen["id"]
    end
    worker.register("echo") { |t| t.params }.start
    @engine.wait_for { @engine.acks(flaky["id"]).size >= 2 && @engine.acks(stolen["id"]).any? }
    sleep 0.5
    f = @engine.acks(flaky["id"])
    assert_equal 2, f.size
    assert_equal f[0].raw_body, f[1].raw_body
    s = @engine.acks(stolen["id"])
    assert_equal 1, s.size
    assert_match(/complete\z/, s[0].path)
  end

  def test_local_timeout_fails_retryable_and_cancels_handler
    task = @engine.add_task("slow", timeout_ms: 200)
    reasons = Queue.new
    worker.register("slow") do |t|
      t.sleep(5)
      reasons << t.cancel_reason
      { "late" => true }
    end.start
    assert_equal :timeout, pop_within(reasons, 3)
    @engine.wait_for { @engine.acks(task["id"]).any? }
    sleep 0.2
    acks = @engine.acks(task["id"])
    assert_equal 1, acks.size
    assert_match(/fail\z/, acks[0].path)
    assert acks[0].body["retryable"]
    assert_match(/timed out/, acks[0].body["message"])
  end

  def test_concurrency_limit_and_poll_limit
    5.times { @engine.add_task("slow") }
    running = 0
    peak = 0
    m = Mutex.new
    w = worker(concurrency: 2)
    w.register("slow") do |t|
      m.synchronize { peak = [peak, running += 1].max }
      t.sleep(0.15)
      m.synchronize { running -= 1 }
      {}
    end
    w.register("other") { {} }
    w.start
    @engine.wait_for(5) { @engine.tasks.values.all? { |t| t["state"] == "completed" } }
    assert_equal 2, peak
    @engine.requests(:poll).each { |p| assert_operator p.body["limit"], :<=, 2 }
  end

  def test_empty_poll_honours_poll_after_ms
    w = worker(poll_interval: 0.01)
    w.register("idle") { {} }
    w.start
    sleep 1.3
    polls = @engine.requests(:poll)
    assert_operator polls.size, :>=, 2
    assert_operator polls.size, :<=, 3, "must wait poll_after_ms (1000ms) between empty polls"
  end

  def test_poll_errors_back_off_and_recover
    fails = 3
    @engine.hooks[:poll] = ->(_req) { (fails -= 1) >= 0 ? [503, error_body("unavailable", "down")] : nil }
    task = @engine.add_task("echo")
    worker(poll_interval: 0.02).register("echo") { {} }.start
    @engine.wait_for(5) { @engine.acks(task["id"]).any? }
    assert_operator @engine.requests(:poll).size, :>=, 4
  end

  def test_queue_version_and_capabilities_on_poll
    task = @engine.add_task("echo", queue: "gpu")
    worker(queue: "gpu", version: "2.3.4", capabilities: { "labels" => { "gpu" => "a100" } })
      .register("echo") { {} }.start
    @engine.wait_for { @engine.acks(task["id"]).any? }
    poll = @engine.requests(:poll).first
    assert_equal "/workers/tasks/poll/queue", poll.path
    assert_equal "gpu", poll.body["queue_name"]
    assert_equal "2.3.4", poll.body["version"]
    assert_equal({ "runtime_id" => "w-test", "handlers" => ["echo"], "labels" => { "gpu" => "a100" } },
                 poll.body["capabilities"])
  end

  def test_graceful_stop_drains_in_flight_and_stops_polling
    task = @engine.add_task("slow")
    started = Queue.new
    w = worker
    w.register("slow") do |t|
      started << true
      t.sleep(0.4)
      { "done" => true }
    end
    w.start
    pop_within(started, 3)
    stop_at = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    assert w.stop(timeout: 5)
    assert_equal 1, @engine.acks(task["id"]).size
    assert_empty(@engine.requests(:poll).select { |p| p.at > stop_at + 0.05 })
    refute w.running?
  end

  def test_stop_timeout_abandons_without_ack
    task = @engine.add_task("stuck")
    started = Queue.new
    w = worker
    w.register("stuck") do |t|
      started << true
      t.sleep(10)
      { "never" => true }
    end
    w.start
    pop_within(started, 3)
    refute w.stop(timeout: 0.2)
    sleep 0.2
    assert_empty @engine.acks(task["id"])
  end

  def test_run_returns_after_stop_from_another_thread
    w = worker
    w.register("idle") { {} }
    t = Thread.new { w.run(signals: []) }
    sleep 0.1
    assert w.stop(timeout: 1)
    assert t.join(2), "run must return after stop"
  end

  def test_register_validation
    w = worker
    assert_raises(ArgumentError) { w.register("x") }
    assert_raises(Orch8::ConfigurationError) { w.start }
    assert_raises(Orch8::ConfigurationError) { worker(concurrency: 0) }
  end
end

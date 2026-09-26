# frozen_string_literal: true

require_relative "test_helper"
require_relative "support/fake_engine"

class WelcomeEmail < Orch8::Job
  queue_as :emails
  retry_policy max_attempts: 5, initial_backoff: 0.5, max_backoff: 30

  PERFORMED = Queue.new

  def perform(user_id, locale: "en", tags: [])
    PERFORMED << [user_id, locale, tags, task&.id]
  end
end

class UrgentEmail < WelcomeEmail
  handler_name "urgent_email"
  priority 9
end

class JobTest < Minitest::Test
  JOB = { "id" => "job_1", "instance_id" => "i", "handler" => "WelcomeEmail", "status" => "scheduled",
          "created_at" => "2026-09-26T10:00:00Z", "run_at" => "2026-09-26T10:00:00Z" }.freeze

  def setup
    @engine = FakeEngine.new
    @engine.server.route("POST", "/jobs") { [201, JOB] }
    @client = Orch8::Client.new(base_url: @engine.base_url, api_key: "k", tenant_id: "t")
    Orch8.client = @client
  end

  def teardown
    Orch8.client = nil
    @engine.close
  end

  def bodies = @engine.server.find("POST", "/jobs").map(&:body)

  def test_perform_later_enqueues_class_handler_with_args_payload
    info = WelcomeEmail.perform_later(42)
    assert_equal "job_1", info.id
    assert_equal({ "handler" => "WelcomeEmail", "payload" => { "args" => [42] }, "queue" => "emails",
                   "retry" => { "max_attempts" => 5, "initial_backoff_ms" => 500, "max_backoff_ms" => 30_000 } },
                 bodies.first)
  end

  def test_set_wait_priority_and_kwargs
    WelcomeEmail.set(wait: 300, priority: 3, idempotency_key: "welcome-42").perform_later(42, locale: :uk)
    body = bodies.first
    assert_equal 300_000, body["delay_ms"]
    assert_equal 3, body["priority"]
    assert_equal "welcome-42", body["idempotency_key"]
    assert_equal({ "args" => [42], "kwargs" => { "locale" => "uk" } }, body["payload"])
  end

  def test_perform_at_sends_run_at
    WelcomeEmail.perform_at(Time.utc(2026, 10, 1, 12, 0, 0), 7)
    assert_equal "2026-10-01T12:00:00Z", bodies.first["run_at"]
    refute bodies.first.key?("delay_ms")
  end

  def test_subclass_inherits_queue_and_overrides_handler_and_priority
    UrgentEmail.perform_later(1)
    body = bodies.first
    assert_equal "urgent_email", body["handler"]
    assert_equal "emails", body["queue"]
    assert_equal 9, body["priority"]
  end

  def test_perform_now_runs_inline
    WelcomeEmail.perform_now(5, locale: "de")
    assert_equal [5, "de", [], nil], pop_within(WelcomeEmail::PERFORMED, 1)
  end

  def test_worker_dispatches_payload_to_perform
    task = @engine.add_task("WelcomeEmail", queue: "emails",
                                            params: { "args" => [42], "kwargs" => { "locale" => "uk", "tags" => ["a"] } })
    worker = Orch8::Worker.new(client: @client, worker_id: "w", queue: "emails", poll_interval: 0.02,
                               logger: NullLogger.new)
    worker.register_jobs(WelcomeEmail)
    worker.start
    assert_equal [42, "uk", ["a"], task["id"]], pop_within(WelcomeEmail::PERFORMED, 3)
    @engine.wait_for { @engine.acks(task["id"]).any? }
    assert_equal({}, @engine.acks(task["id"]).first.body["output"])
  ensure
    worker&.stop(timeout: 1)
  end

  def test_register_all_uses_registry
    registered = []
    fake_worker = Object.new
    fake_worker.define_singleton_method(:register_jobs) { |k| registered << k }
    Orch8::Job.register_all(fake_worker)
    assert_includes registered, WelcomeEmail
    assert_includes registered, UrgentEmail
  end

  def test_unsupported_argument_raises
    assert_raises(ArgumentError) { WelcomeEmail.perform_later(Object.new) }
  end
end

class ConfigFallbackTest < Minitest::Test
  def test_worker_uses_global_configuration
    Orch8.configure do |c|
      c.base_url = "http://127.0.0.1:9/api/v1"
      c.tenant_id = "cfg-tenant"
    end
    w = Orch8::Worker.new(logger: NullLogger.new)
    assert_equal "http://127.0.0.1:9/api/v1", w.transport.base_url
    assert_equal "cfg-tenant", w.transport.tenant_id
  ensure
    Orch8.configure { |c| c.base_url = nil; c.tenant_id = nil } # rubocop:disable Style/Semicolon
    Orch8.client = nil
  end
end

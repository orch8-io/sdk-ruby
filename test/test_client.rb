# frozen_string_literal: true

require_relative "test_helper"

class ClientTest < Minitest::Test
  include ServerTest

  JOB = { "id" => "job_1", "instance_id" => "inst_1", "handler" => "send_email", "status" => "scheduled",
          "created_at" => "2026-09-26T10:00:00Z", "run_at" => "2026-09-26T10:05:00Z", "extra_field" => { "x" => 1 } }.freeze

  def client(**opts)
    Orch8::Client.new(base_url: @server.base_url, api_key: "k1", tenant_id: "t1", retry_base_delay: 0.001, **opts)
  end

  def test_sends_auth_tenant_and_json_headers_under_api_v1
    @server.route("POST", "/sequences") { [201, { "id" => "seq_1", "warnings" => [] }] }
    res = client.sequences.create(name: "onboarding", namespace: "default", tenant_id: "t1", blocks: [])
    assert_equal "seq_1", res.id
    req = @server.find("POST", "/sequences").first
    assert_equal "/api/v1/sequences", req.raw_path
    assert_equal "k1", req.headers["x-api-key"]
    assert_equal "t1", req.headers["x-tenant-id"]
    assert_includes req.headers["content-type"], "application/json"
    assert_equal({ "name" => "onboarding", "namespace" => "default", "tenant_id" => "t1", "blocks" => [] }, req.body)
  end

  def test_list_passes_query_and_returns_array
    @server.route("GET", "/sequences") { [200, [{ "id" => "a" }, { "id" => "b" }]] }
    list = client.sequences.list(namespace: "default", limit: 10, offset: nil)
    assert_equal %w[a b], list.map(&:id)
    assert_equal({ "namespace" => "default", "limit" => "10" }, @server.find("GET", "/sequences").first.query)
  end

  def test_path_ids_are_encoded_as_one_segment
    @server.route("GET", "/jobs/:id") { |r| [200, JOB.merge("id" => r.params["id"])] }
    job = client.jobs.get("a/b c")
    assert_equal "a/b c", job.id
    assert_equal "/api/v1/jobs/a%2Fb%20c", @server.find("GET").first.raw_path
  end

  def test_instances_create_signal_and_cancel
    @server.route("POST", "/instances") { |r| r.body["idempotency_key"] ? [200, { "id" => "i1", "deduplicated" => true }] : [201, { "id" => "i1" }] }
    @server.route("POST", "/instances/:id/signals") { [201, { "signal_id" => "s1" }] }
    c = client
    assert_equal "i1", c.instances.create(sequence_id: "seq", tenant_id: "t1", namespace: "default", context: nil).id
    dup = c.instances.create(sequence_id: "seq", tenant_id: "t1", namespace: "default", idempotency_key: "k")
    assert dup.deduplicated?
    assert_equal "s1", c.instances.signal("i1", { custom: "approve" }, { by: "alice" }).signal_id
    c.instances.cancel("i1")
    c.instances.signal("i1", :pause)
    first = @server.find("POST", "/instances").first
    refute first.body.key?("context"), "nil fields must be omitted"
    sigs = @server.find("POST", "/instances/i1/signals").map(&:body)
    assert_equal({ "signal_type" => { "custom" => "approve" }, "payload" => { "by" => "alice" } }, sigs[0])
    assert_equal({ "signal_type" => "cancel" }, sigs[1])
    assert_equal({ "signal_type" => "pause" }, sigs[2])
  end

  def test_jobs_enqueue_omits_unset_fields_and_maps_options
    @server.route("POST", "/jobs") { [201, JOB] }
    c = client
    job = c.jobs.enqueue("send_email", { to: "a@example.com" })
    c.jobs.enqueue("send_email", {}, queue: :emails, priority: 5,
                                     retry: { max_attempts: 4, initial_backoff_ms: 500, max_backoff_ms: nil },
                                     delay_ms: 250, idempotency_key: "welcome-a", metadata: { source: "signup" })
    c.jobs.enqueue("send_email", nil, run_at: Time.utc(2026, 10, 1))
    c.jobs.enqueue(handler: "send_email", payload: { "n" => 1 })
    bodies = @server.find("POST", "/jobs").map(&:body)
    assert_equal({ "handler" => "send_email", "payload" => { "to" => "a@example.com" } }, bodies[0])
    assert_equal({ "handler" => "send_email", "payload" => {}, "queue" => "emails", "priority" => 5,
                   "retry" => { "max_attempts" => 4, "initial_backoff_ms" => 500 }, "delay_ms" => 250,
                   "idempotency_key" => "welcome-a", "metadata" => { "source" => "signup" } }, bodies[1])
    assert_equal "2026-10-01T00:00:00Z", bodies[2]["run_at"]
    assert_equal({ "handler" => "send_email", "payload" => { "n" => 1 } }, bodies[3])
    %w[id instance_id handler status created_at run_at].each { |k| refute_nil job[k] }
    assert_equal({ "x" => 1 }, job.extra_field.to_h, "unknown fields stay accessible")
    refute job.done?
  end

  def test_jobs_enqueue_rejects_delay_and_run_at_together
    assert_raises(ArgumentError) { client.jobs.enqueue("h", {}, delay_ms: 1, run_at: Time.now) }
  end

  def test_jobs_list_tolerates_page_objects_and_cancel_204
    responses = [[200, [JOB]], [200, { "items" => [JOB] }], [200, { "jobs" => [JOB, JOB] }]]
    @server.route("GET", "/jobs") { responses.shift }
    @server.route("DELETE", "/jobs/:id") { |r| r.params["id"] == "gone" ? [204, nil] : [200, JOB.merge("status" => "cancelled")] }
    c = client
    assert_equal 1, c.jobs.list(status: "scheduled").size
    assert_equal 1, c.jobs.list.size
    jobs = c.jobs.list
    assert_equal 2, jobs.size
    assert_instance_of Orch8::JobInfo, jobs.first
    assert c.jobs.cancel("job_1").done?
    assert_nil c.jobs.cancel("gone")
  end

  def test_safe_requests_retry_transient_statuses
    statuses = [429, 503, 200]
    @server.route("GET", "/jobs/:id") do
      s = statuses.shift
      s == 200 ? [200, JOB] : [s, error_body("unavailable", "try later")]
    end
    assert_equal "job_1", client.jobs.get("job_1").id
    assert_equal 3, @server.find("GET").size
  end

  def test_safe_requests_give_up_after_max_attempts
    @server.route("GET", "/jobs/:id") { [503, error_body("unavailable", "down")] }
    err = assert_raises(Orch8::ServerError) { client(max_attempts: 2).jobs.get("x") }
    assert_equal 503, err.status
    assert_equal 2, @server.find("GET").size
  end

  def test_unsafe_requests_are_never_replayed
    @server.route("POST", "/jobs") { [503, error_body("unavailable", "down")] }
    err = assert_raises(Orch8::ServerError) { client.jobs.enqueue("h", {}) }
    assert_equal "server", err.kind
    assert_equal "unavailable", err.code
    assert_equal 1, @server.find("POST").size
  end

  def test_typed_errors_from_envelope
    {
      400 => [Orch8::BadRequestError, "invalid_argument"], 401 => [Orch8::UnauthorizedError, "unauthorized"],
      403 => [Orch8::ForbiddenError, "forbidden"], 404 => [Orch8::NotFoundError, "not_found"],
      409 => [Orch8::ConflictError, "conflict"], 413 => [Orch8::PayloadTooLargeError, "payload_too_large"],
      422 => [Orch8::UnprocessableEntityError, "unprocessable"], 429 => [Orch8::RateLimitedError, "rate_limited"],
      500 => [Orch8::ServerError, "server"], 502 => [Orch8::ServerError, "server"], 418 => [Orch8::APIError, "api"]
    }.each do |status, (klass, kind)|
      @server.route("POST", "/jobs") { [status, error_body("code_#{status}", "msg #{status}")] }
      err = assert_raises(Orch8::APIError) { client.jobs.enqueue("h", {}) }
      assert_instance_of klass, err
      assert_equal kind, err.kind
      assert_equal status, err.status
      assert_equal "code_#{status}", err.code
      assert_equal "msg #{status}", err.message
    end
  end

  def test_error_without_envelope_still_has_message
    @server.route("GET", "/jobs/:id") { [404, nil] }
    err = assert_raises(Orch8::NotFoundError) { client.jobs.get("x") }
    assert_equal "HTTP 404", err.message
    assert_nil err.code
  end

  def test_transport_error_on_connection_failure
    port = TCPServer.new("127.0.0.1", 0).then { |s| s.addr[1].tap { s.close } }
    c = Orch8::Client.new(base_url: "http://127.0.0.1:#{port}/api/v1", max_attempts: 2, retry_base_delay: 0.001)
    err = assert_raises(Orch8::TransportError) { c.jobs.get("x") }
    assert_equal "transport", err.kind
    assert_kind_of Orch8::Error, err
  end

  def test_protocol_relative_path_is_rejected
    err = assert_raises(Orch8::InvalidRequestError) { client.transport.request("GET", "//untrusted.test/path") }
    assert_match(/invalid_path/, err.message)
  end

  def test_empty_success_body_maps_to_nil
    @server.route("GET", "/probe") { [204, nil] }
    assert_nil client.transport.request("GET", "/probe")
  end

  def test_transport_fixture_cases
    fixture = JSON.parse(File.read(File.join(FIXTURES, "transport.json")))
    fixture["cases"].each do |kase|
      server = FakeServer.new
      begin
        responses = (kase["responses"] || []).dup
        server.route(kase["method"], "/probe") do
          s = responses.shift
          [s, s >= 400 ? error_body("x", "status #{s}") : nil]
        end
        c = Orch8::Client.new(base_url: server.base_url, retry_base_delay: 0.001,
                              max_attempts: fixture["defaults"]["max_attempts"])
        path = kase["path"] || "/probe"
        if kase["expected_error"]
          assert_raises(Orch8::InvalidRequestError, kase["name"]) { c.transport.request(kase["method"], path) }
          next
        end
        result = begin
          c.transport.request(kase["method"], path)
        rescue Orch8::APIError
          :error
        end
        assert_equal kase["expected_attempts"], server.all.size, kase["name"] if kase["expected_attempts"]
        assert_nil result, kase["name"] if kase.key?("expected_body")
      ensure
        server.close
      end
    end
  end

  def test_resource_access
    r = Orch8::Resource.wrap("a" => { "b" => [{ "c" => 1 }] }, "flag" => true)
    assert_equal 1, r.a.b.first.c
    assert_equal 1, r.dig(:a, :b, 0, :c)
    assert r.flag?
    assert r.respond_to?(:a)
    assert_raises(NoMethodError) { r.missing }
    assert_equal({ "a" => { "b" => [{ "c" => 1 }] }, "flag" => true }, JSON.parse(r.to_json))
  end

  def test_configuration_builds_shared_client
    Orch8.configure do |c|
      c.base_url = @server.base_url
      c.api_key = "cfg"
      c.tenant_id = "cfg-t"
    end
    @server.route("GET", "/jobs/:id") { [200, JOB] }
    assert_equal "job_1", Orch8.client.jobs.get("job_1").id
    assert_equal "cfg", @server.find("GET").first.headers["x-api-key"]
  ensure
    Orch8.client = nil
  end

  def test_missing_base_url_is_a_configuration_error
    assert_raises(Orch8::ConfigurationError) { Orch8::Client.new(base_url: nil) }
    assert_raises(Orch8::ConfigurationError) { Orch8::Client.new(base_url: "not a url") }
  end
end

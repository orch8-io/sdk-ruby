# frozen_string_literal: true

require_relative "test_helper"
require_relative "support/fake_engine"

# ActiveJob is not a dependency of this gem. When the real library is
# available (e.g. ACTIVEJOB=1 with activejob installed) it is used; otherwise
# a minimal stand-in with the same surface the adapter touches is defined.
begin
  raise LoadError unless ENV["ACTIVEJOB"] == "1"

  require "active_job"
  REAL_ACTIVE_JOB = true
rescue LoadError
  REAL_ACTIVE_JOB = false
  module ActiveJob
    module QueueAdapters; end

    class Base
      EXECUTED = Queue.new
      attr_accessor :provider_job_id
      attr_reader :arguments, :job_id

      def self.queue_name = "mailers"
      def self.execute(data) = EXECUTED << data

      def initialize(*arguments)
        @arguments = arguments
        @job_id = "aj-#{object_id}"
      end

      def queue_name = self.class.queue_name
      def priority = 4

      def serialize
        { "job_class" => self.class.name, "job_id" => job_id, "queue_name" => queue_name, "arguments" => arguments }
      end
    end
  end
end

require "active_job/queue_adapters/orch8_adapter"

if REAL_ACTIVE_JOB
  ActiveJob::Base.logger = Logger.new(nil)
  class RecordingJob < ActiveJob::Base
    PERFORMED = Queue.new
    def perform(*args) = PERFORMED << args
  end
end

class ActiveJobAdapterTest < Minitest::Test
  class Notify < ActiveJob::Base
    def perform(*); end
  end

  JOB = { "id" => "job_9", "instance_id" => "i", "handler" => "active_job", "status" => "scheduled",
          "created_at" => "2026-09-26T10:00:00Z", "run_at" => "2026-09-26T10:00:00Z" }.freeze

  def setup
    @engine = FakeEngine.new
    @engine.server.route("POST", "/jobs") { [201, JOB] }
    @client = Orch8::Client.new(base_url: @engine.base_url, api_key: "k", tenant_id: "t")
    @adapter = ActiveJob::QueueAdapters::Orch8Adapter.new(client: @client)
  end

  def teardown = @engine.close

  def test_enqueue_posts_serialized_job
    job = Notify.new(1, "x")
    @adapter.enqueue(job)
    body = @engine.server.find("POST", "/jobs").first.body
    assert_equal "active_job", body["handler"]
    assert_equal job.serialize.transform_values { |v| v.is_a?(Symbol) ? v.to_s : v }.slice("job_class", "job_id"),
                 body["payload"].slice("job_class", "job_id")
    assert_equal job.queue_name.to_s, body["queue"]
    assert_equal "job_9", job.provider_job_id
    assert_equal "ActiveJobAdapterTest::Notify", body.dig("metadata", "active_job_class")
  end

  def test_enqueue_at_sends_run_at
    @adapter.enqueue_at(Notify.new, Time.utc(2026, 10, 1, 8).to_f)
    assert_equal "2026-10-01T08:00:00Z", @engine.server.find("POST", "/jobs").first.body["run_at"]
  end

  def test_worker_side_executes_payload
    if REAL_ACTIVE_JOB
      payload = JSON.parse(JSON.generate(RecordingJob.new(1, "two").serialize))
      received = RecordingJob::PERFORMED
      expected = [1, "two"]
    else
      payload = { "job_class" => "X", "arguments" => [1] }
      received = ActiveJob::Base::EXECUTED
      expected = payload
    end
    task = @engine.add_task("active_job", params: payload)
    worker = Orch8::Worker.new(client: @client, worker_id: "w", poll_interval: 0.02, logger: NullLogger.new)
    ActiveJob::QueueAdapters::Orch8Adapter.register(worker)
    worker.start
    assert_equal expected, pop_within(received, 3)
    @engine.wait_for { @engine.acks(task["id"]).any? }
    assert_match(/complete\z/, @engine.acks(task["id"]).first.path)
  ensure
    worker&.stop(timeout: 1)
  end
end

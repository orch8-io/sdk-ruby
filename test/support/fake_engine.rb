# frozen_string_literal: true

require "securerandom"
require "time"

# A scripted worker-protocol engine on top of FakeServer: poll/claim with
# claim_epoch, heartbeat + checkpoint CAS, complete/fail, fault hooks.
class FakeEngine
  attr_reader :server, :tasks, :hooks
  attr_accessor :heartbeat_interval_secs, :lease_secs

  def initialize
    @server = FakeServer.new
    @tasks = {}
    @order = []
    @hooks = {}
    @lock = Mutex.new
    @heartbeat_interval_secs = 15
    @lease_secs = 60
    install
  end

  def base_url = @server.base_url
  def close = @server.close

  def add_task(handler, queue: nil, **overrides)
    id = SecureRandom.uuid
    task = {
      "id" => id, "instance_id" => SecureRandom.uuid, "block_id" => "step", "handler_name" => handler,
      "params" => {}, "context" => { "data" => {} }, "attempt" => 0, "timeout_ms" => nil, "state" => "pending",
      "worker_id" => nil, "claim_epoch" => 0, "checkpoint_seq" => 0, "created_at" => Time.now.utc.iso8601(3)
    }.merge(overrides.transform_keys(&:to_s))
    task["queue_name"] = queue if queue
    @lock.synchronize do
      @tasks[id] = task
      @order << id
    end
    task
  end

  def requests(kind, task_id = nil)
    @server.all.select do |r|
      case kind
      when :poll then r.path.start_with?("/workers/tasks/poll")
      else r.path == "/workers/tasks/#{task_id}/#{kind}"
      end
    end
  end

  def acks(task_id) = @server.all.select { |r| r.path =~ %r{\A/workers/tasks/#{task_id}/(complete|fail)\z} }

  def wait_for(timeout = 5, &block) = @server.wait_for(timeout, &block)

  private

  def install
    poll = lambda do |req|
      hooked = @hooks[:poll]&.call(req)
      next hooked if hooked

      claimed = @lock.synchronize do
        @order.filter_map { |id| @tasks[id] }
              .select { |t| t["state"] == "pending" && t["handler_name"] == req.body["handler_name"] && t["queue_name"] == req.body["queue_name"] }
              .first(req.body["limit"] || 1)
              .each do |t|
                t["state"] = "claimed"
                t["worker_id"] = req.body["worker_id"]
                t["claim_epoch"] += 1
              end
              .map(&:dup)
      end
      [200, { "tasks" => claimed, "lease_secs" => @lease_secs, "heartbeat_interval_secs" => @heartbeat_interval_secs,
              "poll_after_ms" => claimed.empty? ? 1000 : 0 }]
    end
    @server.route("POST", "/workers/tasks/poll", &poll)
    @server.route("POST", "/workers/tasks/poll/queue", &poll)

    @server.route("POST", "/workers/tasks/:id/heartbeat") do |req|
      hooked = @hooks[:heartbeat]&.call(req)
      next hooked if hooked

      @lock.synchronize do
        t = @tasks[req.params["id"]]
        next [404, error_body("not_found", "not found")] unless t
        next [409, error_body("conflict", "ownership changed")] unless owned?(t, req.body)

        if req.body.key?("checkpoint")
          next [409, error_body("conflict", "seq changed")] unless req.body["checkpoint_seq"] == t["checkpoint_seq"]

          t["checkpoint_seq"] += 1
          t["resume_checkpoint"] = req.body["checkpoint"]
        end
        [200, { "checkpoint_seq" => t["checkpoint_seq"] }]
      end
    end

    %w[complete fail].each do |kind|
      @server.route("POST", "/workers/tasks/:id/#{kind}") do |req|
        hooked = @hooks[kind.to_sym]&.call(req)
        next hooked if hooked

        @lock.synchronize do
          t = @tasks[req.params["id"]]
          next [404, error_body("not_found", "not found")] unless t
          next [200, nil] if t["state"] == "completed" && owned_any?(t, req.body) && kind == "complete"
          next [409, error_body("conflict", "not claimed")] unless owned?(t, req.body)

          t["state"] = kind == "complete" ? "completed" : "failed"
          [200, nil]
        end
      end
    end
  end

  def owned?(task, body) = task["state"] == "claimed" && owned_any?(task, body)
  def owned_any?(task, body) = task["worker_id"] == body["worker_id"] && task["claim_epoch"] == body["claim_epoch"]
end

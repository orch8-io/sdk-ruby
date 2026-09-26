# frozen_string_literal: true

module Orch8
  # What a handler receives for one claimed worker task.
  #
  #   worker.register("resize_image") do |task|
  #     start = task.resume_checkpoint&.fetch("page", 0) || 0
  #     (start...pages).each do |page|
  #       break if task.cancelled?
  #       render(page)
  #       task.checkpoint("page" => page + 1)   # CAS sequence tracked automatically
  #     end
  #     { "pages" => pages }                    # -> complete(output)
  #   end
  class TaskContext
    # The raw task object from the poll response (all fields, including unknown ones).
    attr_reader :task

    def initialize(worker, state)
      @worker = worker
      @state = state
      @task = state.task
    end

    def id = @task["id"]
    def instance_id = @task["instance_id"]
    def block_id = @task["block_id"]
    def handler_name = @task["handler_name"]
    def queue_name = @task["queue_name"]
    def params = @task["params"]
    def context = @task["context"]
    def attempt = @task["attempt"].to_i
    def timeout_ms = @task["timeout_ms"]
    def claim_epoch = @task["claim_epoch"]
    def resume_checkpoint = @task["resume_checkpoint"]
    def worker_id = @worker.worker_id
    def logger = @worker.logger
    def [](key) = @task[key.to_s]

    # Current compare-and-swap sequence of the durable checkpoint.
    def checkpoint_seq = @state.seq

    # Persists `value` as the task's durable checkpoint (WORKER_PROTOCOL §4.2)
    # and refreshes the lease. A later claimer (after a crash, reap or
    # engine-driven retry) receives it as `resume_checkpoint`.
    #
    # @return [Integer] the new checkpoint sequence
    # @raise [Orch8::LeaseLostError] when the worker no longer owns the task
    def checkpoint(value) = @worker.__send__(:write_checkpoint, @state, value)

    # Sends an immediate plain heartbeat (the worker also heartbeats on its own).
    def heartbeat! = @worker.__send__(:send_heartbeat, @state)

    # True once the task is cancelled: lease lost (404/409), local
    # `timeout_ms` exceeded, or forced shutdown after the drain timeout.
    def cancelled? = @state.cancelled?

    # :lease_lost, :timeout, :shutdown or nil.
    def cancel_reason = @state.cancel_reason

    # @raise [Orch8::TaskCancelledError] if the task has been cancelled.
    def check_cancelled!
      raise TaskCancelledError, @state.cancel_reason if @state.cancelled?
    end

    # Sleeps up to `seconds`, returning early when the task is cancelled.
    # @return [Boolean] true if the full duration elapsed, false if cancelled
    def sleep(seconds) = @state.wait_cancelled(seconds)

    # Wall-clock deadline derived from `created_at + timeout_ms`, or nil.
    def deadline = @state.deadline
  end
end

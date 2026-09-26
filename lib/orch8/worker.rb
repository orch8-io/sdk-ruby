# frozen_string_literal: true

require "socket"
require "time"

module Orch8
  # Long-poll worker implementing the Orch8 worker wire protocol
  # (sdk-contract/WORKER_PROTOCOL.md).
  #
  #   worker = Orch8::Worker.new(base_url: "http://localhost:8080/api/v1",
  #                              api_key: "...", tenant_id: "acme", concurrency: 8)
  #   worker.register("send_email") { |task| Mailer.deliver(task.params); { "sent" => true } }
  #   worker.run   # blocks; SIGTERM/SIGINT -> graceful drain
  #
  # Behaviour:
  # * one poll loop per handler; concurrency slots are reserved *before* each
  #   poll so `limit` never exceeds free capacity (P11), and a full worker does
  #   not poll;
  # * honours `poll_after_ms` after an empty poll (P8); poll errors back off
  #   exponentially (base = poll_interval, cap 30 s);
  # * heartbeats every in-flight task at min(heartbeat_interval,
  #   server `heartbeat_interval_secs`, lease_secs / 2) (P9);
  # * 404/409 on any mutation = lease lost: stop heartbeating, cancel the
  #   handler, never ack (L2/L3);
  # * `complete` is retried with the identical body on transport errors and
  #   retryable statuses (K3); `retryable` is always sent explicitly (F1);
  # * generic exceptions are reported as retryable (F4); `timeout_ms` is
  #   enforced locally (cooperative cancellation + retryable fail).
  class Worker
    POLL_PATH = "/workers/tasks/poll"
    QUEUE_POLL_PATH = "/workers/tasks/poll/queue"
    MAX_POLL_BACKOFF = 30.0
    MAX_MESSAGE_BYTES = 16 * 1024

    attr_reader :worker_id, :concurrency, :queue, :version, :logger, :transport

    # @param client [Orch8::Client, nil] reuse a client's transport (else built from base_url/api_key/tenant_id)
    # @param worker_id [String] unique per process (default "hostname-pid")
    # @param concurrency [Integer] max tasks executing at once, across all handlers
    # @param poll_interval [Float] seconds between polls when idle (server `poll_after_ms` wins when larger)
    # @param heartbeat_interval [Float] seconds; capped by the server's `heartbeat_interval_secs`
    # @param queue [String, nil] named queue; polls /workers/tasks/poll/queue when set
    # @param version [String, nil] worker/app version sent on polls (version pins, P4)
    # @param shutdown_timeout [Float] seconds to drain in-flight tasks on stop
    # @param capabilities [Hash, nil] RuntimeCapabilities for capability-matched claiming (P5)
    #
    # Connection settings not given explicitly fall back to Orch8.configuration
    # (which itself defaults to ORCH8_BASE_URL / ORCH8_API_KEY / ORCH8_TENANT_ID).
    def initialize(client: nil, base_url: nil, api_key: nil, tenant_id: nil, worker_id: nil, concurrency: 10,
                   poll_interval: 1.0, heartbeat_interval: 15.0, queue: nil, version: nil,
                   shutdown_timeout: 30.0, capabilities: nil, logger: nil, ack_attempts: 5,
                   max_poll_backoff: MAX_POLL_BACKOFF, **transport_options)
      @transport = if client
                     client.transport
                   else
                     cfg = Orch8.configuration
                     Transport.new(base_url: base_url || cfg.base_url, api_key: api_key || cfg.api_key,
                                   tenant_id: tenant_id || cfg.tenant_id, **transport_options)
                   end
      @worker_id = (worker_id || "#{Socket.gethostname}-#{Process.pid}").to_s
      @concurrency = Integer(concurrency)
      raise ConfigurationError, "concurrency must be >= 1" if @concurrency < 1

      @poll_interval = poll_interval.to_f
      @configured_heartbeat = heartbeat_interval.to_f
      @heartbeat_interval = @configured_heartbeat
      @queue = queue.nil? || queue.to_s.empty? ? nil : queue.to_s
      @version = version.nil? || version.to_s.empty? ? nil : version.to_s
      @shutdown_timeout = shutdown_timeout.to_f
      @capabilities = capabilities
      @logger = logger || SimpleLogger.new
      @ack_attempts = [ack_attempts.to_i, 1].max
      @max_poll_backoff = max_poll_backoff.to_f

      @handlers = {}
      @lock = Mutex.new
      @stop_cv = ConditionVariable.new   # broadcast only on stop
      @slot_cv = ConditionVariable.new   # broadcast when a slot frees
      @drain_cv = ConditionVariable.new  # broadcast when an in-flight task finishes
      @hb_cv = ConditionVariable.new
      @free = @concurrency
      @in_flight = {}
      @running = false
      @started = false
      @stopped = false
      @poll_threads = []
      @pool_threads = []
      @work_queue = Thread::Queue.new
      @wake_r, @wake_w = IO.pipe
    end

    # Registers a handler. The callable receives an Orch8::TaskContext and
    # returns the task output (a Hash is merged into the instance context;
    # nil is sent as {}).
    def register(name, callable = nil, &block)
      handler = callable || block
      raise ArgumentError, "handler #{name} needs a block or a callable" unless handler.respond_to?(:call)

      @lock.synchronize do
        raise Error, "cannot register handlers after the worker started" if @started && !@push_only

        @handlers[name.to_s] = handler
      end
      self
    end
    alias handle register

    def handler_names = @lock.synchronize { @handlers.keys }
    def running? = @lock.synchronize { @running }
    def in_flight_count = @lock.synchronize { @in_flight.size }
    def free_slots = @lock.synchronize { @free }

    # Current effective heartbeat interval in seconds.
    def heartbeat_interval = @lock.synchronize { @heartbeat_interval }

    # Starts the worker threads and returns immediately.
    #
    # @param poll [Boolean] false starts only the executor/heartbeat threads
    #   (push-dispatch receivers claim via #claim instead of long-polling)
    def start(poll: true)
      @lock.synchronize do
        raise Error, "worker already started" if @started
        raise ConfigurationError, "register at least one handler before starting" if poll && @handlers.empty?

        @started = true
        @running = true
        @push_only = !poll
      end
      @concurrency.times { |i| @pool_threads << spawn_thread("orch8-exec-#{i}") { executor_loop } }
      @heartbeat_thread = spawn_thread("orch8-heartbeat") { heartbeat_loop }
      handler_names.each { |name| @poll_threads << spawn_thread("orch8-poll-#{name}") { poll_loop(name) } } if poll
      self
    end

    # Starts the worker and blocks until #stop is called or one of `signals`
    # is received, then drains gracefully. Signal traps only write to a pipe;
    # the drain runs on the calling thread.
    def run(signals: %w[TERM INT], poll: true)
      previous = install_traps(signals)
      start(poll: poll)
      wait_for_wakeup
      stop
    ensure
      restore_traps(previous) if previous
    end

    # Graceful shutdown (WORKER_PROTOCOL §7.7): stop polling immediately, keep
    # heartbeating in-flight tasks, let them finish and acknowledge, then
    # return. Tasks still running after `timeout` seconds are cancelled and
    # abandoned without acknowledgement (the engine reclaims them).
    #
    # @return [Boolean] true when every in-flight task finished in time
    def stop(timeout: @shutdown_timeout)
      @lock.synchronize do
        return true unless @started

        if @stopped
          # Another caller is already draining: wait for it to finish.
          @drain_cv.wait(@lock, 1) until @stop_done
          return @stop_result
        end

        @stopped = true
        @running = false
        @stop_cv.broadcast
        @slot_cv.broadcast
      end
      wake_runner
      deadline = monotonic + timeout.to_f
      @poll_threads.each { |t| t.join([deadline - monotonic, 0.01].max) }

      leftover = @lock.synchronize do
        @drain_cv.wait(@lock, deadline - monotonic) while !@in_flight.empty? && monotonic < deadline
        @in_flight.values
      end
      leftover.each do |state|
        logger.warn("abandoning task #{state.id} after #{timeout}s drain timeout")
        state.abandon!
      end

      @lock.synchronize do
        @heartbeat_stopped = true
        @hb_cv.broadcast
      end
      @heartbeat_thread&.join(1)
      @pool_threads.size.times { @work_queue << nil }
      @pool_threads.each { |t| t.join(leftover.empty? ? 1 : 0.1) }
      @lock.synchronize do
        @stop_result = leftover.empty?
        @stop_done = true
        @drain_cv.broadcast
      end
      @stop_result
    end

    # Claims up to `limit` tasks right now and schedules them for execution.
    # Used by push-dispatch receivers (a push is only a wake-up, D2) and
    # available for custom loops. Never requests more than the free slots.
    #
    # @return [Hash] the poll response ("tasks", "poll_after_ms", ...)
    def claim(handler_name:, queue_name: @queue, limit: @concurrency)
      raise Error, "start the worker first (worker.start(poll: false))" unless running?

      poll_once(handler_name.to_s, queue_name, limit)
    end

    private

    # ------------------------------------------------------------------
    # Polling
    # ------------------------------------------------------------------

    def poll_loop(name)
      failures = 0
      while running?
        next unless wait_for_slot

        delay = @poll_interval
        begin
          res = poll_once(name, @queue, @concurrency)
          failures = 0
          delay = if Array(res["tasks"]).empty?
                    [@poll_interval, res["poll_after_ms"].to_f / 1000.0].max
                  else
                    0
                  end
        rescue StandardError => e
          failures += 1
          delay = [@poll_interval * (2**failures), @max_poll_backoff].min
          logger.warn("poll #{name} failed (#{describe(e)}); retrying in #{delay.round(3)}s")
        end
        interruptible_sleep(delay) if delay.positive?
      end
    rescue StandardError => e
      logger.error("poll loop #{name} crashed: #{describe(e)}")
    end

    # Blocks until a slot is free; false when the worker is stopping.
    def wait_for_slot
      @lock.synchronize do
        @slot_cv.wait(@lock, 1) while @running && @free <= 0
        @running
      end
    end

    def interruptible_sleep(seconds)
      deadline = monotonic + seconds
      @lock.synchronize do
        while @running
          remaining = deadline - monotonic
          break if remaining <= 0

          @stop_cv.wait(@lock, remaining)
        end
      end
    end

    # Slots are reserved BEFORE the request is sent so concurrent per-handler
    # loops never ask the engine for more tasks than the worker can start.
    def poll_once(handler_name, queue_name, wanted)
      limit = @lock.synchronize do
        n = [wanted.to_i, @free].min
        @free -= n if n.positive?
        n
      end
      return { "tasks" => [], "poll_after_ms" => 0 } if limit <= 0

      begin
        res = @transport.request("POST", queue_name ? QUEUE_POLL_PATH : POLL_PATH,
                                 body: poll_body(handler_name, queue_name, limit))
      rescue StandardError
        release_slots(limit)
        raise
      end
      res = {} unless res.is_a?(Hash)
      tasks = Array(res["tasks"]).first(limit)
      release_slots(limit - tasks.size)
      update_heartbeat_interval(res)
      tasks.each { |task| dispatch(task) }
      res
    end

    def poll_body(handler_name, queue_name, limit)
      body = { "handler_name" => handler_name, "worker_id" => @worker_id, "limit" => limit }
      body["queue_name"] = queue_name if queue_name
      body["version"] = @version if @version
      if @capabilities
        body["capabilities"] = { "runtime_id" => @worker_id, "handlers" => handler_names }
                               .merge(Client::Util.plain(@capabilities))
      end
      body
    end

    def update_heartbeat_interval(res)
      hint = res["heartbeat_interval_secs"]
      lease = res["lease_secs"]
      interval = @configured_heartbeat
      interval = [interval, hint.to_f].min if hint.is_a?(Numeric) && hint.positive?
      interval = [interval, lease.to_f / 2.0].min if lease.is_a?(Numeric) && lease.positive?
      @lock.synchronize { @heartbeat_interval = [interval, 0.05].max }
    end

    def release_slots(count)
      return unless count.positive?

      @lock.synchronize do
        @free += count
        @slot_cv.broadcast
      end
    end

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------

    def dispatch(task)
      state = TaskState.new(task, self)
      @lock.synchronize do
        state.next_heartbeat_at = monotonic + @heartbeat_interval
        @in_flight[state.key] = state
        @hb_cv.broadcast
      end
      @work_queue << state
    end

    def executor_loop
      while (state = @work_queue.pop)
        execute(state)
      end
    end

    def execute(state)
      ctx = TaskContext.new(self, state)
      state.thread = Thread.current
      handler = @lock.synchronize { @handlers[state.handler_name] }
      output = nil
      error = nil
      begin
        raise NonRetryableError, "no handler registered for #{state.handler_name.inspect}" unless handler

        output = handler.call(ctx)
      rescue StandardError, ScriptError => e
        error = e
      end
      settle(state, output, error)
    rescue StandardError => e
      logger.error("task #{state.id}: internal error: #{describe(e)}")
    ensure
      finish(state)
    end

    def settle(state, output, error)
      if state.lost? || state.abandoned? || error.is_a?(LeaseLostError)
        logger.info("task #{state.id}: lease lost (#{state.cancel_reason}); not acknowledging")
        return
      end
      return unless state.try_settle!

      if error
        retryable = retryable_error?(error)
        logger.info("task #{state.id} (#{state.handler_name}) failed: #{describe(error)} retryable=#{retryable}")
        acknowledge(state, "fail", "message" => error_message(error), "retryable" => retryable)
      else
        acknowledge(state, "complete", "output" => normalize_output(output))
      end
    end

    def finish(state)
      @lock.synchronize do
        @in_flight.delete(state.key)
        @free += 1
        @slot_cv.broadcast
        @drain_cv.broadcast
      end
    end

    def retryable_error?(error)
      case error
      when NonRetryableError then false
      when RetryableError then true
      else true # F4: any other uncaught exception is transient
      end
    end

    def error_message(error)
      msg = error.message.to_s
      msg = error.class.name if msg.empty?
      msg.bytesize > MAX_MESSAGE_BYTES ? "#{msg.byteslice(0, MAX_MESSAGE_BYTES).scrub}..." : msg
    end

    def normalize_output(output)
      case output
      when nil then {}
      when Resource then output.to_h
      else Client::Util.plain(output)
      end
    end

    # complete/fail with retry on transport errors and retryable statuses,
    # resending the identical body (K3/F5). 404/409 = settled elsewhere (L3).
    def acknowledge(state, kind, fields)
      body = { "worker_id" => @worker_id, "claim_epoch" => state.claim_epoch }.merge(fields)
      attempt = 0
      begin
        attempt += 1
        @transport.request("POST", "#{task_path(state)}/#{kind}", body: body)
        state.acked!
        true
      rescue NotFoundError, ConflictError => e
        state.lose!
        logger.warn("task #{state.id}: #{kind} rejected with #{e.status} (#{e.message}); lease lost, not retrying")
        false
      rescue TransportError, APIError => e
        if e.retryable? && attempt < @ack_attempts && !state.abandoned?
          delay = [0.2 * (2**(attempt - 1)), 5.0].min
          logger.warn("task #{state.id}: #{kind} failed (#{describe(e)}); retry #{attempt} in #{delay}s")
          sleep(delay)
          retry
        end
        logger.error("task #{state.id}: #{kind} failed permanently (#{describe(e)}); leaving it for lease recovery")
        false
      end
    end

    # ------------------------------------------------------------------
    # Heartbeats, checkpoints, timeouts
    # ------------------------------------------------------------------

    def heartbeat_loop
      loop do
        due = []
        timed_out = []
        @lock.synchronize do
          return if @heartbeat_stopped

          now = monotonic
          wall = Time.now
          @in_flight.each_value do |s|
            next if s.lost? || s.acked? || s.abandoned?

            timed_out << s if s.deadline && wall >= s.deadline && !s.cancelled?
            next if s.heartbeat_pending || s.next_heartbeat_at > now

            s.heartbeat_pending = true
            s.next_heartbeat_at = now + @heartbeat_interval
            due << s
          end
        end
        timed_out.each { |s| spawn_thread("orch8-timeout") { handle_timeout(s) } }
        due.each do |s|
          spawn_thread("orch8-hb") do
            send_heartbeat(s)
          rescue StandardError => e
            logger.warn("task #{s.id}: heartbeat failed (#{describe(e)})")
          ensure
            s.heartbeat_pending = false
          end
        end
        @lock.synchronize do
          return if @heartbeat_stopped

          next_due = @in_flight.values.map(&:next_heartbeat_at).min
          wait = next_due ? next_due - monotonic : 0.25
          @hb_cv.wait(@lock, wait.clamp(0.01, 0.25))
        end
      end
    end

    def send_heartbeat(state)
      return if state.lost? || state.acked?

      res = mutate(state, "heartbeat", {})
      state.touch!(monotonic + heartbeat_interval)
      res
    end

    def write_checkpoint(state, value)
      raise LeaseLostError, "lease lost for task #{state.id} (#{state.cancel_reason})" if state.lost?

      state.checkpoint_lock.synchronize do
        body = { "checkpoint" => Client::Util.plain(value), "checkpoint_seq" => state.seq }
        res = begin
          mutate(state, "heartbeat", body)
        rescue TransportError
          # Ambiguous failure: retry once with the same seq (WORKER_PROTOCOL §9);
          # a 409 then is treated as lease loss.
          mutate(state, "heartbeat", body)
        end
        seq = res.is_a?(Hash) ? res["checkpoint_seq"] : nil
        state.seq = seq.is_a?(Integer) ? seq : state.seq + 1
        state.touch!(monotonic + heartbeat_interval)
        state.seq
      end
    end

    # Heartbeat/checkpoint request; 404/409 marks the lease lost (L2).
    def mutate(state, kind, extra)
      body = { "worker_id" => @worker_id, "claim_epoch" => state.claim_epoch }.merge(extra)
      @transport.request("POST", "#{task_path(state)}/#{kind}", body: body)
    rescue NotFoundError, ConflictError => e
      state.lose!
      logger.warn("task #{state.id}: #{kind} rejected with #{e.status} (#{e.message}); lease lost")
      raise LeaseLostError, "lease lost for task #{state.id}: #{e.status} #{e.message}"
    end

    def handle_timeout(state)
      return if state.cancelled?

      state.cancel!(:timeout)
      return if state.lost? || !state.try_settle!

      logger.warn("task #{state.id}: timeout_ms=#{state.task['timeout_ms']} exceeded; failing (retryable)")
      acknowledge(state, "fail", "message" => "task timed out locally (timeout_ms=#{state.task['timeout_ms']})",
                                 "retryable" => true)
    rescue StandardError => e
      logger.error("task #{state.id}: timeout handling failed: #{describe(e)}")
    end

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def task_path(state) = "/workers/tasks/#{Transport.escape_segment(state.id)}"

    def spawn_thread(name, &block)
      Thread.new do
        Thread.current.name = name
        Thread.current.report_on_exception = false
        block.call
      end
    end

    def describe(error) = "#{error.class}: #{error.message}"

    def monotonic = Process.clock_gettime(Process::CLOCK_MONOTONIC)

    def install_traps(signals)
      signals.to_h do |sig|
        # Trap context: no mutexes allowed, so only write a byte to the pipe.
        [sig, Signal.trap(sig) { wake_runner }]
      end
    end

    def restore_traps(previous)
      previous.each { |sig, handler| Signal.trap(sig, handler || "DEFAULT") }
    end

    def wake_runner
      @wake_w.write_nonblock(".", exception: false)
    rescue IOError
      nil
    end

    def wait_for_wakeup
      @wake_r.read(1)
    rescue IOError
      nil
    end

    # Per-task bookkeeping shared by the executor, heartbeat and timeout paths.
    class TaskState
      attr_reader :task, :checkpoint_lock, :deadline
      attr_accessor :seq, :next_heartbeat_at, :heartbeat_pending, :thread

      def initialize(task, _worker)
        @task = task
        @seq = task["checkpoint_seq"].is_a?(Integer) ? task["checkpoint_seq"] : 0
        @lock = Mutex.new
        @cv = ConditionVariable.new
        @checkpoint_lock = Mutex.new
        @cancel_reason = nil
        @lost = false
        @settled = false
        @acked = false
        @abandoned = false
        @heartbeat_pending = false
        @next_heartbeat_at = 0
        @deadline = compute_deadline(task)
      end

      def id = @task["id"]
      def key = @task["id"].to_s
      def handler_name = @task["handler_name"].to_s
      def claim_epoch = @task["claim_epoch"]

      def lost? = @lost
      def acked? = @acked
      def abandoned? = @abandoned
      def cancelled? = !@cancel_reason.nil?
      def cancel_reason = @cancel_reason

      def lose!
        @lost = true
        cancel!(:lease_lost)
      end

      def abandon!
        @abandoned = true
        cancel!(:shutdown)
      end

      def acked! = (@acked = true)

      def touch!(next_at)
        @next_heartbeat_at = next_at
      end

      def cancel!(reason)
        @lock.synchronize do
          @cancel_reason ||= reason
          @cv.broadcast
        end
      end

      # Exactly one of {executor, timeout path} gets to acknowledge the claim.
      def try_settle!
        @lock.synchronize do
          return false if @settled

          @settled = true
        end
      end

      def wait_cancelled(seconds)
        deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + seconds.to_f
        @lock.synchronize do
          loop do
            return false if @cancel_reason

            remaining = deadline - Process.clock_gettime(Process::CLOCK_MONOTONIC)
            return true if remaining <= 0

            @cv.wait(@lock, remaining)
          end
        end
      end

      private

      def compute_deadline(task)
        timeout_ms = task["timeout_ms"]
        return nil unless timeout_ms.is_a?(Numeric) && timeout_ms.positive?

        base = begin
          task["created_at"] ? Time.iso8601(task["created_at"].to_s) : Time.now
        rescue ArgumentError
          Time.now
        end
        base + (timeout_ms / 1000.0)
      end
    end
  end
end

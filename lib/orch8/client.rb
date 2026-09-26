# frozen_string_literal: true

require "time"

module Orch8
  # Typed REST client for the Orch8 engine.
  #
  #   client = Orch8::Client.new(base_url: "http://localhost:8080/api/v1",
  #                              api_key: ENV["ORCH8_API_KEY"], tenant_id: "acme")
  #   seq  = client.sequences.create(name: "onboarding", namespace: "default", tenant_id: "acme", blocks: [...])
  #   inst = client.instances.create(sequence_id: seq.id, tenant_id: "acme", namespace: "default")
  #   job  = client.jobs.enqueue("send_email", { to: "a@example.com" }, queue: "emails")
  #
  # All methods return Orch8::Resource objects (or arrays of them) and raise
  # Orch8::APIError subclasses / Orch8::TransportError on failure.
  class Client
    attr_reader :transport

    # @param base_url [String] the `/api/v1` base URL (default: ENV["ORCH8_BASE_URL"])
    # @param api_key [String, nil] sent as `x-api-key` (default: ENV["ORCH8_API_KEY"])
    # @param tenant_id [String, nil] sent as `x-tenant-id` (default: ENV["ORCH8_TENANT_ID"])
    # @param max_attempts [Integer] attempts for safe (GET/HEAD) requests
    # @param retry_base_delay [Float] seconds, base of the exponential backoff
    def initialize(base_url: ENV.fetch("ORCH8_BASE_URL", nil), api_key: ENV.fetch("ORCH8_API_KEY", nil),
                   tenant_id: ENV.fetch("ORCH8_TENANT_ID", nil), transport: nil, **options)
      @transport = transport || Transport.new(base_url: base_url, api_key: api_key, tenant_id: tenant_id, **options)
    end

    def tenant_id = @transport.tenant_id

    def sequences = (@sequences ||= Sequences.new(@transport))
    def instances = (@instances ||= Instances.new(@transport))
    def jobs = (@jobs ||= Jobs.new(@transport))

    # Escape hatch for endpoints without a typed wrapper.
    def request(method, path, query: nil, body: nil)
      Resource.wrap(@transport.request(method, path, query: query, body: body))
    end

    module Util
      module_function

      def seg(id) = Transport.escape_segment(id)

      def compact(hash) = hash.each_with_object({}) { |(k, v), out| out[k.to_s] = v unless v.nil? }

      def plain(value)
        case value
        when Resource then value.to_h
        when Hash then value.each_with_object({}) { |(k, v), out| out[k.to_s] = plain(v) }
        when Array then value.map { |v| plain(v) }
        when Symbol then value.to_s
        else value
        end
      end

      def rfc3339(value)
        case value
        when nil then nil
        when String then value
        when Time then value.utc.iso8601(3).sub(/\.000Z\z/, "Z")
        else value.respond_to?(:to_time) ? rfc3339(value.to_time) : value.to_s
        end
      end

      # Accepts a bare array, or a page object {"items": [...]} / {"jobs": [...]}.
      def list_items(data, *keys)
        return data if data.is_a?(Array)
        return [] if data.nil?

        keys.each { |k| return data[k] if data.is_a?(Hash) && data[k].is_a?(Array) }
        data.is_a?(Hash) && data["items"].is_a?(Array) ? data["items"] : []
      end
    end

    # /sequences
    class Sequences
      include Util

      def initialize(transport) = (@t = transport)

      # POST /sequences. Returns {id, warnings?}.
      def create(definition = nil, **fields)
        Resource.wrap(@t.post("/sequences", plain(definition || fields)))
      end

      def get(id) = Resource.wrap(@t.get("/sequences/#{seg(id)}"))

      # GET /sequences?tenant_id&namespace&limit&offset
      def list(query = nil, **params)
        Resource.wrap(list_items(@t.get("/sequences", query: plain(query || params)), "sequences"))
      end
    end

    # /instances
    class Instances
      include Util

      SIGNALS = %w[pause resume cancel update_context].freeze

      def initialize(transport) = (@t = transport)

      # POST /instances. Body: {sequence_id, tenant_id, namespace, context?, idempotency_key?, ...}.
      # Returns {id, deduplicated?} (201 created or 200 on an idempotent replay).
      def create(request = nil, **fields)
        Resource.wrap(@t.post("/instances", compact(plain(request || fields))))
      end

      def get(id) = Resource.wrap(@t.get("/instances/#{seg(id)}"))

      # GET /instances?tenant_id&namespace&sequence_id&state&limit&offset
      def list(query = nil, **params)
        Resource.wrap(list_items(@t.get("/instances", query: plain(query || params)), "instances"))
      end

      # POST /instances/{id}/signals. `signal_type` is one of "pause", "resume",
      # "cancel", "update_context", or {"custom" => "name"} (a Symbol or a
      # {custom: name} Hash also work). Returns {signal_id}.
      def signal(id, signal_type, payload = nil)
        body = { "signal_type" => normalize_signal(signal_type) }
        body["payload"] = plain(payload) unless payload.nil?
        Resource.wrap(@t.post("/instances/#{seg(id)}/signals", body))
      end

      def cancel(id) = signal(id, "cancel")
      def pause(id) = signal(id, "pause")
      def resume(id) = signal(id, "resume")
      def update_context(id, payload) = signal(id, "update_context", payload)
      def send_custom(id, name, payload = nil) = signal(id, { "custom" => name.to_s }, payload)

      private

      def normalize_signal(type)
        case type
        when Symbol, String then type.to_s
        when Hash then plain(type)
        else raise ArgumentError, "signal_type must be a String, Symbol or {custom: name}"
        end
      end
    end

    # /jobs — single durable handler invocations with retries, delay and idempotency.
    class Jobs
      include Util

      def initialize(transport) = (@t = transport)

      # POST /jobs. Unset optional fields are omitted from the body.
      #
      #   client.jobs.enqueue("send_email", { to: "a@b.c" }, queue: "emails", priority: 5,
      #                       retry: { max_attempts: 4, initial_backoff_ms: 500 },
      #                       delay_ms: 250, idempotency_key: "welcome-a")
      #
      # Also accepts a single Hash body: `enqueue(handler: "x", payload: {...})`.
      def enqueue(handler = nil, payload = nil, **opts)
        handler, opts = opts, {} if handler.nil? && opts.key?(:handler)
        if handler.is_a?(Hash)
          body = compact(plain(handler))
          body["run_at"] = rfc3339(handler[:run_at] || handler["run_at"]) if body.key?("run_at")
        else
          delay_ms = opts.delete(:delay_ms)
          run_at = opts.delete(:run_at)
          retry_policy = opts.delete(:retry)
          raise ArgumentError, "pass either delay_ms or run_at, not both" if delay_ms && run_at
          raise ArgumentError, "delay_ms must be >= 0" if delay_ms&.negative?

          body = compact(
            "handler" => handler.to_s, "payload" => plain(payload.nil? ? {} : payload),
            "queue" => opts.delete(:queue)&.to_s, "priority" => opts.delete(:priority),
            "retry" => retry_policy && compact(plain(retry_policy)),
            "delay_ms" => delay_ms&.round, "run_at" => rfc3339(run_at),
            "idempotency_key" => opts.delete(:idempotency_key), "metadata" => plain(opts.delete(:metadata))
          ).merge(compact(plain(opts)))
        end
        raise ArgumentError, "handler is required" if body["handler"].nil? || body["handler"].to_s.empty?

        body["payload"] = {} unless body.key?("payload")
        JobInfo.wrap(@t.post("/jobs", body))
      end

      def get(id) = JobInfo.wrap(@t.get("/jobs/#{seg(id)}"))

      # GET /jobs?handler&status&limit&cursor. Accepts a bare array or a page
      # object ({"items": [...]} / {"jobs": [...]}); returns an Array.
      def list(query = nil, **params)
        JobInfo.wrap(list_items(@t.get("/jobs", query: plain(query || params)), "jobs"))
      end

      # DELETE /jobs/{id}. Returns the cancelled job, or nil for a 204/empty body.
      def cancel(id)
        data = @t.delete("/jobs/#{seg(id)}")
        data.is_a?(Hash) ? JobInfo.wrap(data) : nil
      end

      # Polls until the job reaches a terminal status (or `timeout` seconds pass).
      def wait_for(id, timeout: 60, interval: 0.5, max_interval: 5.0)
        deadline = timeout && (Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout)
        loop do
          job = get(id)
          return job if job.done?

          now = Process.clock_gettime(Process::CLOCK_MONOTONIC)
          raise Error, "job #{id} still #{job['status']} after #{timeout}s" if deadline && now >= deadline

          sleep(deadline ? [interval, deadline - now].min : interval)
          interval = [interval * 1.5, max_interval].min
        end
      end
    end
  end
end

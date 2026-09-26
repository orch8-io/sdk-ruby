# frozen_string_literal: true

require "json"
require "openssl"

module Orch8
  # Push-dispatch support (WORKER_PROTOCOL §8).
  #
  # The engine POSTs a small JSON envelope to your `push_url` with
  #   X-Orch8-Timestamp: <unix seconds>
  #   X-Orch8-Signature: sha256=<hex(HMAC-SHA256(secret, "<ts>." + raw_body))>
  # A push is only a wake-up: the receiver verifies it, answers 202 quickly
  # and then claims via POST /workers/tasks/poll/queue (Worker#claim).
  module Push
    DEFAULT_TOLERANCE = 300
    SIGNATURE_PREFIX = "sha256="
    TIMESTAMP_HEADER = "X-Orch8-Timestamp"
    SIGNATURE_HEADER = "X-Orch8-Signature"

    module_function

    # @return [String] the header value "sha256=<hex>" for a timestamp and raw body
    def sign(secret:, timestamp:, body:)
      raise ConfigurationError, "push secret must not be empty" if secret.nil? || secret.to_s.empty?

      "#{SIGNATURE_PREFIX}#{OpenSSL::HMAC.hexdigest('SHA256', secret.to_s, "#{timestamp}.".b + body.to_s.b)}"
    end

    # Verifies a push signature over the exact raw body bytes (S1-S3).
    #
    # @param secret [String] the queue's push secret (must not be empty)
    # @param timestamp [String, nil] X-Orch8-Timestamp header value
    # @param signature [String, nil] X-Orch8-Signature header value ("sha256=<64 hex>")
    # @param body [String] the raw request body (never re-serialized JSON)
    # @param now [Integer, Time, nil] clock override (Unix seconds)
    # @param tolerance [Integer] allowed clock skew in seconds, both directions
    # @return [Boolean]
    def verify(secret:, timestamp:, signature:, body:, now: nil, tolerance: DEFAULT_TOLERANCE)
      verify!(secret: secret, timestamp: timestamp, signature: signature, body: body, now: now, tolerance: tolerance)
      true
    rescue SignatureVerificationError
      false
    end

    # Like #verify but raises Orch8::SignatureVerificationError with a reason.
    def verify!(secret:, timestamp:, signature:, body:, now: nil, tolerance: DEFAULT_TOLERANCE)
      raise ConfigurationError, "push secret must not be empty" if secret.nil? || secret.to_s.empty?

      ts = timestamp.is_a?(Integer) ? timestamp.to_s : timestamp
      fail!("missing or non-integer timestamp") unless ts.is_a?(String) && ts.match?(/\A-?\d+\z/)
      fail!("missing signature") unless signature.is_a?(String)
      fail!("signature must start with #{SIGNATURE_PREFIX}") unless signature.start_with?(SIGNATURE_PREFIX)

      hex = signature.delete_prefix(SIGNATURE_PREFIX)
      fail!("signature must be 64 hex digits") unless hex.match?(/\A[0-9a-fA-F]{64}\z/)

      current = case now
                when nil then Time.now.to_i
                when Time then now.to_i
                else Integer(now)
                end
      fail!("timestamp outside the #{tolerance}s tolerance window") if (current - Integer(ts, 10)).abs > tolerance.to_i

      expected = OpenSSL::HMAC.hexdigest("SHA256", secret.to_s, "#{ts}.".b + body.to_s.b)
      fail!("signature mismatch") unless secure_compare(expected, hex.downcase)
      true
    end

    # Constant-time comparison of two equal-length strings.
    def secure_compare(a, b)
      return false unless a.bytesize == b.bytesize

      if OpenSSL.respond_to?(:fixed_length_secure_compare)
        OpenSSL.fixed_length_secure_compare(a, b)
      else
        result = 0
        a.bytes.zip(b.bytes) { |x, y| result |= x ^ y }
        result.zero?
      end
    end

    def fail!(reason)
      raise SignatureVerificationError, reason
    end
    private_class_method :fail!

    # The push request body. Unknown fields stay reachable through #raw.
    Envelope = Struct.new(:task_id, :instance_id, :block_id, :handler_name, :queue_name,
                          :params, :context, :attempt, :timeout_ms, :raw, keyword_init: true) do
      def self.parse(body)
        data = body.is_a?(Hash) ? body : JSON.parse(body.to_s)
        raise ArgumentError, "push envelope must be a JSON object" unless data.is_a?(Hash)

        data = data.transform_keys(&:to_s)
        new(**members.reject { |m| m == :raw }.to_h { |m| [m, data[m.to_s]] }, raw: data)
      end
    end

    # A tiny Rack application (no Rack dependency required) that verifies
    # pushes and triggers a queue claim on a push-only worker.
    #
    #   worker = Orch8::Worker.new(...).tap { |w| w.register("render") { ... } }
    #   worker.start(poll: false)
    #   run Orch8::Push::Receiver.new(worker: worker, secret: ENV["ORCH8_PUSH_SECRET"])  # config.ru
    #
    # Invalid signature -> 401 and no work. Valid -> 202, then (on a
    # background thread) Worker#claim(queue_name:, handler_name:, limit: 1).
    class Receiver
      # @param on_push [#call, nil] custom action instead of a worker claim; receives the Envelope
      def initialize(secret:, worker: nil, tolerance: DEFAULT_TOLERANCE, on_push: nil, logger: nil, async: true)
        raise ConfigurationError, "push secret must not be empty" if secret.nil? || secret.to_s.empty?
        raise ArgumentError, "pass worker: or on_push:" if worker.nil? && on_push.nil?

        @secret = secret.to_s
        @worker = worker
        @tolerance = tolerance
        @on_push = on_push
        @logger = logger || worker&.logger || SimpleLogger.new
        @async = async
      end

      # Rack entry point.
      def call(env)
        return respond(405) unless env["REQUEST_METHOD"] == "POST"

        body = read_body(env["rack.input"])
        status, envelope = handle(timestamp: env["HTTP_X_ORCH8_TIMESTAMP"], signature: env["HTTP_X_ORCH8_SIGNATURE"],
                                  body: body)
        respond(status).tap { dispatch(envelope) if envelope }
      end

      # Framework-agnostic entry point: returns [status, envelope-or-nil]
      # without doing any work. Call #dispatch(envelope) after responding.
      def handle(timestamp:, signature:, body:)
        unless Push.verify(secret: @secret, timestamp: timestamp, signature: signature, body: body,
                           tolerance: @tolerance)
          return [401, nil]
        end

        [202, Envelope.parse(body)]
      rescue JSON::ParserError, ArgumentError
        [400, nil]
      end

      # Claims (or runs on_push) for a verified envelope.
      def dispatch(envelope)
        work = lambda do
          if @on_push
            @on_push.call(envelope)
          else
            @worker.claim(handler_name: envelope.handler_name, queue_name: envelope.queue_name, limit: 1)
          end
        rescue StandardError => e
          @logger.warn("push claim for #{envelope.handler_name}/#{envelope.queue_name} failed: #{e.class}: #{e.message}")
        end
        @async ? Thread.new(&work) : work.call
      end

      private

      def read_body(input)
        return "".b if input.nil?

        input.rewind if input.respond_to?(:rewind)
        data = input.read.to_s.b
        input.rewind if input.respond_to?(:rewind)
        data
      end

      def respond(status)
        [status, { "content-type" => "text/plain" }, [status == 202 ? "accepted" : ""]]
      end
    end
  end
end

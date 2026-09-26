# frozen_string_literal: true

module Orch8
  # Base class for every error raised by this gem.
  class Error < StandardError; end

  # Invalid SDK configuration (missing base URL, empty push secret, ...).
  class ConfigurationError < Error; end

  # A request could not be built (e.g. a protocol-relative path).
  class InvalidRequestError < Error; end

  # The engine could not be reached (DNS, refused connection, timeout, reset).
  class TransportError < Error
    attr_reader :cause_error

    def initialize(message = "transport error", cause_error: nil)
      super(message)
      @cause_error = cause_error
    end

    def kind = "transport"
    def retryable? = true
  end

  # An HTTP error response from the engine, parsed from the error envelope
  # `{"error": {"code", "message", "request_id", "details"}}` (WORKER_PROTOCOL T7).
  class APIError < Error
    RETRYABLE_STATUSES = [408, 425, 429, 500, 502, 503, 504].freeze

    attr_reader :status, :code, :request_id, :details, :body, :headers

    def initialize(message = nil, status: nil, code: nil, request_id: nil, details: nil, body: nil, headers: {})
      @status = status
      @code = code
      @request_id = request_id
      @details = details
      @body = body
      @headers = headers || {}
      super(message || "HTTP #{status}")
    end

    # Stable, language-neutral error kind (see conformance README).
    def kind = "api"

    def retryable? = RETRYABLE_STATUSES.include?(status)

    # Builds the most specific subclass for an HTTP status and response body.
    def self.from_response(status, body, headers = {})
      env = body.is_a?(Hash) && body["error"].is_a?(Hash) ? body["error"] : {}
      message = env["message"]
      message = "HTTP #{status}" if message.nil? || message.to_s.empty?
      klass = STATUS_CLASSES.fetch(status) { status >= 500 ? ServerError : APIError }
      klass.new(message.to_s, status: status, code: env["code"], request_id: env["request_id"],
                               details: env["details"], body: body, headers: headers)
    end
  end

  class BadRequestError < APIError
    def kind = "invalid_argument"
  end

  class UnauthorizedError < APIError
    def kind = "unauthorized"
  end

  class ForbiddenError < APIError
    def kind = "forbidden"
  end

  class NotFoundError < APIError
    def kind = "not_found"
  end

  class ConflictError < APIError
    def kind = "conflict"
  end

  class PayloadTooLargeError < APIError
    def kind = "payload_too_large"
  end

  class UnprocessableEntityError < APIError
    def kind = "unprocessable"
  end

  class RateLimitedError < APIError
    def kind = "rate_limited"
  end

  class ServerError < APIError
    def kind = "server"
  end

  APIError::STATUS_CLASSES = {
    400 => BadRequestError,
    401 => UnauthorizedError,
    403 => ForbiddenError,
    404 => NotFoundError,
    409 => ConflictError,
    413 => PayloadTooLargeError,
    422 => UnprocessableEntityError,
    429 => RateLimitedError
  }.freeze

  # Raise from a handler to fail the task with `retryable: true`; the engine
  # schedules a retry if the step's retry policy allows it.
  class RetryableError < Error
    def retryable? = true
  end

  # Raise from a handler to fail the task permanently (`retryable: false`).
  class NonRetryableError < Error
    def retryable? = false
  end
  PermanentError = NonRetryableError

  # The worker no longer owns the task (404/409 on a mutation, WORKER_PROTOCOL §5).
  # Raised by TaskContext#checkpoint; the worker never acknowledges such a task.
  class LeaseLostError < Error; end

  # Raised by TaskContext#check_cancelled! once the task has been cancelled
  # (lease loss, local timeout, or forced shutdown).
  class TaskCancelledError < Error
    attr_reader :reason

    def initialize(reason = :cancelled)
      @reason = reason
      super("task cancelled (#{reason})")
    end
  end

  # Raised by Orch8::Push.verify! when a push signature does not verify.
  class SignatureVerificationError < Error; end
end

# frozen_string_literal: true

require "json"
require "openssl"
require "time"
require "net/http"
require "uri"

module Orch8
  # Low-level JSON-over-HTTP transport shared by Client and Worker.
  #
  # * sends `x-api-key` / `x-tenant-id` on every request (WORKER_PROTOCOL T3/T4);
  # * retries safe methods (GET/HEAD) on 408/425/429/5xx and transport errors
  #   with exponential backoff; never replays unsafe methods
  #   (sdk-contract/fixtures/transport.json);
  # * maps error responses to typed Orch8::APIError subclasses (T7).
  class Transport
    SAFE_METHODS = %w[GET HEAD].freeze
    METHODS = {
      "GET" => Net::HTTP::Get, "HEAD" => Net::HTTP::Head, "POST" => Net::HTTP::Post,
      "PUT" => Net::HTTP::Put, "PATCH" => Net::HTTP::Patch, "DELETE" => Net::HTTP::Delete
    }.freeze
    NETWORK_ERRORS = [
      IOError, EOFError, SocketError, SystemCallError, Timeout::Error,
      Net::OpenTimeout, Net::ReadTimeout, Net::HTTPBadResponse, OpenSSL::SSL::SSLError
    ].freeze

    attr_reader :base_url, :tenant_id, :max_attempts, :retry_base_delay

    # @param base_url [String] the `/api/v1` base, e.g. "http://localhost:8080/api/v1"
    # @param max_attempts [Integer] total attempts for safe requests
    # @param retry_base_delay [Float] seconds; attempt n waits base * 2**(n-1)
    def initialize(base_url:, api_key: nil, tenant_id: nil, timeout: 30, open_timeout: 10,
                   max_attempts: 3, retry_base_delay: 0.25, max_retry_delay: 10.0,
                   headers: {}, user_agent: nil)
      raise ConfigurationError, "base_url is required" if base_url.nil? || base_url.to_s.empty?

      @base_url = base_url.to_s.sub(%r{/+\z}, "")
      @uri = begin
        URI.parse(@base_url)
      rescue URI::InvalidURIError
        nil
      end
      unless @uri.is_a?(URI::HTTP) && @uri.host
        raise ConfigurationError, "base_url must be an absolute http(s) URL, got #{base_url.inspect}"
      end

      @api_key = api_key
      @tenant_id = tenant_id
      @timeout = timeout
      @open_timeout = open_timeout
      @max_attempts = [max_attempts.to_i, 1].max
      @retry_base_delay = retry_base_delay.to_f
      @max_retry_delay = max_retry_delay.to_f
      @extra_headers = headers.transform_keys(&:to_s)
      @user_agent = user_agent || "orch8-ruby/#{VERSION} ruby/#{RUBY_VERSION}"
    end

    # Performs a request and returns the parsed JSON body (nil for an empty body).
    #
    # @param path [String] path relative to the base URL, starting with "/"
    # @param query [Hash, nil] query parameters; nil values are dropped
    # @param body [Object, nil] JSON-serializable body (omitted when nil)
    # @param retry_safe [Boolean] set false to disable retries for safe methods
    def request(method, path, query: nil, body: nil, headers: {}, retry_safe: true)
      method = method.to_s.upcase
      klass = METHODS.fetch(method) { raise InvalidRequestError, "unsupported HTTP method #{method}" }
      target = build_target(path, query)
      payload = body.nil? ? nil : JSON.generate(body)
      attempts = retry_safe && SAFE_METHODS.include?(method) ? @max_attempts : 1

      attempt = 0
      begin
        attempt += 1
        perform(klass, target, payload, headers)
      rescue TransportError, APIError => e
        raise if attempt >= attempts || !e.retryable?

        sleep(backoff(attempt))
        retry
      end
    end

    def get(path, query: nil) = request("GET", path, query: query)
    def post(path, body = nil) = request("POST", path, body: body)
    def delete(path) = request("DELETE", path)

    # Percent-encodes one path segment ("a/b c" -> "a%2Fb%20c").
    def self.escape_segment(value)
      URI.encode_www_form_component(value.to_s).gsub("+", "%20")
    end

    def self.encode_query(query)
      pairs = query.to_h.filter_map do |k, v|
        next if v.nil?

        [k.to_s, v.is_a?(Time) ? v.utc.iso8601 : v.to_s]
      end
      URI.encode_www_form(pairs)
    end

    private

    def backoff(attempt)
      [@retry_base_delay * (2**(attempt - 1)), @max_retry_delay].min
    end

    def build_target(path, query)
      path = path.to_s
      if !path.start_with?("/") || path.start_with?("//") || path.match?(%r{\A/*[a-z][a-z0-9+.-]*:}i)
        raise InvalidRequestError, "invalid_path: request path must be relative to the base URL (#{path.inspect})"
      end

      qs = query && !query.empty? ? self.class.encode_query(query) : ""
      target = "#{@uri.path}#{path}"
      qs.empty? ? target : "#{target}?#{qs}"
    end

    def perform(klass, target, payload, headers)
      req = klass.new(target)
      req["accept"] = "application/json"
      req["user-agent"] = @user_agent
      req["x-api-key"] = @api_key if @api_key && !@api_key.to_s.empty?
      req["x-tenant-id"] = @tenant_id if @tenant_id && !@tenant_id.to_s.empty?
      @extra_headers.each { |k, v| req[k] = v }
      headers.each { |k, v| req[k.to_s] = v }
      if payload
        req["content-type"] = "application/json"
        req.body = payload
      end

      res = begin
        http = Net::HTTP.new(@uri.host, @uri.port)
        http.use_ssl = @uri.scheme == "https"
        http.open_timeout = @open_timeout
        http.read_timeout = @timeout
        http.write_timeout = @timeout if http.respond_to?(:write_timeout=)
        http.start { |conn| conn.request(req) }
      rescue *NETWORK_ERRORS => e
        raise TransportError.new("#{e.class}: #{e.message}", cause_error: e)
      end

      status = res.code.to_i
      parsed = parse_body(res.body)
      raise APIError.from_response(status, parsed, res.to_hash) if status >= 400

      parsed
    end

    def parse_body(text)
      return nil if text.nil? || text.strip.empty?

      JSON.parse(text)
    rescue JSON::ParserError
      text
    end
  end
end

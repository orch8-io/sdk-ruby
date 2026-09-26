# frozen_string_literal: true

require "json"
require "socket"
require "uri"

# Minimal threaded HTTP/1.1 server for tests (stdlib only; WEBrick is not a
# default gem on Ruby >= 3.0). Routes are matched on method + path with
# `:param` segments; handlers return [status, body_hash_or_nil, headers].
class FakeServer
  Request = Struct.new(:method, :raw_path, :path, :query, :headers, :raw_body, :body, :params, :at,
                       keyword_init: true)

  attr_reader :requests, :port

  def initialize
    @server = TCPServer.new("127.0.0.1", 0)
    @port = @server.addr[1]
    @routes = []
    @requests = []
    @lock = Mutex.new
    @thread = Thread.new { accept_loop }
  end

  def base_url(prefix = "/api/v1") = "http://127.0.0.1:#{@port}#{prefix}"

  def route(method, pattern, &handler)
    @lock.synchronize { @routes.unshift([method, pattern, handler]) }
    self
  end

  def find(method, path = nil)
    @lock.synchronize { @requests.select { |r| r.method == method && (path.nil? || r.path == path) } }
  end

  def all = @lock.synchronize { @requests.dup }

  def wait_for(timeout = 5)
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout
    until yield
      raise "timed out waiting for condition" if Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline

      sleep 0.01
    end
  end

  def close
    @server.close
    @thread.join(1)
  rescue IOError
    nil
  end

  private

  def accept_loop
    loop do
      sock = @server.accept
      Thread.new(sock) { |s| handle(s) }
    end
  rescue IOError, Errno::EBADF
    nil
  end

  def handle(sock)
    line = sock.gets("\r\n") or return
    method, raw_target, = line.split(" ", 3)
    headers = {}
    while (h = sock.gets("\r\n")) && h != "\r\n"
      k, v = h.split(":", 2)
      headers[k.strip.downcase] = v.to_s.strip
    end
    raw_body = headers["content-length"] ? sock.read(headers["content-length"].to_i) : ""
    raw_path, qs = raw_target.split("?", 2)
    path = raw_path.delete_prefix("/api/v1")
    query = qs ? URI.decode_www_form(qs).to_h : {}
    body = raw_body.to_s.empty? ? nil : (JSON.parse(raw_body) rescue nil) # rubocop:disable Style/RescueModifier
    req = Request.new(method: method, raw_path: raw_path, path: path, query: query, headers: headers,
                      raw_body: raw_body, body: body, params: {}, at: Process.clock_gettime(Process::CLOCK_MONOTONIC))
    @lock.synchronize { @requests << req }
    status, resp, resp_headers = dispatch(req)
    payload = resp.nil? ? "" : JSON.generate(resp)
    out = +"HTTP/1.1 #{status} X\r\ncontent-type: application/json\r\ncontent-length: #{payload.bytesize}\r\nconnection: close\r\n"
    (resp_headers || {}).each { |k, v| out << "#{k}: #{v}\r\n" }
    sock.write("#{out}\r\n#{payload}")
  rescue StandardError => e
    warn "fake server error: #{e.class}: #{e.message}\n#{e.backtrace.first(3).join("\n")}"
  ensure
    sock.close
  end

  def dispatch(req)
    routes = @lock.synchronize { @routes.dup }
    routes.each do |method, pattern, handler|
      next unless method == req.method

      params = match(pattern, req.path) or next
      req.params = params
      return handler.call(req)
    end
    [404, { "error" => { "code" => "not_found", "message" => "no route #{req.method} #{req.path}" } }]
  end

  def match(pattern, path)
    pp = pattern.split("/")
    ap = path.split("/", -1)
    return nil unless pp.size == ap.size

    params = {}
    pp.zip(ap).each do |p, a|
      if p.start_with?(":")
        params[p[1..]] = URI.decode_www_form_component(a)
      elsif p != a
        return nil
      end
    end
    params
  end
end

def error_body(code, message) = { "error" => { "code" => code, "message" => message, "request_id" => nil } }

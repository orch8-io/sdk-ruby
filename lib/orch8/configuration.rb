# frozen_string_literal: true

module Orch8
  # Global defaults used by Orch8.client (and therefore Orch8::Job).
  class Configuration
    attr_accessor :base_url, :api_key, :tenant_id, :max_attempts, :retry_base_delay, :timeout

    def initialize
      @base_url = ENV.fetch("ORCH8_BASE_URL", nil)
      @api_key = ENV.fetch("ORCH8_API_KEY", nil)
      @tenant_id = ENV.fetch("ORCH8_TENANT_ID", nil)
      @max_attempts = 3
      @retry_base_delay = 0.25
      @timeout = 30
    end

    def client_options
      { base_url: base_url, api_key: api_key, tenant_id: tenant_id, max_attempts: max_attempts,
        retry_base_delay: retry_base_delay, timeout: timeout }
    end
  end

  CONFIG_LOCK = Mutex.new
  private_constant :CONFIG_LOCK

  class << self
    def configuration = CONFIG_LOCK.synchronize { @configuration ||= Configuration.new }

    #   Orch8.configure do |c|
    #     c.base_url  = "http://localhost:8080/api/v1"
    #     c.api_key   = ENV["ORCH8_API_KEY"]
    #     c.tenant_id = "acme"
    #   end
    def configure
      yield configuration
      CONFIG_LOCK.synchronize { @client = nil }
      configuration
    end

    # Shared client built from the configuration (lazily, thread-safe).
    def client
      cfg = configuration
      CONFIG_LOCK.synchronize { @client ||= Client.new(**cfg.client_options) }
    end

    attr_writer :client
  end
end

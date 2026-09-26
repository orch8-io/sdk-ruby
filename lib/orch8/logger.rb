# frozen_string_literal: true

module Orch8
  # Minimal stderr logger used when no logger is supplied. Any object that
  # responds to debug/info/warn/error (e.g. ::Logger, Rails.logger) can be
  # passed instead. Kept dependency-free on purpose (`logger` is a bundled,
  # not a default, gem on recent Rubies).
  class SimpleLogger
    LEVELS = { debug: 0, info: 1, warn: 2, error: 3 }.freeze

    def initialize(io = $stderr, level: ENV.fetch("ORCH8_LOG_LEVEL", "warn"))
      @io = io
      @level = LEVELS.fetch(level.to_s.downcase.to_sym, 2)
      @lock = Mutex.new
    end

    LEVELS.each do |name, severity|
      define_method(name) do |message = nil, &block|
        return if severity < @level

        message = block.call if message.nil? && block
        line = "#{Time.now.utc.strftime('%Y-%m-%dT%H:%M:%S.%LZ')} #{name.upcase} orch8: #{message}\n"
        @lock.synchronize { @io.write(line) }
      rescue IOError, SystemCallError
        nil
      end
    end
  end
end

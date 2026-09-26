# frozen_string_literal: true

$LOAD_PATH.unshift(File.expand_path("../lib", __dir__))
require "orch8"
require "minitest/autorun"
require_relative "support/fake_server"

FIXTURES = File.expand_path("fixtures", __dir__)

# Silent logger for tests.
class NullLogger
  %i[debug info warn error].each { |m| define_method(m) { |*_args, &_blk| nil } }
end

module ServerTest
  def setup
    super
    @server = FakeServer.new
  end

  def teardown
    @server&.close
    super
  end
end

module Minitest
  class Test
    # Queue#pop(timeout:) is Ruby >= 3.2; this works on 3.1 too.
    def pop_within(queue, seconds)
      deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + seconds
      loop do
        return queue.pop(true)
      rescue ThreadError
        return nil if Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline

        sleep 0.01
      end
    end
  end
end

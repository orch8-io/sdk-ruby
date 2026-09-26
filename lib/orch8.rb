# frozen_string_literal: true

require_relative "orch8/version"
require_relative "orch8/errors"
require_relative "orch8/logger"
require_relative "orch8/transport"
require_relative "orch8/resource"
require_relative "orch8/client"
require_relative "orch8/configuration"
require_relative "orch8/task_context"
require_relative "orch8/worker"
require_relative "orch8/push"
require_relative "orch8/job"

# Orch8 — Ruby SDK for the Orch8 durable workflow engine.
#
# * Orch8::Client — REST client (sequences, instances, jobs)
# * Orch8::Worker — long-poll worker (heartbeats, checkpoints, lease loss)
# * Orch8::Push   — push-dispatch signature verification + Rack receiver
# * Orch8::Job    — ActiveJob-style jobs on top of the jobs API
#
# The ActiveJob queue adapter is opt-in and only loaded when ActiveJob is
# present: `require "active_job/queue_adapters/orch8_adapter"`.
module Orch8
end

require_relative "active_job/queue_adapters/orch8_adapter" if defined?(ActiveJob::QueueAdapters)

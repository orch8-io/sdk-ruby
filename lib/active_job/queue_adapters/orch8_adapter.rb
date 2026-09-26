# frozen_string_literal: true

require "orch8" unless defined?(Orch8::Client)

module ActiveJob
  module QueueAdapters
    # ActiveJob queue adapter backed by the Orch8 jobs API.
    #
    #   # config/application.rb
    #   require "active_job/queue_adapters/orch8_adapter"
    #   config.active_job.queue_adapter = :orch8
    #
    #   # worker process (e.g. bin/orch8_worker, after loading the Rails app)
    #   worker = Orch8::Worker.new(queue: "default")
    #   ActiveJob::QueueAdapters::Orch8Adapter.register(worker)
    #   worker.run
    #
    # Every ActiveJob is enqueued as handler "active_job" (configurable) with
    # the ActiveJob serialized hash as payload, on the job's queue_name. The
    # worker side runs `ActiveJob::Base.execute(payload)`. Retries stay under
    # ActiveJob's control (retry_on / discard_on); exceptions that escape are
    # reported to Orch8 as retryable failures.
    #
    # This file is only loaded on demand; the orch8 gem never requires Rails.
    base = defined?(AbstractAdapter) ? AbstractAdapter : Object
    class Orch8Adapter < base
      DEFAULT_HANDLER = "active_job"

      class << self
        # Registers the ActiveJob handler on an Orch8::Worker.
        def register(worker, handler: DEFAULT_HANDLER)
          worker.register(handler) do |task|
            ActiveJob::Base.execute(task.params)
            nil
          end
        end
      end

      attr_reader :handler

      def initialize(client: nil, handler: DEFAULT_HANDLER, retry_policy: nil)
        super() if defined?(super)
        @client = client
        @handler = handler.to_s
        @retry_policy = retry_policy
      end

      def client = @client || Orch8.client

      def enqueue(job)
        submit(job)
      end

      def enqueue_at(job, timestamp)
        submit(job, run_at: Time.at(timestamp.to_f).utc)
      end

      private

      def submit(job, run_at: nil)
        opts = { queue: job.queue_name&.to_s, priority: job.priority, run_at: run_at, retry: @retry_policy,
                 metadata: { "active_job_class" => job.class.name, "active_job_id" => job.job_id } }.compact
        info = client.jobs.enqueue(@handler, job.serialize, **opts)
        job.provider_job_id = info["id"] if job.respond_to?(:provider_job_id=)
        info
      end
    end
  end
end

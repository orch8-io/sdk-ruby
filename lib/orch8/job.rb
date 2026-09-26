# frozen_string_literal: true

require "time"

module Orch8
  # ActiveJob-style background jobs backed by the Orch8 jobs API.
  #
  #   class WelcomeEmail < Orch8::Job
  #     queue_as :emails
  #     retry_policy max_attempts: 5, initial_backoff: 1, max_backoff: 60
  #
  #     def perform(user_id, locale: "en")
  #       UserMailer.welcome(user_id, locale).deliver_now
  #     end
  #   end
  #
  #   WelcomeEmail.perform_later(42)                                # POST /jobs
  #   WelcomeEmail.set(wait: 300, priority: 3).perform_later(42, locale: "uk")
  #   WelcomeEmail.perform_at(Time.now + 3600, 42)
  #
  #   # worker process
  #   worker = Orch8::Worker.new(...)
  #   worker.register_jobs(WelcomeEmail)   # or Orch8::Job.register_all(worker)
  #   worker.run
  #
  # Wire format: handler = the class name ("WelcomeEmail", override with
  # `handler_name "welcome_email"`), payload = {"args": [...], "kwargs": {...}}
  # (kwargs omitted when empty). The engine hands the payload to the worker
  # as the task `params`; the worker calls `new.perform(*args, **kwargs)`.
  # Arguments must be JSON-serializable (symbols become strings; Time values
  # become ISO-8601 strings).
  class Job
    class << self
      def inherited(subclass)
        super
        Job.registry << subclass
      end

      # Every Orch8::Job subclass defined so far.
      def registry = (@registry ||= [])

      # Registers every known job class (or the given ones) on a worker.
      def register_all(worker, classes = Job.registry)
        classes.select { |k| k.name || k.instance_variable_defined?(:@handler_name) }
               .each { |klass| worker.register_jobs(klass) }
      end

      def queue_as(name = nil, &block)
        @queue_name = block || name&.to_s
      end

      def queue_name
        q = inherited_setting(:@queue_name)
        q.respond_to?(:call) ? q.call&.to_s : q
      end

      def handler_name(name = nil)
        return @handler_name = name.to_s if name

        inherited_setting(:@handler_name) || self.name || raise(Error, "anonymous job classes need handler_name")
      end

      def priority(value = :__unset)
        return inherited_setting(:@priority) if value == :__unset

        @priority = value
      end

      # Retry policy sent with each enqueue. Backoffs in seconds.
      def retry_policy(max_attempts: nil, initial_backoff: 1, max_backoff: nil)
        return inherited_setting(:@retry_policy) if max_attempts.nil?

        @retry_policy = {
          "max_attempts" => Integer(max_attempts),
          "initial_backoff_ms" => (initial_backoff.to_f * 1000).round,
          "max_backoff_ms" => max_backoff && (max_backoff.to_f * 1000).round
        }.compact
      end

      # Client used for enqueueing (defaults to Orch8.client).
      attr_writer :client

      def client = inherited_setting(:@client) || Orch8.client

      def perform_later(*args, **kwargs) = set.perform_later(*args, **kwargs)
      def perform_at(time, *args, **kwargs) = set(wait_until: time).perform_later(*args, **kwargs)
      def perform_in(interval, *args, **kwargs) = set(wait: interval).perform_later(*args, **kwargs)

      # Runs the job inline, in the current process.
      def perform_now(*args, **kwargs) = new.perform(*args, **kwargs)

      # Per-enqueue options: wait: (seconds or a Duration), wait_until: (Time),
      # queue:, priority:, idempotency_key:, metadata:, retry: (Hash).
      def set(**options) = ConfiguredJob.new(self, options)

      # Builds the enqueue payload for the given arguments.
      def serialize_arguments(args, kwargs)
        payload = { "args" => Serializer.dump(args) }
        payload["kwargs"] = Serializer.dump(kwargs) unless kwargs.empty?
        payload
      end

      # Worker-side: turns task params back into a performed job.
      def execute(task)
        params = task.params.is_a?(Hash) ? task.params : {}
        args = Array(params["args"])
        kwargs = (params["kwargs"] || {}).to_h { |k, v| [k.to_sym, v] }
        job = new
        job.task = task
        job.perform(*args, **kwargs)
      end

      private

      def inherited_setting(ivar)
        klass = self
        while klass && klass <= Job
          return klass.instance_variable_get(ivar) if klass.instance_variable_defined?(ivar)

          klass = klass.superclass
        end
        nil
      end
    end

    # The Orch8::TaskContext when running inside a worker (nil for perform_now).
    attr_accessor :task

    def perform(*)
      raise NotImplementedError, "#{self.class.name} must implement #perform"
    end

    # Converts job arguments to JSON-safe values.
    module Serializer
      module_function

      def dump(value)
        case value
        when Hash then value.to_h { |k, v| [k.to_s, dump(v)] }
        when Array then value.map { |v| dump(v) }
        when Symbol then value.to_s
        when Time then value.utc.iso8601(6)
        when String, Integer, Float, true, false, nil then value
        else
          if value.respond_to?(:iso8601) then value.iso8601
          elsif value.respond_to?(:to_h) then dump(value.to_h)
          else raise ArgumentError, "unsupported job argument #{value.class}; pass JSON-compatible values"
          end
        end
      end
    end

    # Result of Job.set(...): enqueues with per-call options.
    class ConfiguredJob
      def initialize(job_class, options)
        @job_class = job_class
        @options = options
      end

      def set(**more) = ConfiguredJob.new(@job_class, @options.merge(more))

      # @return [Orch8::JobInfo] the enqueued job ({id, instance_id, handler, status, ...})
      def perform_later(*args, **kwargs)
        @job_class.client.jobs.enqueue(@job_class.handler_name,
                                       @job_class.serialize_arguments(args, kwargs), **enqueue_options)
      end

      def perform_now(*args, **kwargs) = @job_class.perform_now(*args, **kwargs)

      def enqueue_options
        o = @options
        opts = {
          queue: o.fetch(:queue) { @job_class.queue_name }&.to_s,
          priority: o.fetch(:priority) { @job_class.priority },
          retry: o.fetch(:retry) { @job_class.retry_policy },
          idempotency_key: o[:idempotency_key],
          metadata: o[:metadata]
        }
        if o[:wait_until]
          opts[:run_at] = o[:wait_until]
        elsif o[:wait]
          opts[:delay_ms] = (o[:wait].to_f * 1000).round
        end
        opts.compact
      end
    end
  end

  class Worker
    # Registers Orch8::Job subclasses: each handler name dispatches to
    # `JobClass.new.perform(*args, **kwargs)`.
    def register_jobs(*classes)
      classes.flatten.each do |klass|
        raise ArgumentError, "#{klass} is not an Orch8::Job" unless klass.is_a?(Class) && klass <= Job

        register(klass.handler_name) do |task|
          klass.execute(task)
          nil
        end
      end
      self
    end
  end
end

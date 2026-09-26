# orch8 — Ruby SDK for Orch8

Ruby client, worker, push receiver and ActiveJob-style jobs for
[Orch8](https://orch8.io), the self-hosted durable workflow engine.

- **Zero runtime dependencies.** Only stdlib `net/http`, `json` and `openssl`.
- **Ruby >= 3.1.** CI runs the test suite on 3.1, 3.2, 3.3 and 3.4.
- **Protocol-conformant worker.** Passes all 17 scenarios of the Orch8 SDK
  conformance kit (worker wire protocol, contract version 1). The kit is not
  public yet, so conformance runs are local-only; CI runs the unit tests.

## Install

Not on RubyGems yet. Today, install from the git tag with Bundler:

```ruby
# Gemfile
gem "orch8", git: "https://github.com/orch8-io/sdk-ruby", tag: "v0.1.0"
```

or download `orch8-0.1.0.gem` from the
[v0.1.0 GitHub release](https://github.com/orch8-io/sdk-ruby/releases/tag/v0.1.0)
and install it directly:

```bash
gh release download v0.1.0 -R orch8-io/sdk-ruby -p '*.gem'
gem install ./orch8-0.1.0.gem
```

Once published to RubyGems (the release workflow pushes the gem when the
`RUBYGEMS_API_KEY` repository secret is set):

```ruby
gem "orch8", "~> 0.1"
```

## Client

```ruby
require "orch8"

client = Orch8::Client.new(
  base_url:  "http://localhost:8080/api/v1",   # the /api/v1 base
  api_key:   ENV["ORCH8_API_KEY"],             # -> x-api-key
  tenant_id: "acme",                            # -> x-tenant-id
  max_attempts: 3, retry_base_delay: 0.25       # retries for GET/HEAD only
)

seq = client.sequences.create(
  tenant_id: "acme", namespace: "default", name: "onboarding",
  blocks: [{ type: "step", id: "welcome", handler: "send_email", params: { template: "welcome" } }]
)
client.sequences.get(seq.id)
client.sequences.list(namespace: "default", limit: 10)

inst = client.instances.create(sequence_id: seq.id, tenant_id: "acme", namespace: "default",
                               context: { data: { user_id: 42 } }, idempotency_key: "onboard-42")
inst.deduplicated?                       # true on an idempotent replay
client.instances.get(inst.id).state
client.instances.list(state: "running", limit: 20)
client.instances.signal(inst.id, { custom: "approve" }, { by: "alice" })
client.instances.pause(inst.id); client.instances.resume(inst.id)
client.instances.cancel(inst.id)         # = signal "cancel"
```

Responses are `Orch8::Resource` objects: `res.id`, `res["id"]`, `res.dig(:a, :b)`
and `res.to_h` all work, including fields added by newer engines. Lists return
Arrays.

### Jobs

```ruby
job = client.jobs.enqueue("send_email", { to: "a@example.com" },
                          queue: "emails", priority: 5,
                          retry: { max_attempts: 4, initial_backoff_ms: 500, max_backoff_ms: 10_000 },
                          delay_ms: 250,                  # or run_at: Time.now + 3600
                          idempotency_key: "welcome-a", metadata: { source: "signup" })
job.status                              # "scheduled"
client.jobs.get(job.id)
client.jobs.list(status: "failed", limit: 20)
client.jobs.cancel(job.id)
client.jobs.wait_for(job.id, timeout: 60)
```

Optional fields you don't set are left out of the request body, never sent as `null`.

### Errors

Every error is an `Orch8::Error`. HTTP errors are parsed from the engine's
envelope `{"error": {"code", "message", "request_id"}}`:

| Class | Status | `#kind` |
|---|---|---|
| `Orch8::BadRequestError` | 400 | `invalid_argument` |
| `Orch8::UnauthorizedError` | 401 | `unauthorized` |
| `Orch8::ForbiddenError` | 403 | `forbidden` |
| `Orch8::NotFoundError` | 404 | `not_found` |
| `Orch8::ConflictError` | 409 | `conflict` |
| `Orch8::PayloadTooLargeError` | 413 | `payload_too_large` |
| `Orch8::UnprocessableEntityError` | 422 | `unprocessable` |
| `Orch8::RateLimitedError` | 429 | `rate_limited` |
| `Orch8::ServerError` | 5xx | `server` |
| `Orch8::APIError` | other | `api` |
| `Orch8::TransportError` | connection failure | `transport` |

```ruby
begin
  client.jobs.get("missing")
rescue Orch8::NotFoundError => e
  e.status  # 404
  e.code    # "not_found"
  e.message # "not found: job missing"
end
```

GET and HEAD are retried on 408/425/429/5xx and on transport errors, with
exponential backoff. POST, PUT and DELETE are never replayed. Path ids are
percent-encoded as one segment, so `"a/b c"` becomes `a%2Fb%20c`.

## Worker

```ruby
worker = Orch8::Worker.new(
  base_url: "http://localhost:8080/api/v1", api_key: ENV["ORCH8_API_KEY"], tenant_id: "acme",
  concurrency: 10,           # thread pool size; max tasks in flight
  poll_interval: 1.0,        # seconds (the server's poll_after_ms wins when larger)
  heartbeat_interval: 15,    # seconds, capped by the server's heartbeat_interval_secs
  queue: nil,                # set to poll a named queue
  version: "1.4.2",          # sent on polls (version pins)
  shutdown_timeout: 30       # drain time on stop
)

worker.register("send_email") do |task|
  Mailer.deliver(task.params)          # task.params / context / attempt / instance_id / ...
  { "message_id" => "m-1" }            # output; object keys merge into context.data
end

worker.register("import_rows") do |task|
  start = task.resume_checkpoint&.fetch("row", 0) || 0
  rows.each_slice(500).with_index do |batch, i|
    next if i * 500 < start
    break if task.cancelled?           # lease lost / timed out / forced shutdown
    import(batch)
    task.checkpoint("row" => (i + 1) * 500)  # durable; CAS sequence handled for you
  end
  { "imported" => rows.size }
end

worker.register("charge") do |task|
  raise Orch8::NonRetryableError, "card declined" if declined?   # retryable: false
  raise Orch8::RetryableError, "gateway busy" if busy?            # retryable: true
  # any other exception is reported as retryable
end

worker.run   # blocks; SIGTERM/SIGINT -> stop polling, drain in-flight, return
```

What the worker does, per `WORKER_PROTOCOL.md`:

- It runs one poll loop per handler. Concurrency slots are reserved before
  each poll, so `limit` never exceeds free capacity. A full worker doesn't poll.
- After an empty poll it waits `poll_after_ms`. Poll errors back off
  exponentially, capped at 30 s.
- Every in-flight task gets a heartbeat at `min(heartbeat_interval,
  heartbeat_interval_secs, lease_secs / 2)`.
- A 404 or 409 on a heartbeat, checkpoint, complete or fail means the lease is
  lost. The worker stops heartbeating that task, sets `task.cancelled?`, makes
  `task.checkpoint` raise `Orch8::LeaseLostError`, and never acks the task.
- `complete` is retried with an identical body on 5xx, 429 and transport
  errors, because the engine makes `complete` idempotent. `retryable` is always
  sent explicitly.
- `timeout_ms` is enforced locally. When it runs out, the task is cancelled
  (`task.sleep` returns early and `task.cancelled?` is true) and reported with
  `fail` and `retryable: true`. Cancellation is cooperative: Ruby threads are
  never killed.
- `worker.start` / `worker.stop(timeout:)` let you manage the lifecycle
  yourself. `stop` returns `false` if tasks had to be abandoned. Abandoned tasks
  aren't acked, and the engine reclaims them.

## Push dispatch

In push mode the engine POSTs a signed envelope to your endpoint. The push only
wakes the worker up: it verifies the envelope, answers `202`, and then claims
the task through `POST /workers/tasks/poll/queue`.

```ruby
# Plain verification (constant-time, raw body bytes, 300 s tolerance):
Orch8::Push.verify(secret: ENV["ORCH8_PUSH_SECRET"],
                   timestamp: request.headers["X-Orch8-Timestamp"],
                   signature: request.headers["X-Orch8-Signature"],
                   body: request.raw_post)                      # => true / false
Orch8::Push.verify!(...)   # raises Orch8::SignatureVerificationError with a reason

# Rack endpoint (no Rack dependency needed), e.g. config.ru:
worker = Orch8::Worker.new(concurrency: 4)
worker.register("render") { |task| render(task.params) }
worker.start(poll: false)                        # executor + heartbeats only
run Orch8::Push::Receiver.new(worker: worker, secret: ENV["ORCH8_PUSH_SECRET"])
```

In Rails, mount the receiver: `mount Orch8::Push::Receiver.new(...) => "/orch8/push"`.
Other frameworks can use `receiver.handle(timestamp:, signature:, body:)`, which
returns `[status, envelope]`, and then call `receiver.dispatch(envelope)`.

## ActiveJob-style jobs

```ruby
Orch8.configure do |c|
  c.base_url  = "http://localhost:8080/api/v1"
  c.api_key   = ENV["ORCH8_API_KEY"]
  c.tenant_id = "acme"
end

class WelcomeEmail < Orch8::Job
  queue_as :emails
  retry_policy max_attempts: 5, initial_backoff: 1, max_backoff: 60   # seconds
  # handler_name "welcome_email"     # default: the class name

  def perform(user_id, locale: "en")
    UserMailer.welcome(user_id, locale).deliver_now
  end
end

WelcomeEmail.perform_later(42)
WelcomeEmail.set(wait: 5 * 60, priority: 3).perform_later(42, locale: "uk")   # wait: 5.minutes works too
WelcomeEmail.perform_at(Time.now + 3600, 42)
WelcomeEmail.perform_now(42)                                                  # inline

# worker process
worker = Orch8::Worker.new(queue: "emails")
worker.register_jobs(WelcomeEmail)        # or Orch8::Job.register_all(worker)
worker.run
```

Each job goes to the jobs API with `handler` set to the class name and
`payload` set to `{"args": [...], "kwargs": {...}}`. The worker then calls
`WelcomeEmail.new.perform(*args, **kwargs)`, and `job.task` holds the task
context. Arguments must be JSON-compatible: symbols become strings and times
become ISO-8601 strings.

### ActiveJob queue adapter (optional)

The gem never loads Rails. To use Orch8 as an ActiveJob backend:

```ruby
# config/application.rb
require "active_job/queue_adapters/orch8_adapter"
config.active_job.queue_adapter = :orch8

# worker process, after booting the app
worker = Orch8::Worker.new(queue: "default")
ActiveJob::QueueAdapters::Orch8Adapter.register(worker)   # handler "active_job"
worker.run
```

Jobs are enqueued as handler `active_job`, with the serialized ActiveJob as
payload and the job's `queue_name` and `priority`. `set(wait:)` and
`wait_until:` map to `run_at`. ActiveJob keeps control of retries through
`retry_on` and `discard_on`.

## Development

```bash
rake test          # minitest; no network, uses an in-process fake engine
rake conformance   # local only: needs Node >= 20 and the non-public ../sdk-contract checkout
# or directly:
node ../sdk-contract/conformance/run.mjs --adapter "$PWD/bin/conformance"
```

`bin/conformance` is the adapter for the conformance kit. It supports the
`worker | push | verify | client` modes and is written against the gem's
public API only.

## License

MIT

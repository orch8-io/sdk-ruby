# frozen_string_literal: true

require_relative "lib/orch8/version"

Gem::Specification.new do |spec|
  spec.name = "orch8"
  spec.version = Orch8::VERSION
  spec.authors = ["Oleksii Vasylenko"]
  spec.summary = "Ruby SDK for the Orch8 durable workflow engine"
  spec.description = "Typed REST client, long-poll worker (heartbeats, checkpoints, lease loss), " \
                     "push-dispatch signature verification and ActiveJob-style jobs for Orch8. " \
                     "Zero runtime dependencies."
  spec.homepage = "https://orch8.io"
  spec.license = "MIT"
  spec.required_ruby_version = ">= 3.1"

  spec.files = Dir["lib/**/*.rb", "README.md", "LICENSE"]
  spec.require_paths = ["lib"]
  spec.metadata = { "rubygems_mfa_required" => "true" }
end

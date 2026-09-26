# frozen_string_literal: true

require "rake/testtask"

Rake::TestTask.new(:test) do |t|
  t.libs << "test" << "lib"
  t.test_files = FileList["test/test_*.rb"]
  t.warning = false
end

desc "Run the sdk-contract conformance kit (needs Node >= 20 and ../sdk-contract)"
task :conformance do
  contract = ENV.fetch("SDK_CONTRACT", File.expand_path("../sdk-contract", __dir__))
  adapter = File.expand_path("bin/conformance", __dir__)
  sh "node", File.join(contract, "conformance/run.mjs"), "--adapter", adapter
end

task default: :test

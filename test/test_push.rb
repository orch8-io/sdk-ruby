# frozen_string_literal: true

require_relative "test_helper"
require "stringio"

class PushTest < Minitest::Test
  VECTORS = JSON.parse(File.read(File.join(FIXTURES, "push_signatures.json")))

  def test_shared_signature_vectors
    VECTORS["cases"].each do |v|
      got = Orch8::Push.verify(secret: v["secret"], timestamp: v["timestamp"], signature: v["signature"],
                               body: v["body"], now: v["now"], tolerance: v["tolerance_secs"] || 300)
      assert_equal v["valid"], got, v["name"]
    end
  end

  def test_sign_roundtrip_and_verify_bang_reasons
    body = '{"a":1}'
    sig = Orch8::Push.sign(secret: "s", timestamp: 100, body: body)
    assert_match(/\Asha256=[0-9a-f]{64}\z/, sig)
    assert Orch8::Push.verify(secret: "s", timestamp: "100", signature: sig, body: body, now: 100)
    assert Orch8::Push.verify(secret: "s", timestamp: 100, signature: sig, body: body, now: Time.at(150))
    err = assert_raises(Orch8::SignatureVerificationError) do
      Orch8::Push.verify!(secret: "s", timestamp: "100", signature: sig, body: body, now: 1000)
    end
    assert_match(/tolerance/, err.message)
    assert_raises(Orch8::SignatureVerificationError) do
      Orch8::Push.verify!(secret: "s", timestamp: "100", signature: sig, body: "#{body} ", now: 100)
    end
  end

  def test_empty_secret_is_a_configuration_error
    assert_raises(Orch8::ConfigurationError) do
      Orch8::Push.verify(secret: "", timestamp: "1", signature: "sha256=#{'0' * 64}", body: "", now: 1)
    end
    assert_raises(Orch8::ConfigurationError) { Orch8::Push::Receiver.new(secret: nil, on_push: ->(_) {}) }
  end

  def test_envelope_parse_keeps_unknown_fields
    env = Orch8::Push::Envelope.parse('{"task_id":"t","handler_name":"echo","queue_name":"q","params":{"n":1},"new_field":2}')
    assert_equal "echo", env.handler_name
    assert_equal "q", env.queue_name
    assert_equal({ "n" => 1 }, env.params)
    assert_equal 2, env.raw["new_field"]
  end

  def rack_env(body, ts: Time.now.to_i, secret: "whsec", method: "POST", signature: :auto)
    sig = signature == :auto ? Orch8::Push.sign(secret: secret, timestamp: ts, body: body) : signature
    env = { "REQUEST_METHOD" => method, "rack.input" => StringIO.new(body) }
    env["HTTP_X_ORCH8_TIMESTAMP"] = ts.to_s if ts
    env["HTTP_X_ORCH8_SIGNATURE"] = sig if sig
    env
  end

  def test_rack_receiver_rejects_invalid_and_dispatches_valid
    pushed = []
    app = Orch8::Push::Receiver.new(secret: "whsec", on_push: ->(e) { pushed << e }, async: false)
    body = '{"task_id":"t1","handler_name":"echo","queue_name":"push-q"}'

    assert_equal 401, app.call(rack_env(body, signature: nil)).first
    assert_equal 401, app.call(rack_env(body, secret: "wrong")).first
    assert_equal 401, app.call(rack_env(body, ts: Time.now.to_i - 1000)).first
    tampered = rack_env(body)
    tampered["rack.input"] = StringIO.new(body.sub("echo", "ohce"))
    assert_equal 401, app.call(tampered).first
    assert_equal 405, app.call(rack_env(body, method: "GET")).first
    assert_empty pushed

    status, = app.call(rack_env(body))
    assert_equal 202, status
    assert_equal ["echo"], pushed.map(&:handler_name)
  end

  def test_rack_receiver_rejects_signed_non_json
    app = Orch8::Push::Receiver.new(secret: "whsec", on_push: ->(_) { flunk "must not dispatch" }, async: false)
    assert_equal 400, app.call(rack_env("not json")).first
  end
end

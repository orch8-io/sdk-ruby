# frozen_string_literal: true

module Orch8
  # A read-only view over a JSON object returned by the engine.
  #
  # Every field (including ones this SDK version does not know about) is
  # reachable via `resource["field"]`, `resource[:field]`, `resource.field`
  # or `resource.to_h`. Nested objects are wrapped lazily; arrays of objects
  # are wrapped element-wise.
  class Resource
    include Enumerable

    def self.wrap(value)
      case value
      when Hash then new(value)
      when Array then value.map { |v| wrap(v) }
      else value
      end
    end

    def initialize(attributes)
      @attributes = attributes.transform_keys(&:to_s).freeze
    end

    def [](key) = Resource.wrap(@attributes[key.to_s])
    def key?(key) = @attributes.key?(key.to_s)
    alias has_key? key?
    def keys = @attributes.keys
    def fetch(key, *default, &block) = Resource.wrap(@attributes.fetch(key.to_s, *default, &block))
    def dig(*path) = Resource.wrap(@attributes.dig(*path.map { |p| p.is_a?(Symbol) ? p.to_s : p }))
    def each(&block) = @attributes.each(&block)

    # The raw attributes (string keys, plain Hash/Array values).
    def to_h = @attributes
    alias to_hash to_h

    def to_json(*args) = @attributes.to_json(*args)
    def ==(other) = other.is_a?(Resource) ? other.to_h == to_h : other == to_h
    def inspect = "#<#{self.class.name} #{@attributes.inspect}>"
    alias to_s inspect

    def respond_to_missing?(name, include_private = false)
      @attributes.key?(name.to_s.delete_suffix("?")) || super
    end

    def method_missing(name, *args)
      key = name.to_s
      if args.empty? && @attributes.key?(key)
        self[key]
      elsif args.empty? && key.end_with?("?") && @attributes.key?(key.chomp("?"))
        !!@attributes[key.chomp("?")]
      else
        super
      end
    end
  end

  # A background job returned by the jobs API.
  class JobInfo < Resource
    TERMINAL_STATUSES = %w[completed failed cancelled dead_lettered].freeze

    def done? = TERMINAL_STATUSES.include?(@attributes["status"])
  end
end

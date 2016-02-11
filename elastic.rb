require 'rubygems' if RUBY_VERSION < '1.9.0'
require 'elasticsearch'
require 'patron'

require 'json'
require 'date'
require 'oj'


module Sensu::Extension

  class Elastic < Handler

    def name
      'elastic'
    end

    def description
      'outputs metrics to ES'
    end

    def post_init
      @esclient = Elasticsearch::Client.new host: settings['elastic']['host'], timeout: settings['elastic']['timeout']
      @index = settings['elastic']['index']
      @type = settings['elastic']['type']

      # Use symbol as json keys
      Oj.default_options = { :symbol_keys => true }
    end

    def run(event)
      begin
        event = Oj.load(event)
      end
      rescue => e
        @logger.error("ES: Error setting up event object - #{e.backtrace.to_s}")
      end

      begin
        timestamp = event[:client][:timestamp]
        iso_timestamp = Time.parse(Time.at(timestamp.to_i).to_s).iso8601
        data = event.merge({ client: { timestamp: iso_timestamp } })
      rescue => e
        @logger.error("ES: Error parsing output lines - #{e.backtrace.to_s}")
      end

      begin
          @esclient.create index: @index, type: @type, body: data
      rescue => e
        @logger.error("ES: Error indexing event - #{e.backtrace.to_s}")
      end
      yield("ES: Handler finished", 0)
    end

  end
end

# frozen_string_literal: true

require_relative '../helper'

# The single-server get_multi reads its replies into one buffer in large
# reads, and a large value into a buffer of its own. These cover values on
# either side of the socket's read sizes and of VALUE_BUFFER_MIN_BYTES.
describe 'get_multi with large values' do
  chunk = Dalli::Socket::InstanceMethods::READ_CHUNK_SIZE
  large = Dalli::Protocol::Meta::VALUE_BUFFER_MIN_BYTES
  sizes = [0, 1, 100, chunk - 1, chunk, chunk + 1, (3 * chunk) + 7, large + 1, 600_000]

  # Random bytes, so compression is tried and doesn't shrink them
  values = sizes.each_with_index.to_h { |size, i| ["large:#{i}:#{size}", Random.bytes(size)] }
  values['large:utf8'] = 'é' * 40_000
  values['large:kéy'] = Random.bytes(large * 2)
  client_options = [{}, { raw: true }]

  MemcachedManager.supported_protocols.each do |p|
    describe "using the #{p} protocol" do
      it 'round-trips them on one server' do
        memcached_persistent(p) do |_, port|
          client_options.each do |opts|
            dc = Dalli::Client.new("localhost:#{port}", **opts)
            dc.flush
            dc.set_multi(values, 0)
            # Raw values come back as the bytes stored
            expected = opts[:raw] ? values.transform_values(&:b) : values

            assert_equal expected, dc.get_multi(values.keys), opts.inspect
            assert_equal expected, values.keys.to_h { |k| [k, dc.get(k)] }, opts.inspect
          end
        end
      end

      it 'round-trips them on two servers' do
        memcached_persistent(p, 21_355) do |_, port1|
          memcached_persistent(p, 21_356) do |_, port2|
            dc = Dalli::Client.new(["localhost:#{port1}", "localhost:#{port2}"])
            dc.flush
            dc.set_multi(values, 0)

            assert_equal values, dc.get_multi(values.keys)
            yielded = {}
            dc.get_multi(values.keys) { |k, v| yielded[k] = v }

            assert_equal values, yielded
          end
        end
      end

      it 'round-trips them over SSL' do
        memcached_ssl_persistent(p) do |dc|
          dc.set_multi(values, 0)

          assert_equal values, dc.get_multi(values.keys)
        end
      end

      it 'leaves the connection usable after a get_multi with a value that cannot be read' do
        memcached_persistent(p) do |dc, port|
          dc.set_multi(values, 0)
          store_undeserializable(port, 'large:bad')

          assert_raises(Dalli::UnmarshalError) { dc.get_multi(values.keys + ['large:bad']) }
          assert_equal values, dc.get_multi(values.keys)
        end
      end
    end
  end

  describe 'single-server get_multi reading the replies' do
    # A fake memcached answering each batch of quiet mg requests with replies,
    # written in pieces of piece bytes so headers and values arrive split
    def with_fake_memcached(port, reply, piece: nil)
      server = TCPServer.new('127.0.0.1', port)
      acceptor = Thread.new do
        loop do
          Thread.new(server.accept) do |sock|
            pending = []
            while (line = sock.gets("\r\n"))
              words = line.split
              case words.first
              when 'version' then sock.write("VERSION 1.6.45\r\n")
              when 'mg' then pending << words[1]
              when 'mn'
                bytes = "#{pending.map { |k| reply.call(k) }.join}MN\r\n"
                pending.clear
                (piece ? bytes.scan(/.{1,#{piece}}/m) : [bytes]).each do |part|
                  sock.write(part)
                  sleep 0.001 if piece
                end
              end
            end
          rescue IOError, SystemCallError
            nil
          end
        end
      rescue IOError
        nil
      end
      yield
    ensure
      server&.close
      acceptor&.kill
    end

    it 'reads replies that arrive a few bytes at a time' do
      replies = { 'a' => 'x' * 5, 'b' => '', 'c' => 'y' * 300 }
      reply = ->(k) { "VA #{replies[k].bytesize} k#{k} s#{replies[k].bytesize}\r\n#{replies[k]}\r\n" }
      with_fake_memcached(19_471, reply, piece: 7) do
        dc = Dalli::Client.new('127.0.0.1:19471', raw: true, socket_timeout: 1)

        assert_equal replies, dc.get_multi(replies.keys)
        assert_equal replies, dc.get_multi(replies.keys)
      end
    end

    it 'reads large values that arrive in pieces, with the replies around them' do
      large = Dalli::Protocol::Meta::VALUE_BUFFER_MIN_BYTES * 2
      replies = { 'a' => 'x' * 5, 'b' => 'y' * large, 'c' => 'z' * 300, 'd' => 'w' * (large + 1), 'e' => '' }
      reply = ->(k) { "VA #{replies[k].bytesize} k#{k} s#{replies[k].bytesize}\r\n#{replies[k]}\r\n" }
      with_fake_memcached(19_476, reply, piece: 16_381) do
        dc = Dalli::Client.new('127.0.0.1:19476', raw: true, socket_timeout: 1)

        assert_equal replies, dc.get_multi(replies.keys)
        assert_equal replies, dc.get_multi(replies.keys)
      end
    end

    it 'skips a reply that is not a value and reads the rest' do
      reply = ->(k) { k == 'b' ? "SERVER_ERROR out of memory\r\n" : "VA 1 k#{k} s1\r\nv\r\n" }
      with_fake_memcached(19_472, reply) do
        dc = Dalli::Client.new('127.0.0.1:19472', raw: true, socket_timeout: 1)

        with_nil_logger do
          assert_equal({ 'a' => 'v', 'c' => 'v' }, dc.get_multi(%w[a b c]))
        end
      end
    end

    it 'raises when a reply line has no CRLF within the line limit' do
      long = 'x' * (Dalli::Protocol::ConnectionManager::MAX_LINE_BYTES + 10)
      reply = ->(_k) { long }
      with_fake_memcached(19_473, reply) do
        dc = Dalli::Client.new('127.0.0.1:19473', raw: true, socket_timeout: 5, socket_max_failures: 0)
        started = Process.clock_gettime(Process::CLOCK_MONOTONIC)

        with_nil_logger do
          assert_raises(Dalli::DalliError) { dc.get_multi(%w[a b]) }
        end
        assert_operator Process.clock_gettime(Process::CLOCK_MONOTONIC) - started, :<, 2
        refute_predicate dc.send(:ring).servers.first, :connected?
      end
    end

    it 'raises when there are more replies than keys' do
      reply = ->(k) { "VA 1 k#{k} s1\r\nv\r\nVA 1 k#{k} s1\r\nv\r\n" }
      with_fake_memcached(19_474, reply) do
        dc = Dalli::Client.new('127.0.0.1:19474', raw: true, socket_timeout: 1)

        with_nil_logger do
          assert_raises(Dalli::DalliError) { dc.get_multi(%w[a b]) }
        end
      end
    end

    it 'times out on a server that stops partway through a value' do
      [[19_475, 100_000], [19_477, 400_000]].each do |port, size|
        reply = ->(k) { "VA #{size} k#{k} s#{size}\r\n#{'v' * 50_000}" }
        with_fake_memcached(port, reply) do
          dc = Dalli::Client.new("127.0.0.1:#{port}", raw: true, socket_timeout: 0.3)
          started = Process.clock_gettime(Process::CLOCK_MONOTONIC)

          with_nil_logger do
            assert_raises(Dalli::NetworkError) { dc.get_multi(%w[a]) }
          end
          assert_operator Process.clock_gettime(Process::CLOCK_MONOTONIC) - started, :<, 5
        end
      end
    end
  end
end

# frozen_string_literal: true

require_relative '../helper'

# set_multi writes values of ConnectionManager::WRITEV_MIN_PART_BYTES or more
# without copying them into the batch (with writev, or joined over TLS).
# These cover values on either side of that size, in mixed encodings.
describe 'set_multi with large values' do
  separate = Dalli::Protocol::Meta::RequestFormatter::SEPARATE_VALUE_BYTES
  sizes = [0, 1, 100, separate - 1, separate, separate + 1, 200_000, 600_000]

  # Random bytes, so compression is tried and doesn't shrink them
  values = sizes.each_with_index.to_h { |size, i| ["large:#{i}:#{size}", Random.bytes(size)] }
  values['large:utf8'] = 'é' * 40_000
  values['large:kéy'] = Random.bytes(separate * 2)
  raw_values = values.merge('large:utf16' => ('é' * 40_000).encode('UTF-16LE'))
  # Raw values come back as the bytes stored
  raw_expected = raw_values.transform_values(&:b)

  MemcachedManager.supported_protocols.each do |p|
    describe "using the #{p} protocol" do
      it 'stores them on one server' do
        memcached_persistent(p) do |_, port|
          dc = Dalli::Client.new("localhost:#{port}")
          dc.set_multi(values, 0)

          assert_equal(values, values.keys.to_h { |k| [k, dc.get(k)] })

          raw = Dalli::Client.new("localhost:#{port}", raw: true)
          raw.set_multi(raw_values, 0)

          assert_equal(raw_expected, raw_values.keys.to_h { |k| [k, raw.get(k)] })
        end
      end

      it 'stores them on two servers' do
        memcached_persistent(p, 21_357) do |_, port1|
          memcached_persistent(p, 21_358) do |_, port2|
            servers = ["localhost:#{port1}", "localhost:#{port2}"]
            dc = Dalli::Client.new(servers)
            dc.set_multi(values, 0)

            assert_equal(values, values.keys.to_h { |k| [k, dc.get(k)] })

            raw = Dalli::Client.new(servers, raw: true)
            raw.set_multi(raw_values, 0)

            assert_equal(raw_expected, raw_values.keys.to_h { |k| [k, raw.get(k)] })
          end
        end
      end

      it 'stores them over SSL' do
        memcached_ssl_persistent(p) do |dc|
          dc.set_multi(values, 0)

          assert_equal(values, values.keys.to_h { |k| [k, dc.get(k)] })
        end
      end
    end
  end
end

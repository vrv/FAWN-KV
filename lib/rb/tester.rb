#!/usr/bin/env ruby
require 'optparse'
require 'TFawnKV'

options = {}
optparse = OptionParser.new do |opts|
    opts.banner = "Usage: ./tester.rb [-c clientIP] frontendIP [port]"

    # Define the options, and what they do
    options[:clientIP] = "localhost"
    opts.on( '-c', '--clientIP myIP', 'Set this client IP (default localhost)' ) do |cip|
        options[:clientIP] = cip
    end
    opts.on( '-h', '--help', 'Display this screen' ) do
        puts opts
        exit
    end
end

optparse.parse!

if (ARGV.length < 1)
    puts "Usage: ./tester.rb [-c clientIP] frontendIP [port]"
    exit
end

ip = ARGV[0]
port = ARGV[1].to_i || 4001
myIP = options[:clientIP]

fc = FawnKVClient.new(ip, port, myIP)
key = "key"
value = "value"

count = 0
while count < 10000
  fc.put(key, value);
  count += 1
  puts fc.get(key);
end

fc.close()


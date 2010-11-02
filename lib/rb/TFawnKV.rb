#!/usr/bin/env ruby

# Generate thrift files by runnning
# thrift -r --gen rb  $(srcdir)/fawnkv/fawnkv.thrift

$:.push('gen-rb')

require 'thread'
require 'thrift'
require 'fawn_k_v'
require 'fawn_k_v_app'

class FawnKVAppHandler
    def initialize(client)
        @client = client
    end

    def get_response(value, continuation)
         @client.mutex.synchronize {
            @client.val = value
            @client.cv.signal
        }
    end

    def put_response(continuation)
        @client.mutex.synchronize {
            @client.cv.signal
        }

    end

    def remove_response(continuation)
        @client.mutex.synchronize {
            @client.cv.signal
        }

    end

end

class FawnKVClient
    attr_accessor :val, :cv, :mutex
    def initialize(ip, port, cip)
        begin
            @cv = ConditionVariable.new
            @mutex = Mutex.new

            @transport = Thrift::BufferedTransport.new(Thrift::Socket.new(ip, port))
            @protocol = Thrift::BinaryProtocol.new(@transport)
            @client = FawnKV::Client.new(@protocol)
            @ip = ip
            @transport.open()

            @handler = FawnKVAppHandler.new(self)
            @processor = FawnKVApp::Processor.new(@handler)
            @my_transport = Thrift::ServerSocket.new(port+1)
            @transportFactory = Thrift::BufferedTransportFactory.new()
            @server = Thrift::SimpleServer.new(@processor, @my_transport, @transportFactory)

            thread = Thread.new() { @server.serve() }

            @continuation = 0

            sleep(1)

            # This serves two functions: it sets up the return
            # connection on the frontend and returns the client id
            # needed for subsequent put/get/remove calls
            @cid = @client.init(cip, port+1)

        rescue Thrift::Exception => tx
            print 'Thrift::Exception: ', tx.message, "\n"
        end
    end

    def get(key)
        begin
            @client.get(key, @continuation, @cid)
            @continuation += 1
        rescue Thrift::Exception => tx
            print 'Thrift::Exception: ', tx.message, "\n"
        end

        # wait for response
        @mutex.synchronize {
            @cv.wait(@mutex)
        }

        return @val
    end

    def put(key, value)
        begin
            @client.put(key, value, @continuation, @cid)
            @continuation += 1
        rescue Thrift::Exception => tx
            print 'Thrift::Exception: ', tx.message, "\n"
        end


        # wait for response
        @mutex.synchronize {
            @cv.wait(@mutex)
        }
    end

    def close()
        @transport.close
    end
end

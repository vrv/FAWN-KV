/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

// Generated code
package fawn;
import fawn.*;

import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;

public class FawnKVClt {
    public FawnKVClt(String frontendIP, int port, String clientIP, int myPort)
    {
	myIP = clientIP;

	try {
	    TTransport transport = new TSocket(frontendIP, port);
	    TProtocol protocol = new TBinaryProtocol(transport);
	    client = new FawnKV.Client(protocol);
	    continuation = 0;

	    transport.open();


	    FawnKVCltHandler handler = new FawnKVCltHandler(this);
	    FawnKVApp.Processor processor = new FawnKVApp.Processor(handler);
	    TServerTransport serverTransport = new TServerSocket(myPort);
	    TServer server = new TSimpleServer(processor, serverTransport);

	    ServerThread st = new ServerThread(server);
	    st.start();

	    Thread.sleep(1000);

	    cid = client.init(myIP, myPort);

	} catch (TException x) {
	    x.printStackTrace();
	} catch (InterruptedException x) {
	    x.printStackTrace();
	}

    }


    public String get(String key) {
	synchronized(myMonitorObject) {

	    try {
		client.get(key.getBytes(), continuation, cid);
		continuation++;
	    } catch (TException x) {
		x.printStackTrace();
	    }

	    has_data = false;
	    while (!has_data) {
		try {
		    myMonitorObject.wait();
		} catch (InterruptedException e) {
		    e.printStackTrace();
		}
	    }
	    has_data = false;
	}

	return data;
    }

    public void put(String key, String value) {
	synchronized(myMonitorObject) {
	    try {
		client.put(key.getBytes(), value.getBytes(), continuation, cid);
		continuation++;
	    } catch (TException x) {
		x.printStackTrace();
	    }

	    has_data = false;
	    while (!has_data) {
		try {
		    myMonitorObject.wait();
		} catch (InterruptedException e) {
		    e.printStackTrace();
		}
	    }
	    has_data = false;
	}

	return;

    }

    public void remove(String key) {
	synchronized(myMonitorObject) {
	    try {
		client.remove(key.getBytes(), continuation, cid);
		continuation++;
	    } catch (TException x) {
		x.printStackTrace();
	    }

	    has_data = false;
	    while (!has_data) {
		try {
		    myMonitorObject.wait();
		} catch (InterruptedException e) {
		    e.printStackTrace();
		}
	    }
	    has_data = false;
	}

	return;

    }

    private FawnKV.Client client;
    private String myIP;
    private int cid;
    private long continuation;

    public String data;
    public boolean has_data;
    public class MonitorObject {}
    public MonitorObject myMonitorObject = new MonitorObject();


    private class FawnKVCltHandler implements FawnKVApp.Iface {
	public FawnKVCltHandler(FawnKVClt c) {
	    client = c;
	}

	public void get_response(byte[] value, long continuation)
	{
	    synchronized(client.myMonitorObject) {
		client.has_data = true;
		client.data = new String(value);
		client.myMonitorObject.notify();
	    }
	}

	public void put_response(long continuation)
	{
	    synchronized(client.myMonitorObject) {
		client.has_data = true;
		client.myMonitorObject.notify();
	    }
	}

	public void remove_response(long continuation)
	{
	    synchronized(client.myMonitorObject) {
		client.has_data = true;
		client.myMonitorObject.notify();
	    }
	}

	private FawnKVClt client;
    }

    class ServerThread extends Thread {
	TServer server;
	ServerThread(TServer server) {
	    this.server = server;
	}
	public void run() {
	    server.serve();
	}
    }

}


/**
 * FawnKV client binding for YCSB.
 *
 * By Vijay Vasudevan
 *
 *
 */

package com.yahoo.ycsb.db;

import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;

import fawn.*;

/**
 * FawnKV client for YCSB framework.
 *
 * Properties to set:
 *
 * @author Vijay Vasudevan
 *
 */
public class FawnKVClient extends DB {
    static AtomicInteger listenPort = new AtomicInteger(4002);
    private fawn.FawnKVClt client;
    private int fePort = 4001;
    /**
     * Initialize any state for this DB. Called once per DB instance; there is
     * one DB instance per client thread.
     */
    public void init() throws DBException {
	int myListenPort = listenPort.getAndIncrement();
	System.out.println("Starting client listening on port " + myListenPort);
	client = new fawn.FawnKVClt("localhost", fePort, "localhost", myListenPort);
    }

    /**
     * Delete a record from the database.
     *
     * @param table The name of the table
     * @param key The record key of the record to delete.
     * @return Zero on success, a non-zero error code on error. See this class's description for a discussion of error codes.
     */
    public int delete(String table, String key) {
	client.remove(key);
	return 0;
    }

    /**
     * Insert a record in the database. Any field/value pairs in the specified values HashMap will be written into the record with the specified
     * record key.
     *
     * @param table The name of the table
     * @param key The record key of the record to insert.
     * @param values A HashMap of field/value pairs to insert in the record
     * @return Zero on success, a non-zero error code on error. See this class's description for a discussion of error codes.
     */
    public int insert(String table, String key, HashMap<String, String> values) {
	// Iterate over collection of values, serialize, insert.
	StringBuffer data = new StringBuffer();
	for (Map.Entry<String, String> entry : values.entrySet())
	    {
		data.append(entry.getKey());
		data.append(":");
		data.append(entry.getValue());
		data.append(";");
	    }
	client.put(key, data.toString());
	return 0;
    }

    /**
     * Read a record from the database. Each field/value pair from the result will be stored in a HashMap.
     *
     * @param table The name of the table
     * @param key The record key of the record to read.
     * @param fields The list of fields to read, or null for all of them
     * @param result A HashMap of field/value pairs for the result
     * @return Zero on success, a non-zero error code on error or "not found".
     */
    public int read(String table, String key, Set<String> fields,
			HashMap<String, String> result) {
	//System.out.println("Reading (client port " + myPort + ")");
	String data = client.get(key);
	String[] values = data.split(";");
	for (int i = 0; i < values.length; i++) {
	    String[] pair = values[i].split(":");
	    if (fields != null && (fields.isEmpty() || fields.contains(pair[0]))) {
		result.put(pair[0], pair[1]);
	    }
	}
	return 0;

    }

    /**
     * Perform a range scan for a set of records in the database. Each field/value pair from the result will be stored in a HashMap.
     *
     * @param table The name of the table
     * @param startkey The record key of the first record to read.
     * @param recordcount The number of records to read
     * @param fields The list of fields to read, or null for all of them
     * @param result A Vector of HashMaps, where each HashMap is a set field/value pairs for one record
     * @return Zero on success, a non-zero error code on error. See this class's description for a discussion of error codes.
     */
    public int scan(String table, String startkey, int recordcount,
			Set<String> fields, Vector<HashMap<String, String>> result) {
	// Unsupported for now
	return 1;
    }

    /**
     * Update a record in the database. Any field/value pairs in the specified values HashMap will be written into the record with the specified
     * record key, overwriting any existing values with the same field name.
     *
     * @param table The name of the table
     * @param key The record key of the record to write.
     * @param values A HashMap of field/value pairs to update in the record
     * @return Zero on success, a non-zero error code on error. See this class's description for a discussion of error codes.
     */
    public int update(String table, String key, HashMap<String, String> values) {
	//System.out.println("Updating (client port " + myPort + ")");
	return insert(table, key, values);
    }
}



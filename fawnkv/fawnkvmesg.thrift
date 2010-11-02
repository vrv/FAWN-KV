namespace cpp fawn

struct repairInfo {
       1: i32 last_put,
       2: i32 last_ack
}

struct NodeData {
       1: binary VnodeID,
       2: string ip,
       3: i32 port
}

struct ringState {
       1: list<NodeData> nodes,
       2: i32 replication,
       3: list<string> temptails,
       4: list<string> shorters,
}

enum flushmode {SPLIT, MERGE, PLAIN}

enum returnStatus {SUCCESS, NOT_EXIST, STALE_RING}

service FawnKVBackend {
    oneway void static_join_response(1: list<binary> VnodeIDs, 16: string ip),
    oneway void rejoin_response(1: list<binary> VnodeIDs, 16: string ip),
    oneway void join_response(1: list<binary> VnodeIDs, 2: list<binary> StartIDs, 16: string ip),
    oneway void init_response(1: list<binary> VnodeIDs, 16: string ip),

    oneway void vnode_pre_join_response(1: binary VnodeID, 2: list<binary> start_keys, 3: list<binary> end_keys, 4: list<string> tail_ips, 5: list<i32> tail_ports, 16: string ip),

    oneway void precopy_request(1: binary startKey, 2: binary endKey, 3: binary vid, 16: string ip, 17: i32 port),
    oneway void precopy_response(1: binary startKey, 2: binary endKey, 3: binary vid, 16: string ip),

    oneway void flush_split(1: binary startKey, 2: binary endKey, 3: binary vid, 4: string joiner_ip, 5: list<string> forwarding_ips, 6: list<string> neighbor_ips, 7: list<binary> neighbor_vids, 8: i32 hops, 9: list<i32> forwarding_ports, 10: list<i32> neighbor_ports, 16: string ip),

    oneway void flush_merge(1: binary merge_start_key, 2: binary merge_end_key, 3: binary startKey, 4: binary endKey, 6: i32 hops),
    oneway void flush(1: binary startKey, 2: binary endKey, 3: i32 hops),
    oneway void put(1: binary key, 2: binary value, 3: i32 hops, 4: i32 ackhops, 5: i64 continuation, 6: i64 seq, 7: bool flush, 8: bool remove, 16: string ip),
    oneway void put_w(1: binary key, 2: binary value, 3: i32 hops, 4: i32 ackhops, 5: i64 continuation, 6: i64 seq, 7: bool flush, 8: bool remove, 16: string ip),
    oneway void put_ack(1: binary key, 2: i64 seq, 3: i32 hops, 16: string ip),
    oneway void get(1: binary key, 3: i64 continuation, 16: string ip),

    oneway void neighbor_update(1: binary myvid, 2: list<binary> pred_vids, 3: binary succ_vid, 4: string succid, 5: binary pred_vid, 6: string predid, 16: string ip),

    oneway void heartbeat_response(1: string ip),

    oneway void chain_repair_tail(1: binary key, 2: i32 hops, 3: i32 ack_hops, 5: binary sid, 4: binary sip),
    oneway void chain_repair_mid(1: binary key, 2: binary my_vid, 3: binary sid, 4: binary ip, 5: i32 hops, 6: i32 ack_hops),
    repairInfo chain_repair_mid_succ(1: binary key, 2: binary p_vid, 3: binary p_ip),

    oneway void chain_repair_single(1:binary new_start, 2: binary end_id),
    oneway void vnode_extend_chain( 1: binary vid,2: binary nid, 3: binary start_key, 4: binary end_key, 5: binary ip, 6: i32 port, 7: flushmode mode),
    oneway void integrity(1: i32 hops),

    oneway void init(1: string ip, 2: i32 port),
}


service FawnKVFrontend {
    oneway void put_response(1: binary key, 3: i64 continuation, 4: bool success, 16: string ip),
    oneway void put_w_response(1: binary key, 3: i64 continuation, 4: bool success, 5: i64 version, 16: string ip),
    oneway void get_response(1: binary key, 2: binary value, 3: i64 continuation, 4: returnStatus status, 16: string ip),
    oneway void remove_response(1: binary key, 3: i64 continuation, 4: bool success, 16: string ip),
}

service FawnKVManager {
    oneway void static_join(16: string ip, 17: i32 port),
    oneway void rejoin(1: list<binary> vnodeids, 2: list<binary> startids, 16: string ip, 17: i32 port),
    oneway void join(16: string ip, 17: i32 port, 18: bool merge),

    oneway void vnode_pre_join(1: binary vid, 16: string ip, 17: i32 port),
    oneway void vnode_join(1: binary vid, 16: string ip, 17: i32 port, 18: i32 merge),

    oneway void flush_split(1: binary start_id, 2: binary end_id, 3: binary vid, 4: string joiner_ip, 5: list<string> forwarding_ips, 6: list<string> neighbor_ips, 7: list<binary> neighbor_vids, 8: i32 hops, 16: string ip),
    oneway void flush_nosplit(3: binary vid),

    oneway void chain_repair_done(1: binary key,3:binary endkey),
    oneway void heartbeat(1: string ip),

    ringState get_ring_state(),

}
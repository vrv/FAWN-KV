namespace cpp fawn
namespace java fawn

service FawnKV {
    oneway void get(1: binary key, 3: i64 continuation, 4: i32 cid),
    oneway void put(1: binary key, 2: binary value, 3: i64 continuation, 4: i32 cid),
    oneway void remove(1: binary key, 3: i64 continuation, 4: i32 cid),
    i32 init(1: string ip, 2: i32 port),
}

service FawnKVApp {
    oneway void get_response(1: binary value, 3: i64 continuation),
    oneway void put_response(3: i64 continuation),
    oneway void remove_response(3: i64 continuation),
}

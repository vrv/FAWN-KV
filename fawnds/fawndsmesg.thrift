namespace cpp fawn

enum FawnDSVType {
   SET=2,
}

enum FawnDSSetOPType {
   INSERT_INTO_SET=1,
   DELETE_FROM_SET=2,
}

struct FawnDSKItem {
   1: i32 type,
   2: binary data,
}


struct FawnDSKSet {
   1: list<FawnDSKItem> item,
}



struct FawnDSSetOp {
   1: binary key,
   2: FawnDSSetOPType type,
   3: list<FawnDSKItem> item,
}


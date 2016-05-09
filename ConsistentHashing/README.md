Contains the implementation of consistent hashing based on this [implementation](https://github.com/gomathi/ConsistentHashing).

**Consistent hashing** is a special kind of hashing such that when a hash table is resized and consistent hashing is used, only _K/n_ keys need to be remapped on average, where _K_ is the number of keys, and _n_ is the number of slots. In contrast, in most traditional hash tables, a change in the number of array slots causes nearly all keys to be remapped.

Consistent hashing places buckets (i.e., servers) and members (i.e., elements) on a _ring_. Each member's corresponding bucket is found by walking _clockwise_ on the ring, and whichever bucket comes first is the owner of the member.

If few buckets are placed closely on the ring, then the members distribution to buckets wont be fair. Sometimes a bucket may get almost 90% of the members. To avoid that, the ring is divided into fixed number of _segments_ (i.e., **virtual nodes**), and buckets are mapped to these segments. Hence each bucket will get almost close-to-equivalent number of members.

This implementation uses the concept of virtual nodes, where each bucket will be mapped to many logical buckets. This helps in getting fair distribution of members to buckets.

The requirements for this consistent hashing library are:

1. It must map the bucket ids (servers).
2. It must map the member ids (elements).
3. Retrieve all members that fall into a particular bucket, given the bucket name.
4. Implementation should have virtual nodes, so members to bucket distribution will be even. 
5. A generic interface, so that any kind of buckets (strings, integers, custom datatypes), and any kind of members can be stored.
6. [optional] The whole object should be _thread-safe_.

## Usage

A consistent hashing object can be created as following

    ConsistentHasher<Integer, Integer> cHasher = new ConsistentHasherImpl<>(
        1,
        ConsistentHasher.getIntegerToBytesConverter(),
        ConsistentHasher.getIntegerToBytesConverter(),
        ConsistentHasher.SHA1);

The above object defines a consistent hasher with bucket type as `Integer` type, also member type as `Integer` type. Since buckets and members can be of any data type, hasher requires byte converter objects to convert buckets and members into corresponding `byte` array. Also each `byte` array is hashed by sha1 to uniformly place it on the consistent hashing ring. Here I am using virtual nodes size as 1.
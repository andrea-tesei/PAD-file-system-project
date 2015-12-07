package it.cnr.isti.pad.ConsistentHashing;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class ConsistentHasher<B,M> implements IConsistentHasher<B,M> {
	
	
	// Hash function
	private final IHashFunction hashFunction;
	// Converter for members
	private final BytesConverter<M> memberToBytesConverter;
	// Converter for buckets
	private final BytesConverter<B> bucketToBytesConverter;
	// Number of replicated virtual node
	private final int numberOfVrtNode;
	// The map of buckets
	private final ConcurrentNavigableMap<ByteBuffer, B> bucketMap;
	// The map of virtual nodes of each bucket
	private final ConcurrentHashMap<B, ArrayList<ByteBuffer>> vrtlBucketMap;
	// The map of members
	private final ConcurrentNavigableMap<ByteBuffer, M> membersMap;
	
	ConsistentHasher(int numberOfVirtualNodes, BytesConverter<B> bucketBytesConverter, BytesConverter<M> membersBytesConverter, IHashFunction hashFunction){
		this.numberOfVrtNode = numberOfVirtualNodes;
		this.memberToBytesConverter = membersBytesConverter;
		this.bucketToBytesConverter = bucketBytesConverter;
		this.hashFunction = hashFunction;
		this.bucketMap = new ConcurrentSkipListMap<ByteBuffer, B>();
		this.vrtlBucketMap = new ConcurrentHashMap<B, ArrayList<ByteBuffer>>();
		this.membersMap = new ConcurrentSkipListMap<ByteBuffer, M>();
	}
	
	public boolean insertBucket(B newBucket) {
		if(newBucket == null) throw new NullPointerException("Error:ConsistentHasher.insertBucket(): the input bucket cannot be null.");
		byte[] bucketBytes = this.bucketToBytesConverter.convert(newBucket);
		ArrayList<ByteBuffer> vrtlNodes = new ArrayList<ByteBuffer>();
		for(int i = 1; i <= this.numberOfVrtNode; i++){
			byte[] bucketAndIdBytes = new byte[4 + bucketBytes.length];
			ByteBuffer bb = ByteBuffer.wrap(bucketAndIdBytes);
			bb.put(bucketBytes);
			bb.putInt(i);
			ByteBuffer result = ByteBuffer.wrap(this.hashFunction.hash(bb.array()));
			this.bucketMap.put(result, newBucket);
			vrtlNodes.add(result);
		}
		this.vrtlBucketMap.put(newBucket, vrtlNodes);
		return true;
	}
	
	public boolean removeBucket(B bucket) {
		if(bucket == null) throw new NullPointerException("Error:ConsistentHasher.removeBucket(): the input bucket cannot be null.");
		ArrayList<ByteBuffer> bucketForRemove = this.vrtlBucketMap.get(bucket);
		bucketForRemove.forEach(bucketBytes -> {
			this.bucketMap.remove(bucketBytes);
		});
		this.vrtlBucketMap.remove(bucket);
		return true;
	}

	public boolean insertMember(M newMember) {
		if(newMember == null) throw new NullPointerException("Error:ConsistentHasher.insertMember(): the input member cannot be null.");
		byte[] memberBytes = this.memberToBytesConverter.convert(newMember);
		this.membersMap.put(ByteBuffer.wrap(this.hashFunction.hash(memberBytes)), newMember);
		return true;
	}

	public boolean removeMember(M member) {
		if(member == null) throw new NullPointerException("Error:ConsistentHasher.removeMember(): the input member cannot be null.");
		byte[] memberBytes = this.memberToBytesConverter.convert(member);
		this.membersMap.remove(ByteBuffer.wrap(this.hashFunction.hash(memberBytes)));
		return true;
	}

	public ArrayList<M> getMembersFromBucket(B bucket) {
		if(bucket == null) throw new NullPointerException("Error:ConsistentHasher.getMembersFromBucket(): the input bucket cannot be null.");
		if(!this.vrtlBucketMap.contains(bucket))
			return new ArrayList<M>();
		ArrayList<ByteBuffer> vrtNodeOfBucket = this.vrtlBucketMap.get(bucket);
		ArrayList<M> results = new ArrayList<M>();
		vrtNodeOfBucket.forEach(bb -> {
			ByteBuffer prev = this.bucketMap.lowerKey(bb);
			this.membersMap.subMap(prev, bb).forEach((key, value) -> {
				results.add(value);
			});
		});
		return results;
	}
	
	public static BytesConverter<String> getStringBytesConverter() {
		return new BytesConverter<String>() {

			@Override
			public byte[] convert(String data) throws NullPointerException {
				if(data == null) throw new NullPointerException("Error:BytesConverterString.convert: the input value cannot be null.");
				return data.getBytes();
			}
		};
	}
	
	public static BytesConverter<Integer> getIntegerBytesConverter(){
		return new BytesConverter<Integer>(){
			
			@Override
			public byte[] convert(Integer data) {
				ByteBuffer bb = ByteBuffer.allocate(4); 
			    bb.putInt(data.intValue()); 
			    return bb.array();
			}
		};
	}
}

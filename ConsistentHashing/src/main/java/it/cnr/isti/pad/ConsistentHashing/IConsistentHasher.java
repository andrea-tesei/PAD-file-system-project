package it.cnr.isti.pad.ConsistentHashing;

import java.nio.ByteBuffer;
import java.util.ArrayList;


public interface IConsistentHasher<B,M> {
	
	boolean insertBucket(B newBucket);
	
	boolean removeBucket(B bucket);
	
	boolean insertMember(M newMember);
	
	boolean removeMember(M member);
	
	ArrayList<M> getMembersFromBucket(B bucket);
	
	public static interface BytesConverter<T> {
		byte[] convert(T data);
	}

}

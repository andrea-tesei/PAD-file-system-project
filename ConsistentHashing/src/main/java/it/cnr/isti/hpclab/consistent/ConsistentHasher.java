package it.cnr.isti.hpclab.consistent;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import com.google.common.hash.Hashing;

/**
 * Defines consistent hash interface. Consistent hasher tries to reduce the
 * number of values that are getting rehashed while new bucket addition/removal.
 * More information is available on {@link http
 * ://en.wikipedia.org/wiki/Consistent_hashing}.
 * 
 * Defined the interface, so that methods will be clear, rather than being
 * buried inside the implementation.
 *
 * @param <B> the type of a bucket, i.e. a node
 * @param <M> the type of a member, i.e. a stored element
 */
public interface ConsistentHasher<B, M> 
{
	/**
	 * Adds the bucket.
	 * 
	 * @param bucketName the bucket name to add.
	 * @throws NullPointerException	if the given argument is null.
	 */
	void addBucket(B bucketName);

	/**
	 * Removes the bucket. There can be virtual nodes for given a bucket.
	 * Removing a bucket, and listing the members of a bucket should be executed
	 * atomically, otherwise {@link #getMembersFor(Object)} might return partial
	 * members of the given bucket. To avoid that, a lock is used on every
	 * physical bucket. If there is a call {@link #getMembersFor(Object)}
	 * getting executed, then this method waits until all those threads to
	 * finish. In worst case this function might wait for the lock for longer
	 * period of time if multiple readers are using the same lock, and if you
	 * want to return in fixed amount of time then use
	 * {@link #tryRemoveBucket(Object, long, TimeUnit)}
	 * 
	 * @param bucketName  the bucket name to remove.
	 * @throws NullPointerException if the given argument is null.
	 */
	void removeBucket(B bucketName) throws InterruptedException;

	/**
	 * Similar to {@link #removeBucket(Object)}, except that this function
	 * returns within the given timeout value.
	 * 
	 * @param bucketName		the bucket name to remove.
	 * @param timeout		the timeout for the operation.
	 * @param unit			the time measure for the timeout.
	 * @throws NullPointerException	if the given argument is null.
	 */
	boolean tryRemoveBucket(B bucketName, long timeout, TimeUnit unit) throws InterruptedException;

	/**
	 * Adds member to the consistent hashing ring.
	 * 
	 * @param memberName	the member name to add.
	 * @throws NullPointerException	if the given argument is null.
	 */
	void addMember(M memberName);

	/**
	 * Removes member from the consistent hashing ring.
	 * 
	 * @param memberName	the member name to remove.
	 * @throws NullPointerException	if the given argument is null.
	 */
	void removeMember(M memberName);

	/**
	 * Returns all the members that belong to the given bucket. If there is no
	 * such bucket returns an empty list.
	 * 
	 * @param bucketName the bucket name.
	 * @return the list of members of the given bucket, otherwise an empty list.
	 * @throws NullPointerException	 if the given argument is null.
	 */
	List<M> getMembersFor(B bucketName);

	/**
	 * Returns all the buckets and corresponding members of that buckets.
	 * 
	 * @return a map of bucket to list of members, if there are buckets and members, otherwise an empty map.
	 * 
	 */
	Map<B, List<M>> getAllBucketsToMembersMapping();

	/**
	 * Returns all buckets that are stored. If there are no buckets, returns an
	 * empty list.
	 * 
	 * @return all buckets that are stored, otherwise an empty list.
	 */
	List<B> getAllBuckets();
	
	/**
	 * Returns all virtual buckets that are stored. If there are no buckets, returns an
	 * empty list.
	 * 
	 * @param bucketName
	 * @return all virtual buckets that are stored, otherwise an empty list.
	 */
	List<ByteBuffer> getAllVirtualBucketsFor(B bucketName);
	
	/**
	 * Returns the next key greater than or equal to the given key. Returns
	 * null in case of error and fromBucket in case of NOT_EXISTS error on
	 * fromBucket.
	 * 
	 * @param fromBucket: the key from which start
	 * @return the next key greater than or equal to fromBucket
	 */
	B getDescendantBucketKey(B fromBucket, ByteBuffer VirtNode);
	
	/**
	 * Returns the prev key greater than or equal to the given key (in particular taking into . Returns
	 * null in case of error and fromBucket in case of NOT_EXISTS error on
	 * fromBucket.
	 * 
	 * @param fromBucket: the key from which start
	 * @return the next key greater than or equal to fromBucket
	 */
	B getLowerKey(B fromBucket, ByteBuffer VirtNode);

	/**
	 * This fetches the members for the given bucket from the given members
	 * list. This method does not use the members which are already stored on
	 * this instance.
	 * 
	 * @param bucketName
	 * @param members
	 * @return
	 * @throws NullPointerException  if any of the arguments is null.
	 */
	List<M> getMembersFor(B bucketName, List<? extends M> members);
	
	/**
	 * This fetches the members for the given virtual bucket from the given members
	 * list. This method does not use the members which are already stored on
	 * this instance.
	 * 
	 * @param bucketName
	 * @param virtBucket: index of the virtual bucket considered
	 * @param members
	 * @return
	 * @throws NullPointerException  if any of the arguments is null.
	 */
	List<M> getMembersForVirtualBucket(B bucketName, ByteBuffer virtBucket, List<? extends M> members);
	
	/**
	 * This fetches the members for the given new bucket (case of new node) from the given members
	 * list. This method does not use the members which are already stored on
	 * this instance.
	 * 
	 * @param myBucketName
	 * @param newBucket
	 * @param members
	 * @return
	 * @throws NullPointerException  if any of the arguments is null.
	 */
	List<M> getMembersForNewBackupBucket(B myBucketName, B newBucket, List<? extends M> members);

	/**
	 * Returns all members that are stored in this instance. If there are no
	 * members, returns an empty list.
	 * 
	 * @return all members that are stored in this instance, otherwise an empty list.
	 */
	List<M> getAllMembers();

	/**
	 * Converts the given data into bytes. Implementation should be thread safe.
	 *
	 * @param <T> the type of the input data to be converted into butes
	 */
	public static interface BytesConverter<T> 
	{
		/**
		 * Converts a given data into an array of bytes.
		 * @param data	the data to be converted.
		 * @return the data represented as array of bytes.
		 */
		byte[] convert(T data);
	}

	/**
	 * Converts the given data into bytes. Implementation should be thread safe.
	 *
	 */
	public static interface HashFunction 
	{
		/**
		 * Computes the hash value of an array of bytes.
		 * 
		 * @param input the array of bytes to be hashed
		 * @return the hash value 
		 */
		byte[] hash(byte[] input);
	}

	// Helper implementations

	public static final HashFunction SHA1 = new SHA1HashFunction();

	public static HashFunction getSHA1HashFunction() 
	{
		return SHA1;
	}

	/**
	 * 
	 * @return
	 */
	public static BytesConverter<String> getStringToBytesConverter() 
	{
		return new BytesConverter<String>() 
		{
			@Override
			public byte[] convert(String data) 
			{
				Preconditions.checkNotNull(data);
				return data.getBytes();
			}
		};
	}

	/**
	 * 
	 */
	public static class SHA1HashFunction implements HashFunction 
	{
		@Override
		public byte[] hash(byte[] input) 
		{
			Preconditions.checkNotNull(input);
			return Hashing.sha1().hashBytes(input).asBytes();
		}
	}

	/**
	 * Returned object is thread safe.
	 * 
	 * @return
	 */
	public static BytesConverter<Integer> getIntegerToBytesConverter() 
	{
		return new BytesConverter<Integer>() 
		{
			@Override
			public byte[] convert(Integer input) 
			{
				byte[] inputBytes = new byte[Integer.BYTES / Byte.BYTES];
				ByteBuffer bb = ByteBuffer.wrap(inputBytes);
				bb.putInt(input);
				return inputBytes;
			}
		};
	}
	
}
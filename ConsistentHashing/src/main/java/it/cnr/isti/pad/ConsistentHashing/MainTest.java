package it.cnr.isti.pad.ConsistentHashing;

import java.io.IOException;

/**
 * Hello world!
 *
 */
public class MainTest 
{
    public static void main( String[] args )
    {
    	SHA1HashFunction hashFunction = new SHA1HashFunction();
    	ConsistentHasher<Integer, Integer> hasher = new ConsistentHasher<Integer,Integer>(
    			4,
    			ConsistentHasher.getIntegerBytesConverter(), 
    			ConsistentHasher.getIntegerBytesConverter(), 
    			hashFunction);
    	
    	for(int i = 0; i < 10; i++)
    		hasher.insertBucket(i);
    	for(int i = 0; i < 100; i++)
    		hasher.insertMember(i);
    	
        try {
			System.in.read();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
}

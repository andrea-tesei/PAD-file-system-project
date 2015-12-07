package it.cnr.isti.pad.ConsistentHashing;

import com.google.common.hash.Hashing;

public class SHA1HashFunction implements IHashFunction {

	@Override
	public byte[] hash(byte[] input) {
		if(input == null) throw new NullPointerException("Error: the given input is null. I'm not able to hash it.");
		return Hashing.sha1().hashBytes(input).asBytes();
	}
}

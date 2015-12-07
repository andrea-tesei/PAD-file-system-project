package it.cnr.isti.pad.ConsistentHashing;

public interface IHashFunction {
	
	byte[] hash(byte[] input);
}

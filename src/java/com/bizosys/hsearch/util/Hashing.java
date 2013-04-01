package com.bizosys.hsearch.util;

import org.apache.hadoop.hbase.util.Hash;
import org.apache.hadoop.hbase.util.MurmurHash;

public final class Hashing {
	static Hash mmh = MurmurHash.getInstance();
	
	public static final int hash(String word) {
		return mmh.hash(word.getBytes());
	}
	
}

package com.bizosys.hsearch.util;

import java.util.Comparator;

public class MultiKeyValue<K,V> implements Comparator<MultiKeyValue<K,V>>{
	public K key;
	public V value;
	
	public MultiKeyValue() {
	}

	public MultiKeyValue(K key, V value) {
		this.key = key;
		this.value = value;
	}

	@Override
	public int compare(MultiKeyValue<K, V> o1, MultiKeyValue<K, V> o2) {
		return 0;
	}
	
}

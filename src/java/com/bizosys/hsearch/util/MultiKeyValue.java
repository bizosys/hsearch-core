package com.bizosys.hsearch.util;

import java.util.Comparator;

public final class MultiKeyValue<K,V> implements Comparator<MultiKeyValue<K,V>>{
	public K key;
	public V value;
	
	public MultiKeyValue() {
	}

	public MultiKeyValue(final K key, final V value) {
		this.key = key;
		this.value = value;
	}

	@Override
	public final int compare(final MultiKeyValue<K, V> o1, final MultiKeyValue<K, V> o2) {
		return 0;
	}
	
}
package com.bizosys.hsearch.treetable;

public class CellKeyValue<K,V> {
	public K key;
	public V value;
	
	public CellKeyValue() {
	}

	public CellKeyValue(K key, V val ) {
		 this.key = key;
		 this.value = val;
	}
	
	public K getKey() {
		return key;
	}

	public V getValue() {
		return this.value;
	}
	
	@Override
	public String toString() {
		return key.toString() + "-" + value.toString();
	}
	
}

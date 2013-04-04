package com.bizosys.hsearch.util;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

public class EmptyMap<K, V> implements Map<K, V> {

	@Override
	public final int size() {
		return 0;
	}

	@Override
	public final boolean isEmpty() {
		return false;
	}

	@Override
	public final boolean containsKey(Object key) {
		return false;
	}

	@Override
	public final boolean containsValue(Object value) {
		return false;
	}

	@Override
	public final V get(Object key) {
		return null;
	}

	@Override
	public V put(K key, V value) {
		return null;
	}

	@Override
	public final V remove(Object key) {
		return null;
	}

	@Override
	public final void putAll(Map<? extends K, ? extends V> m) {
	}

	@Override
	public final void clear() {
	}

	@Override
	public final Set<K> keySet() {
		return null;
	}

	@Override
	public final Collection<V> values() {
		return null;
	}

	@Override
	public final Set<java.util.Map.Entry<K, V>> entrySet() {
		return null;
	}

}

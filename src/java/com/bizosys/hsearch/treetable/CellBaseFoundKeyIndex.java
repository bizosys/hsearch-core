package com.bizosys.hsearch.treetable;

import java.util.Collection;
import java.util.Iterator;

import com.bizosys.hsearch.byteutils.ISortedByte;

public class CellBaseFoundKeyIndex<K1> implements Collection<Integer>{
	
	private ISortedByte<K1> k1Sorter = null;
	private Collection<K1> foundKeys = null;

	public CellBaseFoundKeyIndex(ISortedByte<K1> k1Sorter, Collection<K1> foundKeys) {
		this.k1Sorter = k1Sorter;
		this.foundKeys = foundKeys;
	}

	@Override
	public int size() {
		return 0;
	}

	@Override
	public boolean isEmpty() {
		return false;
	}

	@Override
	public boolean contains(Object o) {
		return false;
	}

	@Override
	public Iterator<Integer> iterator() {
		return null;
	}

	@Override
	public Object[] toArray() {
		return null;
	}

	@Override
	public <T> T[] toArray(T[] a) {
		return null;
	}

	@Override
	public final boolean add(Integer position)  {
		foundKeys.add( k1Sorter.getValueAt(position) );
		return true;
	}

	@Override
	public boolean remove(Object o) {
		return false;
	}

	@Override
	public boolean containsAll(Collection<?> c) {
		return false;
	}

	@Override
	public boolean addAll(Collection<? extends Integer> c) {
		return false;
	}

	@Override
	public boolean removeAll(Collection<?> c) {
		return false;
	}

	@Override
	public boolean retainAll(Collection<?> c) {
		return false;
	}

	@Override
	public void clear() {
	}
}

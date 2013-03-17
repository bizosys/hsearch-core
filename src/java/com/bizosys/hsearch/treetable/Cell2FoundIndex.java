package com.bizosys.hsearch.treetable;

import java.util.Collection;
import java.util.Iterator;

import com.bizosys.hsearch.byteutils.ISortedByte;

public class Cell2FoundIndex<K1, V> implements Collection<Integer>{
	
	private ISortedByte<K1> k1Sorter = null;
	private ISortedByte<V> vSorter = null;
	private Cell2Visitor<K1,V> visitor = null;

	public Cell2FoundIndex(ISortedByte<K1> k1Sorter, ISortedByte<V> vSorter, Cell2Visitor<K1,V> visitor) {
		this.k1Sorter = k1Sorter;
		this.vSorter = vSorter;
		this.visitor = visitor;
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
	public final boolean add(Integer i)  {
		visitor.visit(k1Sorter.getValueAt(i), vSorter.getValueAt(i));
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

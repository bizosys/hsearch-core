package com.bizosys.hsearch.byteutils;

import java.io.IOException;
import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

public abstract class SortedByte<T> {
	
	public static <K,V extends Comparable<? super V>> SortedSet<Map.Entry<K,V>> entriesSortedByValues(Map<K,V> map) {
        SortedSet<Map.Entry<K,V>> sortedEntries = new TreeSet<Map.Entry<K,V>>(
            new Comparator<Map.Entry<K,V>>() {
                @Override public int compare(Map.Entry<K,V> e1, Map.Entry<K,V> e2) {
                    int res = e1.getValue().compareTo(e2.getValue());
                    return res != 0 ? res : 1; // Special fix to preserve items with equal values
                }
            }
        );
        sortedEntries.addAll(map.entrySet());
        return sortedEntries;
    }
	
	public abstract byte[] toBytes(Collection<T> sortedCollection, boolean clearList) throws IOException;

	public abstract void addAll(byte[] bytes, Collection<T> vals) throws IOException;
	public abstract void addAll(byte[] bytes, int offset, Collection<T> vals) throws IOException;
	
	public abstract T getValueAt(byte[] bytes, int pos) throws IOException;
	public abstract T getValueAt(byte[] bytes, int offset, int pos) throws IOException;

	public abstract int getEqualToIndex(byte[] inputData, T matchNo) throws IOException;
	public abstract int getEqualToIndex(byte[] inputData, int offset, T matchNo) throws IOException;
	
	public abstract void getEqualToIndexes(byte[] inputData, T matchNo, Collection<Integer> matchings) throws IOException;
	public abstract void getGreaterThanIndexes(byte[] inputData, T matchingNo, Collection<Integer> matchingPos) throws IOException;
	public abstract void getGreaterThanEuqalToIndexes(byte[] inputData, T matchingNo, Collection<Integer> matchingPos) throws IOException;
	public abstract void getLessThanIndexes(byte[] inputData, T matchingNo, Collection<Integer> matchingPos ) throws IOException;
	public abstract void getLessThanEuqalToIndexes(byte[] inputData, T matchingNo, Collection<Integer> matchingPos) throws IOException;

	public abstract void getRangeIndexes(byte[] inputData, T matchNoStart, T matchNoEnd, Collection<Integer> matchings) throws IOException;
	public abstract void getRangeIndexesInclusive(byte[] inputData, T matchNoStart, T matchNoEnd, Collection<Integer> matchings) throws IOException;
	
	
}

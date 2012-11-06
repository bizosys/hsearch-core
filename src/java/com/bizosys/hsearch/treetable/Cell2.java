package com.bizosys.hsearch.treetable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.bizosys.hsearch.byteutils.SortedByte;
import com.bizosys.hsearch.byteutils.SortedBytesArray;

public class Cell2<K1, V> {
	
	public SortedByte<K1> k1Sorter = null;
	public SortedByte<V> vSorter = null;
	private List<CellKeyValue<K1, V>> sortedList = null;
	public byte[] data = null;
	
	public Cell2(SortedByte<K1> k1Sorter, SortedByte<V> vSorter) {
		this.k1Sorter = k1Sorter;
		this.vSorter = vSorter;
	}

	public Cell2(SortedByte<K1> k1Sorter, SortedByte<V> vSorter, List<CellKeyValue<K1, V>> sortedList) {
		this(k1Sorter, vSorter);
		this.sortedList = sortedList;
	}

	
	public Cell2 (SortedByte<K1> k1Sorter, SortedByte<V> vSorter, byte[] data) {
		this(k1Sorter, vSorter);
		this.data = data;
	}
	
	public void add(K1 k, V v) {
		if ( null == sortedList) sortedList = new ArrayList<CellKeyValue<K1,V>>();
		sortedList.add(new CellKeyValue<K1, V>(k, v) );
	}
	
	public void sort(Comparator<CellKeyValue<K1, V>> comp) {
		if ( null == sortedList) return;
		Collections.sort(sortedList, comp);
	}
	
	public List<CellKeyValue<K1, V>> getMap(byte[] data) throws IOException {
		this.data = data;
		parseElements();
		return sortedList;
	}
	
	public List<CellKeyValue<K1, V>> getMap() throws IOException {
		if ( null != sortedList) return sortedList;
		if ( null != this.data) {
			parseElements();
			return sortedList;
		}
		throw new IOException("Cell is not initialized");
	}
	
	
	public byte[] toBytes() throws IOException {

		if ( sortedList.size() == 0 ) return null;
		Collection<K1> keys = new ArrayList<K1>();
		Collection<V> values = new ArrayList<V>();
		
		for (CellKeyValue<K1, V> entry : sortedList) {
			keys.add(entry.getKey());
			values.add(entry.getValue());
		}

		List<byte[]> bytesElems = new ArrayList<byte[]>();
		bytesElems.add(k1Sorter.toBytes(keys, true));
		bytesElems.add(vSorter.toBytes(values, true));
		
		byte[] cellB = SortedBytesArray.getInstance().toBytes(bytesElems, false);
		return cellB;
	}
	
	public Collection<Integer> indexOf(V exactValue) throws IOException {
		List<Integer> foundPositions = new ArrayList<Integer>();
		findMatchingPositions(exactValue, null, null, foundPositions);
		return foundPositions;
	}
	
	public Collection<Integer> indexOf(V minimumValue, V maximumValue) throws IOException {
		List<Integer> foundPositions = new ArrayList<Integer>();
		findMatchingPositions(null, minimumValue, maximumValue, foundPositions);
		return foundPositions;
	}

	public Set<K1> keySet(V exactValue) throws IOException {
		Set<K1> keys = new HashSet<K1>();
		keySet(exactValue, null, null, keys);
		return keys;
	}
	
	public void keySet(V exactValue, Collection<K1> keys) throws IOException {
		keySet(exactValue, null, null, keys);
	}

	public Set<K1> keySet(V minimumValue, V maximumValue) throws IOException {
		Set<K1> keys = new HashSet<K1>();
		keySet(minimumValue, maximumValue, keys);
		return keys;
	}
	
	public void keySet(V minimumValue, V maximumValue, Collection<K1> keys) throws IOException {
		keySet(null, minimumValue, maximumValue, keys);
	}
	
	private void keySet( V exactValue, V minimumValue, V maximumValue, Collection<K1> foundKeys) throws IOException {
		byte[] allKeysB = SortedBytesArray.getInstance().getValueAt(data, 0);
		List<Integer> foundPositions = new ArrayList<Integer>();
		findMatchingPositions(exactValue, minimumValue, maximumValue, foundPositions);

		for (int position : foundPositions) {
			foundKeys.add( k1Sorter.getValueAt(allKeysB, position) );
		}
	}
	
	public Collection<V> values(V exactValue) throws IOException {
		Collection<V> values = new ArrayList<V>();
		matchValues(exactValue, null, null, values);
		return values;
	}
	
	public Collection<V> values(V minimumValue, V maximumValue) throws IOException {
		Collection<V> values = new ArrayList<V>();
		matchValues(null, minimumValue, maximumValue, values);
		return values;
	}

	public void values(V exactValue, Collection<V> foundValues) throws IOException {
		matchValues(exactValue, null, null, foundValues);
	}
	
	public void values(V minimumValue, V maximumValue, Collection<V> foundValues) throws IOException {
		matchValues(null, minimumValue, maximumValue, foundValues);
	}	
	
	private void matchValues( V exactValue, V minimumValue, V maximumValue, Collection<V> foundValues) throws IOException {
		List<Integer> foundPositions = new ArrayList<Integer>();
		byte[] allValuesB = findMatchingPositions(exactValue, minimumValue, maximumValue, foundPositions);

		for (int position : foundPositions) {
			foundValues.add( vSorter.getValueAt(allValuesB, position) );
		}
	}

	public Collection<V> values() throws IOException {

		Collection<V> values = new ArrayList<V>();
		values(values);
		return values;
	}
	
	public void values( Collection<V> values) throws IOException {
		
		byte[] allValuesB = SortedBytesArray.getInstance().getValueAt(data, 1);
		int length = ( null == allValuesB) ? 0 : allValuesB.length;
		int size = vSorter.getSize(allValuesB, 0, length);
		
		for ( int i=0; i<size; i++) {
			values.add(vSorter.getValueAt(allValuesB, i));
		}
	}
		
	public Collection<V> valuesAt(Collection<Integer> foundPositions) throws IOException {
		List<V> foundValues = new ArrayList<V>();
		valuesAt(foundValues, foundPositions );
		return foundValues;
	}

	public void valuesAt(Collection<V> foundValues, Collection<Integer> foundPositions) throws IOException {
		
		byte[] allValuesB = SortedBytesArray.getInstance().getValueAt(data, 1);
		for (int position : foundPositions) {
			foundValues.add( vSorter.getValueAt(allValuesB, position) );
		}
	}
	
	public byte[] findMatchingPositions( V exactValue, V minimumValue, V maximumValue, List<Integer> foundPositions) throws IOException {
			
		byte[] allValsB = SortedBytesArray.getInstance().getValueAt(data, 1);
		if ( null == allValsB) return null;
			
		if ( null != exactValue || null != minimumValue || null != maximumValue ) {
				
			if ( null != exactValue ) {
				vSorter.getEqualToIndexes(allValsB, exactValue, foundPositions);
			} else {
				if ( null != minimumValue && null != maximumValue ) {
					vSorter.getRangeIndexesInclusive(allValsB, minimumValue, maximumValue, foundPositions);
				} else if ( null != minimumValue) {
					vSorter.getGreaterThanEqualToIndexes(allValsB, minimumValue, foundPositions);
				} else {
					vSorter.getLessThanEqualToIndexes(allValsB, maximumValue, foundPositions);
				}
			}
		}
		return allValsB;
	}

	public Collection<K1> keySet() throws IOException {
		List<K1> keys = new ArrayList<K1>();
		keySet(keys);
		return keys;
	}
	
	public void keySet( Collection<K1> keys) throws IOException {
		
		byte[] allKeysB = SortedBytesArray.getInstance().getValueAt(data, 0);
		
		int length = ( null == allKeysB) ? 0 : allKeysB.length;
		int size = k1Sorter.getSize(allKeysB, 0, length);
		
		for ( int i=0; i<size; i++) {
			keys.add(k1Sorter.getValueAt(allKeysB, i));
		}
	}
	
	public void parseElements() throws IOException {
		if ( null == this.sortedList) this.sortedList = new ArrayList<CellKeyValue<K1, V>>();
		else this.sortedList.clear();
		
		List<K1> allKeys = new ArrayList<K1>();
		keySet(allKeys);
		
		List<V> allValues = new ArrayList<V>(); 
		values(allValues);
		
		int allKeysT = allKeys.size();
		if ( allKeysT != allValues.size() ) throw new IOException( 
			"Keys and Values tally mismatched : keys(" + allKeysT + ") , values(" + allValues.size() + ")");
		
		for ( int i=0; i<allKeysT; i++) {
			sortedList.add( new CellKeyValue<K1, V>(allKeys.get(i), allValues.get(i)));
		}
	}
	
	public void remove(K1 key) {
		if ( null == this.sortedList) return;
		int elemIndex = this.sortedList.indexOf(key);
		if ( -1 == elemIndex) return;
		this.sortedList.remove(elemIndex);
	}

}

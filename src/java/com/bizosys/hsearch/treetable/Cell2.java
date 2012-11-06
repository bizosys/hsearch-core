package com.bizosys.hsearch.treetable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import com.bizosys.hsearch.byteutils.SortedByte;
import com.bizosys.hsearch.byteutils.SortedBytesArray;

public class Cell2<K1, V> {
	
	public SortedByte<K1> k1Sorter = null;
	public SortedByte<V> vSorter = null;
	public List<CellKeyValue<K1, V>> sortedList = null;
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
	
	public void findKeys( V exactValue, V minimumValue, V maximumValue, Collection<K1> foundKeys) throws IOException {
		byte[] allKeysB = SortedBytesArray.getInstance().getValueAt(data, 0);
		List<Integer> foundPositions = new ArrayList<Integer>();
		findMatchingPositions(exactValue, minimumValue, maximumValue, foundPositions);

		for (int position : foundPositions) {
			foundKeys.add( k1Sorter.getValueAt(allKeysB, position) );
		}
	}
	
	public void findValues( V exactValue, V minimumValue, V maximumValue, Collection<V> foundValues) throws IOException {
		List<Integer> foundPositions = new ArrayList<Integer>();
		byte[] allValuesB = findMatchingPositions(exactValue, minimumValue, maximumValue, foundPositions);

		getValuesAt(foundValues, foundPositions, allValuesB);
	}

	public void getValuesAt(Collection<V> foundValues,
			List<Integer> foundPositions, byte[] allValuesB) throws IOException {
		
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
	
	public void getAllKeys( Collection<K1> keys) throws IOException {
		
		byte[] allKeysB = SortedBytesArray.getInstance().getValueAt(data, 0);
		
		int length = ( null == allKeysB) ? 0 : allKeysB.length;
		int size = k1Sorter.getSize(allKeysB, 0, length);
		
		for ( int i=0; i<size; i++) {
			keys.add(k1Sorter.getValueAt(allKeysB, i));
		}
	}
	
	public void getAllValues( Collection<V> values) throws IOException {
		
		byte[] allValuesB = SortedBytesArray.getInstance().getValueAt(data, 1);
		
		int length = ( null == allValuesB) ? 0 : allValuesB.length;
		int size = vSorter.getSize(allValuesB, 0, length);
		
		for ( int i=0; i<size; i++) {
			values.add(vSorter.getValueAt(allValuesB, i));
		}
	}
	
	public void parseElements() throws IOException {
		if ( null == this.sortedList) this.sortedList = new ArrayList<CellKeyValue<K1, V>>();
		else this.sortedList.clear();
		
		List<K1> allKeys = new ArrayList<K1>();
		List<V> allValues = new ArrayList<V>();
		
		getAllKeys(allKeys);
		getAllValues(allValues);
		int allKeysT = allKeys.size();
		if ( allKeysT != allValues.size() ) throw new IOException( 
			"Keys and Values tally mismatched : keys(" + allKeysT + ") , values(" + allValues.size() + ")");
		
		for ( int i=0; i<allKeysT; i++) {
			sortedList.add( new CellKeyValue<K1, V>(allKeys.get(i), allValues.get(i)));
		}
	}
	
	public void delete (K1 key) {
		if ( null == this.sortedList) return;
		int elemIndex = this.sortedList.indexOf(key);
		if ( -1 == elemIndex) return;
		this.sortedList.remove(elemIndex);
	}

}

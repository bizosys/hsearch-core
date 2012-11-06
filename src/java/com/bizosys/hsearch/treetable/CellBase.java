package com.bizosys.hsearch.treetable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.bizosys.hsearch.byteutils.SortedByte;
import com.bizosys.hsearch.byteutils.SortedBytesArray;

public abstract class CellBase<K1> {
	public byte[] data;
	public SortedByte<K1> k1Sorter = null;

	public byte[] findMatchingPositions(K1 exactValue, K1 minimumValue, K1 maximumValue, Collection<Integer> foundPositions) throws IOException {
		
		byte[] allKeysB = SortedBytesArray.getInstance().getValueAt(data, 0);
		if ( null == allKeysB) return null;
			
		if ( null != exactValue || null != minimumValue || null != maximumValue ) {
				
			if ( null != exactValue ) {
				k1Sorter.getEqualToIndexes(allKeysB, exactValue, foundPositions);
			} else {
				if ( null != minimumValue && null != maximumValue ) {
					k1Sorter.getRangeIndexesInclusive(allKeysB, minimumValue, maximumValue, foundPositions);
				} else if ( null != minimumValue) {
					k1Sorter.getGreaterThanEqualToIndexes(allKeysB, minimumValue, foundPositions);
				} else {
					k1Sorter.getLessThanEqualToIndexes(allKeysB, maximumValue, foundPositions);
				}
			}
		}
		return allKeysB;
	}
	
	public void findKeys(K1 exactValue, K1 minimumValue,
			K1 maximumValue, Collection<K1> foundKeys) throws IOException {

		byte[] allKeysB = SortedBytesArray.getInstance().getValueAt(data, 0);
		List<Integer> foundPositions = new ArrayList<Integer>();
		findMatchingPositions(exactValue, minimumValue, maximumValue, foundPositions);

		for (int position : foundPositions) {
			foundKeys.add( k1Sorter.getValueAt(allKeysB, position) );
		}
		
	}
	
	public void getAllKeys(Collection<K1> keys) throws IOException {
		
		if ( null == data) {
			throw new IOException("Null Data - Use sortedList to get Keys directly");
		}
		
		byte[] allKeysB = SortedBytesArray.getInstance().getValueAt(data, 0);
		
		int length = ( null == allKeysB) ? 0 : allKeysB.length;
		int size = k1Sorter.getSize(allKeysB, 0, length);
		
		for ( int i=0; i<size; i++) {
			keys.add(k1Sorter.getValueAt(allKeysB, i));
		}
	}
	
	protected abstract Collection<byte[]> getEmbeddedCellBytes() throws IOException;
	protected abstract byte[] getKeyBytes() throws IOException;
	
	public byte[] toBytes() throws IOException {

		List<byte[]> bytesElems = new ArrayList<byte[]>();

		bytesElems.add(getKeyBytes());
		byte[] valB = SortedBytesArray.getInstance().toBytes(getEmbeddedCellBytes(), false);
		
		bytesElems.add(valB);
		byte[] cellB = SortedBytesArray.getInstance().toBytes(bytesElems, true);
		return cellB;
	}
	
	public byte[] delete(K1 exactValue, K1 minimumValue, K1 maximumValue) throws IOException {

		byte[] allKeysB = SortedBytesArray.getInstance().getValueAt(data, 0);
		Set<Integer> foundPositions = new HashSet<Integer>();
		findMatchingPositions(exactValue, minimumValue, maximumValue, foundPositions);
	
		int totalSize = k1Sorter.getSize(allKeysB, 0, allKeysB.length);
		List<K1> allKeys = new ArrayList<K1>(totalSize);
		List<byte[]> allValues = new ArrayList<byte[]>(totalSize);
		SortedByte<byte[]> sba = SortedBytesArray.getInstance();
		byte[] allValuesB = SortedBytesArray.getInstance().getValueAt(data, 1);
		
		for (int position = 0 ; position< totalSize; position++) {
			if ( foundPositions.contains(position)) continue;
			allKeys.add(k1Sorter.getValueAt(allKeysB, position) );
			allValues.add(sba.getValueAt(allValuesB, position));
		}
		
		List<byte[]> bytesElems = new ArrayList<byte[]>();
		bytesElems.add(k1Sorter.toBytes(allKeys, true));
		bytesElems.add(sba.toBytes(allValues, true));
		
		byte[] cellB = SortedBytesArray.getInstance().toBytes(bytesElems, false);
		return cellB;
	}


}

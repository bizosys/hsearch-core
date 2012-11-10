package com.bizosys.hsearch.treetable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.bizosys.hsearch.byteutils.ISortedByte;
import com.bizosys.hsearch.byteutils.SortedBytesArray;

public class Cell3<K1, K2, V> extends CellBase<K1> {

	public ISortedByte<K2> k2Sorter = null;
	public ISortedByte<V> vSorter = null;
	
	private Map<K1, Cell2<K2, V>> sortedList;
	
	public Cell3(ISortedByte<K1> k1Sorter, ISortedByte<K2> k2Sorter,
			ISortedByte<V> vSorter) {
		this.k1Sorter = k1Sorter;
		this.k2Sorter = k2Sorter;
		this.vSorter = vSorter;
	}

	public Cell3 (ISortedByte<K1> k1Sorter,ISortedByte<K2> k2Sorter, ISortedByte<V> vSorter, Map<K1, Cell2<K2, V>> sortedList ) {
		this(k1Sorter, k2Sorter, vSorter);
		this.sortedList = sortedList;
	}

	public Cell3 (ISortedByte<K1> k1Sorter,ISortedByte<K2> k2Sorter, ISortedByte<V> vSorter, byte[] data ) {
		this(k1Sorter, k2Sorter, vSorter);
		this.data = data;
	}
	
	//Builder
	
	public void put(K1 k1, K2 k2, V v) {
		if ( null == sortedList) {
			sortedList = new TreeMap<K1, Cell2<K2,V>>();
		}
		
		Cell2<K2,V> val = null;
		if ( sortedList.containsKey(k1)) val = sortedList.get(k1);
		else {
			val = new Cell2<K2, V>(k2Sorter, vSorter);
			sortedList.put(k1, val);
		}

		val.add(k2, v);
		
	}
	
	public void sort(Comparator<CellKeyValue<K2, V>> comp) {
		if ( null == sortedList) return;
		for (Cell2<K2,V> entry : sortedList.values()) {
			entry.sort(comp);
		}
	}
	

	public byte[] toBytes(Comparator<CellKeyValue<K2, V>> comp) throws IOException {
		this.sort(comp);
		return toBytes();
	}	
	
	public byte[] toBytes(V minValue, V maximumValue, boolean leftInclusize, boolean rightInclusize, Comparator<V> vComp) throws IOException {
		
		List<K1> keysL = new ArrayList<K1>(1);
		List<byte[]> valuesL = new ArrayList<byte[]>(1);

		for (K1 k : this.getMap().keySet()) {
			byte[] valueB = this.getMap().get(k).toBytes(minValue, maximumValue, leftInclusize, rightInclusize, vComp);
			if ( null == valueB) continue;
			keysL.add(k);
			valuesL.add(valueB);
		}
		
		if (keysL.size() == 0 ) return null;
		
		byte[] cellB = serializeKV(k1Sorter.toBytes(keysL), SortedBytesArray.getInstance().toBytes(valuesL));
		keysL.clear();
		valuesL.clear();

		return cellB;
	}		

	public Map<K1, Cell2<K2, V>> getMap(byte[] data) throws IOException {
		this.data = data;
		parseElements();
		return sortedList;
	}	

	public Map<K1, Cell2<K2, V>> getMap() throws IOException {
		if ( null != sortedList) return sortedList;
		if ( null != this.data) {
			parseElements();
			return sortedList;
		}
		throw new IOException("Cell is not initialized");
	}	
	
	
	public Collection<Cell2<K2, V>> values(K1 exactValue) throws IOException {
		Collection<Cell2<K2, V>> values = new ArrayList<Cell2<K2, V>>();
		values(exactValue, null, null, values);
		return values;
	}	
	
	public Collection<Cell2<K2, V>> values(K1 minimumValue, K1 maximumValue) throws IOException {
		Collection<Cell2<K2, V>> values = new ArrayList<Cell2<K2, V>>();
		values(null, minimumValue, maximumValue, values);
		return values;
	}		
	
	public void values(K1 exactValue, Collection<Cell2<K2, V>> foundValues) throws IOException {
		values(exactValue, null, null, foundValues);
	}
	
	public void values(K1 minimumValue, K1 maximumValue, Collection<Cell2<K2, V>> foundValues) throws IOException {
		values(null, minimumValue, maximumValue, foundValues);
	}
	
	public void values(K1 exactValue, K1 minimumValue, K1 maximumValue, 
			Collection<Cell2<K2, V>> foundValues) throws IOException {

		List<Integer> foundPositions = new ArrayList<Integer>();
		findMatchingPositions(exactValue, minimumValue, maximumValue, foundPositions);

		ISortedByte<byte[]> sortedBA = SortedBytesArray.getInstance();
		byte[] valuesB = sortedBA.parse(data).getValueAt(1);

		sortedBA.parse(valuesB);
		for (int position : foundPositions) {
			foundValues.add( new Cell2<K2, V>(
				k2Sorter, vSorter, sortedBA.getValueAt(position)));
		}
	}
	
	public Collection<Cell2<K2, V>> values() throws IOException {
		Collection<Cell2<K2, V>> values = new ArrayList<Cell2<K2, V>>();
		values(values);
		return values;
	}	
	
	public void values(Collection<Cell2<K2, V>> values) throws IOException {
		ISortedByte<byte[]> sortedBA = SortedBytesArray.getInstance();
		byte[] allValuesB = sortedBA.parse(data).getValueAt(1);
		
		if ( null == allValuesB) return;
		
		sortedBA.parse(allValuesB);
		int size = sortedBA.getSize();
		for ( int i=0; i<size; i++) {
			values.add( new Cell2<K2, V>( k2Sorter, vSorter, sortedBA.getValueAt(i)) );
		}
	}
	
	public void parseElements() throws IOException {
		if ( null == this.sortedList) this.sortedList = new TreeMap<K1, Cell2<K2, V>>();
		else this.sortedList.clear();
		
		List<K1> allKeys = new ArrayList<K1>();
		List<Cell2<K2,V>> allValues = new ArrayList<Cell2<K2,V>>();
		
		keySet(allKeys);
		values(allValues);

		int allKeysT = allKeys.size();
		if ( allKeysT != allValues.size() ) throw new IOException( 
			"Keys and Values tally mismatched : keys(" + allKeysT + ") , values(" + allValues.size() + ")");
		
		for ( int i=0; i<allKeysT; i++) {
			sortedList.put(allKeys.get(i), allValues.get(i));
		}
	}
	
	@Override
	protected List<byte[]> getEmbeddedCellBytes() throws IOException {
		List<byte[]> values = new ArrayList<byte[]>();
		for (Cell2<K2, V> cell2 : sortedList.values()) {
			values.add(cell2.toBytes());
		}
		return values;
	}
	
	@Override
	protected byte[] getKeyBytes() throws IOException {
		return k1Sorter.toBytes(sortedList.keySet());
	}

}

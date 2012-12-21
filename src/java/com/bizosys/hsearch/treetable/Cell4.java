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
import com.bizosys.hsearch.hbase.ObjectFactory;

public class Cell4<K1, K2, K3, V> extends CellBase<K1> {

	public ISortedByte<K2> k2Sorter = null;
	public ISortedByte<K3> k3Sorter = null;
	public ISortedByte<V> vSorter = null;
	
	private Map<K1, Cell3<K2,K3,V>> sortedList;
	
	public Cell4 (ISortedByte<K1> k1Sorter, ISortedByte<K2> k2Sorter,
			ISortedByte<K3> k3Sorter, ISortedByte<V> vSorter) {
		this.k1Sorter = k1Sorter;
		this.k2Sorter = k2Sorter;
		this.k3Sorter = k3Sorter;
		this.vSorter = vSorter;
	}

	public Cell4 (ISortedByte<K1> k1Sorter,ISortedByte<K2> k2Sorter, ISortedByte<K3> k3Sorter, 
			ISortedByte<V> vSorter, Map<K1, Cell3<K2,K3,V>> sortedList ) {
		this(k1Sorter, k2Sorter, k3Sorter, vSorter);
		this.sortedList = sortedList;
	}

	public Cell4 (ISortedByte<K1> k1Sorter,ISortedByte<K2> k2Sorter, ISortedByte<K3> k3Sorter, ISortedByte<V> vSorter, byte[] data ) {
		this(k1Sorter, k2Sorter, k3Sorter, vSorter);
		this.data = data;
	}
	
	//Builder
	
	public void put(K1 k1, K2 k2, K3 k3, V v) {
		if ( null == sortedList) {
			sortedList = new TreeMap<K1, Cell3<K2,K3,V>>();
		}
			
		Cell3<K2,K3,V> val = null;
		if ( sortedList.containsKey(k1)) val = sortedList.get(k1);
		else {
			val = new Cell3<K2, K3, V>(k2Sorter, k3Sorter, vSorter);
			sortedList.put(k1, val);
		}
		val.put(k2, k3, v);
	}
	
	public void sort(Comparator<CellKeyValue<K3, V>> comp) {
		if ( null == sortedList) return;
		for (Cell3<K2,K3,V> entry : sortedList.values()) {
			entry.sort(comp);
		}
	}	
	
	public byte[] toBytes(Comparator<CellKeyValue<K3, V>> comp) throws IOException {
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
		
		
		byte[] cellB = super.serializeKV(k1Sorter.toBytes(keysL) , SortedBytesArray.getInstance().toBytes(valuesL));
		keysL.clear(); valuesL.clear();
		return cellB;
	}		

	public void getMap(K1 exactValue, K1 minimumValue, K1 maximumValue, Map<K1, Cell3<K2, K3, V>> rows) throws IOException 
	{
		List<Integer> foundPositions = ObjectFactory.getInstance().getIntegerList();
		findMatchingPositions(exactValue, minimumValue, maximumValue, foundPositions);

		ISortedByte<byte[]> dataBytesA = SortedBytesArray.getInstance();
		ISortedByte<byte[]>  dataA = dataBytesA.parse(data);
		byte[] valuesB = dataA.getValueAt(1);
		byte[] keysB = dataA.getValueAt(0);

		ISortedByte<byte[]> valuesA = SortedBytesArray.getInstance().parse(valuesB);
		ISortedByte<K1> keysA = k1Sorter.parse(keysB);

		for (int position : foundPositions) {
			Cell3<K2, K3, V> cell3 = new Cell3<K2, K3, V>(
					k2Sorter, k3Sorter, vSorter, valuesA.getValueAt(position) );
			rows.put( keysA.getValueAt(position), cell3);
		}
		
		ObjectFactory.getInstance().putIntegerList(foundPositions);
	}
	
	public Map<K1, Cell3<K2, K3, V>> getMap(byte[] data) throws IOException {
		this.data = data;
		parseElements();
		return sortedList;
	}
	
	public Map<K1, Cell3<K2, K3, V>> getMap() throws IOException {
		if ( null != sortedList) return sortedList;
		if ( null != this.data) {
			parseElements();
			return sortedList;
		}
		throw new IOException("Cell is not initialized");
	}		
	
	public Collection<Cell3<K2, K3, V>> values(K1 exactValue) throws IOException {
		Collection<Cell3<K2, K3, V>> values = new ArrayList<Cell3<K2, K3, V>>();
		values(exactValue, null, null, values);
		return values;
	}
	
	public Collection<Cell3<K2, K3, V>> values(K1 minimumValue, K1 maximumValue) throws IOException {
		Collection<Cell3<K2, K3, V>> values = new ArrayList<Cell3<K2, K3, V>>();
		values(null, minimumValue, maximumValue, values);
		return values;
	}

	public void values(K1 exactValue, Collection<Cell3<K2, K3, V>> foundValues) throws IOException {
		values(exactValue, null, null, foundValues);
	}
	
	public void values(K1 minimumValue, K1 maximumValue, Collection<Cell3<K2, K3, V>> foundValues) throws IOException {
		values(null, minimumValue, maximumValue, foundValues);
	}
	
	private void values(K1 exactValue, K1 minimumValue, K1 maximumValue, 
			Collection<Cell3<K2, K3, V>> foundValues) throws IOException {

		if ( null == this.data) return; 
		
		List<Integer> foundPositions = new ArrayList<Integer>();
		findMatchingPositions(exactValue, minimumValue, maximumValue, foundPositions);

		ISortedByte<byte[]> sortedBA = SortedBytesArray.getInstance();
		byte[] valuesB = sortedBA.parse(data).getValueAt(1);

		for (int position : foundPositions) {
			foundValues.add( new Cell3<K2, K3, V>(
				k2Sorter, k3Sorter, vSorter, sortedBA.parse(valuesB).getValueAt(position)));
		}
	}
	

	public Collection<Cell3<K2, K3, V>> values() throws IOException {
		Collection<Cell3<K2, K3, V>> values = new ArrayList<Cell3<K2, K3, V>>();
		values(values);
		return values;
	}
	
	public void values(Collection<Cell3<K2, K3, V>> values) throws IOException {
		ISortedByte<byte[]> sortedBA = SortedBytesArray.getInstance();
		byte[] allValuesB = sortedBA.parse(data).getValueAt(1);
		int size = sortedBA.parse( allValuesB).getSize();
		
		for ( int i=0; i<size; i++) {
			values.add( new Cell3<K2, K3, V>( k2Sorter, k3Sorter, vSorter, sortedBA.getValueAt(i)) );
		}
	}
	
	public void parseElements() throws IOException {
		if ( null == this.sortedList) this.sortedList = new TreeMap<K1, Cell3<K2,K3,V>>();
		else this.sortedList.clear();
		
		List<K1> allKeys = new ArrayList<K1>();
		List<Cell3<K2,K3,V>> allValues = new ArrayList<Cell3<K2,K3,V>>();
		
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
		for (Cell3<K2,K3,V> cell3 : sortedList.values()) {
			values.add(cell3.toBytes());
		}
		return values;
	}
	
	
	@Override
	protected byte[] getKeyBytes() throws IOException {
		return k1Sorter.toBytes(sortedList.keySet());
	}

}

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
public class Cell9< K1, K2, K3, K4, K5, K6, K7, K8,V> extends CellBase<K1> {
	public ISortedByte<K2> k2Sorter = null;
	public ISortedByte<K3> k3Sorter = null;
	public ISortedByte<K4> k4Sorter = null;
	public ISortedByte<K5> k5Sorter = null;
	public ISortedByte<K6> k6Sorter = null;
	public ISortedByte<K7> k7Sorter = null;
	public ISortedByte<K8> k8Sorter = null;
	
	public ISortedByte<V> vSorter = null;
	
	private Map<K1, Cell8< K2, K3, K4, K5, K6, K7, K8,V>> sortedList;
	public Cell9 (ISortedByte<K1> k1Sorter,ISortedByte<K2> k2Sorter,ISortedByte<K3> k3Sorter,ISortedByte<K4> k4Sorter,ISortedByte<K5> k5Sorter,ISortedByte<K6> k6Sorter,ISortedByte<K7> k7Sorter,ISortedByte<K8> k8Sorter, ISortedByte<V> vSorter) {
		this.k1Sorter = k1Sorter;
		this.k2Sorter = k2Sorter;
		this.k3Sorter = k3Sorter;
		this.k4Sorter = k4Sorter;
		this.k5Sorter = k5Sorter;
		this.k6Sorter = k6Sorter;
		this.k7Sorter = k7Sorter;
		this.k8Sorter = k8Sorter;
		
		this.vSorter = vSorter;
	}
	
	public Cell9 (ISortedByte<K1> k1Sorter,ISortedByte<K2> k2Sorter,ISortedByte<K3> k3Sorter,ISortedByte<K4> k4Sorter,ISortedByte<K5> k5Sorter,ISortedByte<K6> k6Sorter,ISortedByte<K7> k7Sorter,ISortedByte<K8> k8Sorter, 
			ISortedByte<V> vSorter, Map<K1, Cell8< K2, K3, K4, K5, K6, K7, K8,V>> sortedList ) {
		this(k1Sorter,k2Sorter,k3Sorter,k4Sorter,k5Sorter,k6Sorter,k7Sorter,k8Sorter,vSorter);
		this.sortedList = sortedList;
	}
	public Cell9 (ISortedByte<K1> k1Sorter,ISortedByte<K2> k2Sorter,ISortedByte<K3> k3Sorter,ISortedByte<K4> k4Sorter,ISortedByte<K5> k5Sorter,ISortedByte<K6> k6Sorter,ISortedByte<K7> k7Sorter,ISortedByte<K8> k8Sorter,
			ISortedByte<V> vSorter, byte[] data ) {
		this(k1Sorter,k2Sorter,k3Sorter,k4Sorter,k5Sorter,k6Sorter,k7Sorter,k8Sorter,vSorter);
		this.data = data;
	}
	
	//Builder
	public void put(K1 k1,K2 k2,K3 k3,K4 k4,K5 k5,K6 k6,K7 k7,K8 k8, V v) {
		if ( null == sortedList) sortedList = new TreeMap<K1, Cell8< K2, K3, K4, K5, K6, K7, K8,V> >();
		
		Cell8< K2, K3, K4, K5, K6, K7, K8,V> val = null;
		if ( sortedList.containsKey(k1)) val = sortedList.get(k1);
		else {
			val = new Cell8< K2, K3, K4, K5, K6, K7, K8,V>(k2Sorter,k3Sorter,k4Sorter,k5Sorter,k6Sorter,k7Sorter,k8Sorter, vSorter);
			sortedList.put(k1, val);
		}
		
		sortedList.put(k1, val);
		val.put(k2,k3,k4,k5,k6,k7,k8, v);
	}
	
	public void sort(Comparator<CellKeyValue<K8, V>> comp) {
		if ( null == sortedList) return;
		for (Cell8< K2, K3, K4, K5, K6, K7, K8,V> entry : sortedList.values()) {
			entry.sort(comp);
		}
	}	
	
	public byte[] toBytes(Comparator<CellKeyValue<K8, V>> comp) throws IOException {
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
		
		byte[] cellB =  serializeKV(k1Sorter.toBytes(keysL) , SortedBytesArray.getInstance().toBytes(valuesL));
		return cellB;
	}		
	public Map<K1, Cell8< K2, K3, K4, K5, K6, K7, K8,V>> getMap(byte[] data) throws IOException {
		this.data = data;
		parseElements();
		return sortedList;
	}	
	
	public Map<K1, Cell8< K2, K3, K4, K5, K6, K7, K8,V>> getMap() throws IOException {
		if ( null != sortedList) return sortedList;
		if ( null != this.data) {
			parseElements();
			return sortedList;
		}
		throw new IOException("Cell is not initialized");
	}		
	
	/**
	 * Find matching exact value
	 * @param exactValue
	 * @return
	 * @throws IOException
	 */
	public Collection<Cell8< K2, K3, K4, K5, K6, K7, K8,V>> values(K1 exactValue) throws IOException {
		Collection<Cell8< K2, K3, K4, K5, K6, K7, K8,V>> values = new ArrayList<Cell8< K2, K3, K4, K5, K6, K7, K8,V>>();
		values(exactValue, null, null, values);
		return values;
	}
	public Collection<Cell8< K2, K3, K4, K5, K6, K7, K8,V>> values(K1 minimumValue, K1 maximumValue) throws IOException {
		Collection<Cell8< K2, K3, K4, K5, K6, K7, K8,V>> values = new ArrayList<Cell8< K2, K3, K4, K5, K6, K7, K8,V>>();
		values(null, minimumValue, maximumValue, values);
		return values;
	}	
	
	public void values(K1 exactValue, Collection<Cell8< K2, K3, K4, K5, K6, K7, K8,V>> foundValues) throws IOException {
		values(exactValue, null, null, foundValues);
	}
	
	public void values(K1 minimumValue, K1 maximumValue, Collection<Cell8< K2, K3, K4, K5, K6, K7, K8,V>> foundValues) throws IOException {
		values(null, minimumValue, maximumValue, foundValues);
	}
	
	private void values(K1 exactValue, K1 minimumValue, K1 maximumValue, 
			Collection<Cell8< K2, K3, K4, K5, K6, K7, K8,V>> foundValues) throws IOException {
		List<Integer> foundPositions = new ArrayList<Integer>();
		findMatchingPositions(exactValue, minimumValue, maximumValue, foundPositions);
		ISortedByte<byte[]> sortedBA = SortedBytesArray.getInstance();
		byte[] valuesB = sortedBA.parse(data).getValueAt(1);
		sortedBA.parse(valuesB);
		for (int position : foundPositions) {
			foundValues.add( new Cell8< K2, K3, K4, K5, K6, K7, K8,V>(
				k2Sorter,k3Sorter,k4Sorter,k5Sorter,k6Sorter,k7Sorter,k8Sorter, vSorter, sortedBA.getValueAt(position)));
		}
	}
	
	public Collection<Cell8< K2, K3, K4, K5, K6, K7, K8,V>> values() throws IOException {
		Collection<Cell8< K2, K3, K4, K5, K6, K7, K8,V>> values = new ArrayList<Cell8< K2, K3, K4, K5, K6, K7, K8,V>>();
		values(values);
		return values;
	}	
	
	public void values(Collection<Cell8< K2, K3, K4, K5, K6, K7, K8,V>> values) throws IOException {
		ISortedByte<byte[]> sortedBA = SortedBytesArray.getInstance();
		byte[] allValuesB = sortedBA.parse(data).getValueAt(1);
		
		if ( null == allValuesB) return;
		int size = sortedBA.parse(allValuesB).getSize();
		
		sortedBA.parse(allValuesB);
		for ( int i=0; i<size; i++) {
			values.add( new Cell8< K2, K3, K4, K5, K6, K7, K8,V>( k2Sorter,k3Sorter,k4Sorter,k5Sorter,k6Sorter,k7Sorter,k8Sorter, vSorter, sortedBA.getValueAt(i)) );
		}
	}
	
	public void parseElements() throws IOException {
		if ( null == this.sortedList) this.sortedList = new TreeMap<K1, Cell8< K2, K3, K4, K5, K6, K7, K8,V>>();
		else this.sortedList.clear();
		
		List<K1> allKeys = new ArrayList<K1>();
		List<Cell8< K2, K3, K4, K5, K6, K7, K8,V>> allValues = new ArrayList<Cell8< K2, K3, K4, K5, K6, K7, K8,V>>();
		
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
		for (Cell8< K2, K3, K4, K5, K6, K7, K8,V> cell8 : sortedList.values()) {
			values.add(cell8.toBytes());
		}
		return values;
	}
	
	@Override
	protected byte[] getKeyBytes() throws IOException {
		return k1Sorter.toBytes(sortedList.keySet());
	}
	
}


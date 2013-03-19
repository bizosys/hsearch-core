package com.bizosys.hsearch.treetable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import com.bizosys.hsearch.byteutils.ISortedByte;
import com.bizosys.hsearch.byteutils.SortedBytesArray;
import com.bizosys.hsearch.byteutils.SortedBytesBase.Reference;
import com.bizosys.hsearch.hbase.ObjectFactory;

public class Cell4< K1, K2, K3,V> extends CellBase<K1> {
	public ISortedByte<K2> k2Sorter = null;
	public ISortedByte<K3> k3Sorter = null;
	
	public ISortedByte<V> vSorter = null;
	
	public Map<K1, Cell3< K2, K3,V>> sortedList;
	public Cell4 (ISortedByte<K1> k1Sorter,ISortedByte<K2> k2Sorter,ISortedByte<K3> k3Sorter, ISortedByte<V> vSorter) {
		this.k1Sorter = k1Sorter;
		this.k2Sorter = k2Sorter;
		this.k3Sorter = k3Sorter;
		
		this.vSorter = vSorter;
	}
	
	public Cell4 (ISortedByte<K1> k1Sorter,ISortedByte<K2> k2Sorter,ISortedByte<K3> k3Sorter, 
			ISortedByte<V> vSorter, Map<K1, Cell3< K2, K3,V>> sortedList ) {
		this(k1Sorter,k2Sorter,k3Sorter,vSorter);
		this.sortedList = sortedList;
	}
	public Cell4 (ISortedByte<K1> k1Sorter,ISortedByte<K2> k2Sorter,ISortedByte<K3> k3Sorter,
			ISortedByte<V> vSorter, byte[] data ) {
		this(k1Sorter,k2Sorter,k3Sorter,vSorter);
		int len = ( null == data) ? 0 : data.length;
		this.data = new BytesSection(data, 0, len);
	}
	
	public Cell4 (ISortedByte<K1> k1Sorter,ISortedByte<K2> k2Sorter,ISortedByte<K3> k3Sorter,
			ISortedByte<V> vSorter, BytesSection data ) {
		this(k1Sorter,k2Sorter,k3Sorter,vSorter);
		this.data = data;
	}
	//Builder
	public void put(K1 k1,K2 k2,K3 k3, V v) {
		if ( null == sortedList) sortedList = new TreeMap<K1, Cell3< K2, K3,V> >();
		
		Cell3< K2, K3,V> val = null;
		if ( sortedList.containsKey(k1)) val = sortedList.get(k1);
		else {
			val = new Cell3< K2, K3,V>(k2Sorter,k3Sorter, vSorter);
			sortedList.put(k1, val);
		}
		val.put(k2,k3, v);
	}
	
	public void sort(Comparator<CellKeyValue<K3, V>> comp) {
		if ( null == sortedList) return;
		for (Cell3< K2, K3,V> entry : sortedList.values()) {
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
		
		byte[] cellB =  serializeKV(k1Sorter.toBytes(keysL) , SortedBytesArray.getInstance().toBytes(valuesL));
		keysL.clear();
		valuesL.clear();
		return cellB;
	}		
	public Map<K1, Cell3< K2, K3,V>> getMap(byte[] data) throws IOException {
		if ( null == data) return null;
		this.data = new BytesSection(data, 0, data.length);
		parseElements();
		return sortedList;
	}	
	
	public Map<K1, Cell3< K2, K3,V>> getMap() throws IOException {
		if ( null != sortedList) return sortedList;
		if ( null != this.data) {
			parseElements();
			return sortedList;
		}
		throw new IOException("Cell is not initialized");
	}
	
	public void getMap(K1 exactValue, K1 minimumValue, K1 maximumValue, Map<K1, Cell3< K2, K3,V>> rows) throws IOException 
	{
		if ( null == data) {
			System.err.println("Null Data - It should be an warning");
			return;
		}
		
		SortedBytesArray kvbytesA =  SortedBytesArray.getInstanceArr();
		kvbytesA.parse(data.data, data.offset, data.length);
		Reference keyRef = new Reference();
		kvbytesA.getValueAtReference(0, keyRef);
		
		Reference valRef = new Reference();
		kvbytesA.getValueAtReference(1, valRef);
		
		ISortedByte<byte[]> valSorter = SortedBytesArray.getInstance();
		valSorter.parse(data.data, valRef.offset, valRef.length);
		
		Callback callback = new Callback(rows, valSorter);
		findMatchingPositions(exactValue, minimumValue, maximumValue, callback);
	}
				
	
	/**
	 * Find matching exact value
	 * @param exactValue
	 * @return
	 * @throws IOException
	 */
	public Collection<Cell3< K2, K3,V>> values(K1 exactValue) throws IOException {
		Collection<Cell3< K2, K3,V>> values = new ArrayList<Cell3< K2, K3,V>>();
		values(exactValue, null, null, values);
		return values;
	}
	public Collection<Cell3< K2, K3,V>> values(K1 minimumValue, K1 maximumValue) throws IOException {
		Collection<Cell3< K2, K3,V>> values = new ArrayList<Cell3< K2, K3,V>>();
		values(null, minimumValue, maximumValue, values);
		return values;
	}	
	
	public void values(K1 exactValue, Collection<Cell3< K2, K3,V>> foundValues) throws IOException {
		values(exactValue, null, null, foundValues);
	}
	
	public void values(K1 minimumValue, K1 maximumValue, Collection<Cell3< K2, K3,V>> foundValues) throws IOException {
		values(null, minimumValue, maximumValue, foundValues);
	}
	
	private void values(K1 exactValue, K1 minimumValue, K1 maximumValue, 
			Collection<Cell3< K2, K3,V>> foundValues) throws IOException {
		if ( null == data) {
			System.err.println("Null Data - It should be an warning");
			return;
		}
		
		List<Integer> foundPositions = ObjectFactory.getInstance().getIntegerList();
		findMatchingPositions(exactValue, minimumValue, maximumValue, foundPositions);
		SortedBytesArray kvbytesA =  SortedBytesArray.getInstanceArr();
		kvbytesA.parse(data.data, data.offset, data.length);
		Reference valRef = new Reference();
		kvbytesA.getValueAtReference(1, valRef);
		
		SortedBytesArray valSorter = SortedBytesArray.getInstanceArr();
		valSorter.parse(data.data, valRef.offset, valRef.length);
		Reference ref = new Reference();
		for (int position : foundPositions) {
			valSorter.getValueAtReference(position, ref);
			BytesSection sec = new BytesSection(data.data, ref.offset, ref.length);
			foundValues.add( new Cell3< K2, K3,V>(
				k2Sorter,k3Sorter, vSorter, sec));
		}
		ObjectFactory.getInstance().putIntegerList(foundPositions);
	}
	
	public Collection<Cell3< K2, K3,V>> values() throws IOException {
		Collection<Cell3< K2, K3,V>> values = new ArrayList<Cell3< K2, K3,V>>();
		values(values);
		return values;
	}	
	
	public void values(Collection<Cell3< K2, K3,V>> values) throws IOException {
		if ( null == data) {
			System.err.println("Null Data - It should be an warning");
			return;
		}
		SortedBytesArray kvbytesA =  SortedBytesArray.getInstanceArr();
		kvbytesA.parse(data.data, data.offset, data.length);
		Reference valRef = new Reference();
		kvbytesA.getValueAtReference(1, valRef);
		SortedBytesArray valSorter = SortedBytesArray.getInstanceArr();
		valSorter.parse(data.data, valRef.offset, valRef.length);
		int size = valSorter.getSize();
		Reference ref = new Reference();
		for ( int i=0; i<size; i++) {
			valSorter.getValueAtReference(i, ref);
			BytesSection sec = new BytesSection(data.data, ref.offset, ref.length);
			values.add( new Cell3< K2, K3,V>( k2Sorter,k3Sorter, vSorter, sec) );
		}
	}
	
	public void parseElements() throws IOException {
		if ( null == this.sortedList) this.sortedList = new TreeMap<K1, Cell3< K2, K3,V>>();
		else this.sortedList.clear();
		
		List<K1> allKeys = new ArrayList<K1>();
		keySet(allKeys);
		int allKeysT = allKeys.size();
		List<Cell3< K2, K3,V>> allValues = new ArrayList<Cell3< K2, K3,V>>(allKeysT);
		values(allValues);
	
		if ( allKeysT != allValues.size() ) throw new IOException( 
			"Keys and Values tally mismatched : keys(" + allKeysT + ") , values(" + allValues.size() + ")");
		
		Iterator<K1> itrKey = allKeys.iterator();
		Iterator<Cell3< K2, K3,V>> itrVal = allValues.iterator();
		while ( itrKey.hasNext() ) {
			sortedList.put(itrKey.next(), itrVal.next());
		}
	}
	
	@Override
	protected List<byte[]> getEmbeddedCellBytes() throws IOException {
		List<byte[]> values = new ArrayList<byte[]>();
		for (Cell3< K2, K3,V> cell3 : sortedList.values()) {
			values.add(cell3.toBytes());
		}
		return values;
	}
	
	@Override
	protected byte[] getKeyBytes() throws IOException {
		if ( null == sortedList) throw new IOException("Cell is not initialized");
		return k1Sorter.toBytes(sortedList.keySet());
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public void valuesUnchecked(K1 exactValue, K1 minimumValue, K1 maximumValue, Collection foundValues) throws IOException {
		this.values(exactValue, minimumValue, maximumValue, foundValues );
	}
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public void valuesUnchecked(Collection foundValues) throws IOException {
		this.values(foundValues );
	}
	
	
	public final class Callback extends EmptyList {
		
		public ISortedByte<byte[]> valSorter;
		Map<K1, Cell3< K2, K3,V>> rows;
		
		public Callback(Map<K1, Cell3< K2, K3,V>> rows, ISortedByte<byte[]> valSorter ) {
			this.rows = rows;
			this.valSorter = valSorter;
		}
		
		@Override
		public final boolean add(Integer position) {
			SortedBytesArray sortedArr = (SortedBytesArray) valSorter;
			Reference valueSectionPoints = new Reference();
			sortedArr.getValueAtReference(position, valueSectionPoints);
			BytesSection valueSection = new BytesSection(
				data.data, valueSectionPoints.offset, valueSectionPoints.length);
			
			Cell3< K2, K3,V> cell = new Cell3< K2, K3,V>(
				k2Sorter,k3Sorter, vSorter, valueSection);
			
			rows.put( k1Sorter.getValueAt(position), cell);
			return true;
		}
	};

	@Override
	public String toString() {
		if ( null == sortedList) try {parseElements();} catch (Exception e) {return e.getMessage();};
		return sortedList.toString();
	}
	
}


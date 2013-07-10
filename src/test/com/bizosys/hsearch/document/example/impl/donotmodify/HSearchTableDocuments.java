package com.bizosys.hsearch.document.example.impl.donotmodify;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.bizosys.hsearch.byteutils.SortedBytesArray;
import com.bizosys.hsearch.byteutils.SortedBytesDouble;
import com.bizosys.hsearch.byteutils.SortedBytesLong;
import com.bizosys.hsearch.byteutils.SortedBytesFloat;
import com.bizosys.hsearch.byteutils.SortedBytesInteger;
import com.bizosys.hsearch.byteutils.SortedBytesUnsignedShort;
import com.bizosys.hsearch.byteutils.SortedBytesChar;
import com.bizosys.hsearch.byteutils.SortedBytesBoolean;
import com.bizosys.hsearch.byteutils.SortedBytesString;
import com.bizosys.hsearch.byteutils.Storable;

import com.bizosys.hsearch.document.example.impl.*;

import com.bizosys.hsearch.treetable.Cell2;
import com.bizosys.hsearch.treetable.Cell3;
import com.bizosys.hsearch.treetable.Cell4;
import com.bizosys.hsearch.treetable.Cell5;
import com.bizosys.hsearch.treetable.Cell6;

import com.bizosys.hsearch.treetable.BytesSection;
import com.bizosys.hsearch.treetable.Cell2Visitor;
import com.bizosys.hsearch.treetable.CellBase;

import com.bizosys.hsearch.treetable.client.HSearchQuery;
import com.bizosys.hsearch.treetable.client.IHSearchPlugin;
import com.bizosys.hsearch.treetable.client.IHSearchTable;

import com.bizosys.hsearch.treetable.unstructured.IIndexFrequencyPayloadTable;
import com.bizosys.hsearch.treetable.unstructured.IIndexOffsetTable;
import com.bizosys.hsearch.treetable.unstructured.IIndexFrequencyTable;
import com.bizosys.hsearch.treetable.unstructured.IIndexPositionsTable;

import com.bizosys.hsearch.treetable.CellComparator;
import com.bizosys.hsearch.treetable.CellComparator.BooleanComparator;
import com.bizosys.hsearch.treetable.CellComparator.ByteComparator;
import com.bizosys.hsearch.treetable.CellComparator.BytesComparator;
import com.bizosys.hsearch.treetable.CellComparator.ShortComparator;
import com.bizosys.hsearch.treetable.CellComparator.IntegerComparator;
import com.bizosys.hsearch.treetable.CellComparator.FloatComparator;
import com.bizosys.hsearch.treetable.CellComparator.LongComparator;
import com.bizosys.hsearch.treetable.CellComparator.DoubleComparator;
import com.bizosys.hsearch.treetable.CellComparator.StringComparator;
import com.bizosys.hsearch.treetable.ValueComparator;
import com.bizosys.hsearch.util.EmptyMap;

public final class HSearchTableDocuments implements IIndexFrequencyPayloadTable {
	
	public static boolean DEBUG_ENABLED = false;
	
	public static final int MODE_COLS = 0;
    public static final int MODE_KEY = 1;
    public static final int MODE_VAL = 2;
    public static final int MODE_KEYVAL = 3;
	
	
	public static final class Cell5Map
		 extends EmptyMap<Integer, Cell5<Integer, byte[], Integer, Integer, Integer>> {

	public HSearchQuery query;
	public Cell2FilterVisitor cell2Visitor;
	public Integer matchingCell1;
	public Integer cellMin1; 
	public Integer cellMax1;
	public Integer[] inValues1;
	public Map<Integer, Cell4<byte[], Integer, Integer, Integer>> cell4L = null;

	public Cell5Map(final HSearchQuery query, final Cell2FilterVisitor cell2Visitor, final Integer matchingCell5, final Integer cellMin5, final Integer cellMax5, final Integer[] inValues5, final Integer matchingCell3, final Integer cellMin3, final Integer cellMax3, final Integer[] inValues3, final byte[] matchingCell2, final byte[] cellMin2, final byte[] cellMax2, final byte[][] inValues2, final Integer matchingCell1, final Integer cellMin1, final Integer cellMax1, final Integer[] inValues1) {
		this.query = query; 
		this.cell2Visitor = cell2Visitor;
		this.matchingCell1 = matchingCell1;
		this.cellMin1 = cellMin1;
		this.cellMax1 = cellMax1;
		this.inValues1 = inValues1;
		this.cell4L = new Cell4Map(query, cell2Visitor,matchingCell5, cellMin5, cellMax5, inValues5,matchingCell3, cellMin3, cellMax3, inValues3,matchingCell2, cellMin2, cellMax2, inValues2);
	}
	@Override
	public final Cell5<Integer, byte[], Integer, Integer, Integer> put( final Integer key, final Cell5<Integer, byte[], Integer, Integer, Integer> value) {
	try {
		cell2Visitor.cell5Key = key;
		if (query.filterCells[1]) {
			if (query.notValCells[1])
				value.getNotMap(matchingCell1, cell4L);
			else if (query.inValCells[1])
				value.getInMap(inValues1, cell4L);
			else
			value.getMap(matchingCell1, cellMin1, cellMax1, cell4L);
		 } else {
			value.sortedList = cell4L;
			value.parseElements();
		}
		return value;
		} catch (IOException e) {
			throw new IndexOutOfBoundsException(e.getMessage());
		}
	}
}


public static final class Cell4Map
		 extends EmptyMap<Integer, Cell4<byte[], Integer, Integer, Integer>> {

	public HSearchQuery query;
	public Cell2FilterVisitor cell2Visitor;
	public byte[] matchingCell2;
	public byte[] cellMin2; 
	public byte[] cellMax2;
	public byte[][] inValues2;
	public Map<byte[], Cell3<Integer, Integer, Integer>> cell3L = null;

	public Cell4Map(final HSearchQuery query, final Cell2FilterVisitor cell2Visitor, final Integer matchingCell5, final Integer cellMin5, final Integer cellMax5, final Integer[] inValues5, final Integer matchingCell3, final Integer cellMin3, final Integer cellMax3, final Integer[] inValues3, final byte[] matchingCell2, final byte[] cellMin2, final byte[] cellMax2, final byte[][] inValues2) {
		this.query = query; 
		this.cell2Visitor = cell2Visitor;
		this.matchingCell2 = matchingCell2;
		this.cellMin2 = cellMin2;
		this.cellMax2 = cellMax2;
		this.inValues2 = inValues2;
		this.cell3L = new Cell3Map(query, cell2Visitor,matchingCell5, cellMin5, cellMax5, inValues5,matchingCell3, cellMin3, cellMax3, inValues3);
	}
	@Override
	public final Cell4<byte[], Integer, Integer, Integer> put( final Integer key, final Cell4<byte[], Integer, Integer, Integer> value) {
	try {
		cell2Visitor.cell4Key = key;
		if (query.filterCells[2]) {
			if (query.notValCells[2])
				value.getNotMap(matchingCell2, cell3L);
			else if (query.inValCells[2])
				value.getInMap(inValues2, cell3L);
			else
			value.getMap(matchingCell2, cellMin2, cellMax2, cell3L);
		 } else {
			value.sortedList = cell3L;
			value.parseElements();
		}
		return value;
		} catch (IOException e) {
			throw new IndexOutOfBoundsException(e.getMessage());
		}
	}
}


public static final class Cell3Map
		 extends EmptyMap<byte[], Cell3<Integer, Integer, Integer>> {

	public HSearchQuery query;
	public Cell2FilterVisitor cell2Visitor;
	public Integer matchingCell3;
	public Integer cellMin3; 
	public Integer cellMax3;
	public Integer[] inValues3;
	public Map<Integer, Cell2<Integer, Integer>> cell2L = null;

	public Cell3Map(final HSearchQuery query, final Cell2FilterVisitor cell2Visitor, final Integer matchingCell5, final Integer cellMin5, final Integer cellMax5, final Integer[] inValues5, final Integer matchingCell3, final Integer cellMin3, final Integer cellMax3, final Integer[] inValues3) {
		this.query = query; 
		this.cell2Visitor = cell2Visitor;
		this.matchingCell3 = matchingCell3;
		this.cellMin3 = cellMin3;
		this.cellMax3 = cellMax3;
		this.inValues3 = inValues3;
		this.cell2L = new Cell2Map(query, cell2Visitor,matchingCell5, cellMin5, cellMax5, inValues5);
	}
	@Override
	public final Cell3<Integer, Integer, Integer> put( final byte[] key, final Cell3<Integer, Integer, Integer> value) {
	try {
		cell2Visitor.cell3Key = key;
		if (query.filterCells[3]) {
			if (query.notValCells[3])
				value.getNotMap(matchingCell3, cell2L);
			else if (query.inValCells[3])
				value.getInMap(inValues3, cell2L);
			else
			value.getMap(matchingCell3, cellMin3, cellMax3, cell2L);
		 } else {
			value.sortedList = cell2L;
			value.parseElements();
		}
		return value;
		} catch (IOException e) {
			throw new IndexOutOfBoundsException(e.getMessage());
		}
	}
}



	
	public static final class Cell2Map extends EmptyMap<Integer, Cell2<Integer, Integer>> {

		public HSearchQuery query;
		public Integer matchingCell5;
		public Integer cellMin5;
		public Integer cellMax5;
		public Integer[] inValues5;
		public Cell2FilterVisitor cell2Visitor;
		
		public Cell2Map(final HSearchQuery query, final Cell2FilterVisitor cell2Visitor,
			final Integer matchingCell5, final Integer cellMin5, final Integer cellMax5, final Integer[] inValues5) {
			this.query = query;
			this.cell2Visitor = cell2Visitor;

			this.matchingCell5 = matchingCell5;
			this.cellMin5 = cellMin5;
			this.cellMax5 = cellMax5;
			this.inValues5 = inValues5;
		}
		
		@Override
		public final Cell2<Integer, Integer> put(final Integer key, final Cell2<Integer, Integer> value) {
			
			try {
				cell2Visitor.cell2Key = key;
				Cell2<Integer, Integer> cell2Val = value;

				if (query.filterCells[5]) {
					if(query.notValCells[5])
						cell2Val.processNot(matchingCell5, cell2Visitor);
					else if(query.inValCells[5])
						cell2Val.processIn(inValues5, cell2Visitor);
					else 
						cell2Val.process(matchingCell5, cellMin5, cellMax5,cell2Visitor);
				} else {
					cell2Val.process(cell2Visitor);
				}
				return value;
			} catch (IOException e) {
				throw new IndexOutOfBoundsException(e.getMessage());
			}
		}
	}
	
	///////////////////////////////////////////////////////////////////	
	
	public static final class Cell2FilterVisitor implements Cell2Visitor<Integer, Integer> {
		public HSearchQuery query;
		public IHSearchPlugin plugin;

		public PluginDocumentsBase.TablePartsCallback tablePartsCallback = null;

		public Integer matchingCell4;
		public Integer cellMin4;
		public Integer cellMax4;
		public Integer[] inValues4;

		public int cell5Key;
		public int cell4Key;
		public byte[] cell3Key;
		public int cell2Key;
		
		public int mode = MODE_COLS;
        public Cell2FilterVisitor(final HSearchQuery query, final IHSearchPlugin plugin, final PluginDocumentsBase.TablePartsCallback tablePartsCallback, final int mode) {
			
            this.query = query;
            this.plugin = plugin;
            this.tablePartsCallback = tablePartsCallback;
            this.mode = mode;

        }
		
		public final void set(final Integer matchingCell4, final Integer cellMin4, final Integer cellMax4, final Integer[] inValues4) {
			this.matchingCell4 = matchingCell4;
			this.cellMin4 = cellMin4;
			this.cellMax4 = cellMax4;
			this.inValues4 = inValues4;
		}
		
		
		
		@Override
		public final void visit(final Integer cell1Key, final Integer cell1Val) {

			//Is it all or not.
			if (query.filterCells[4]) {
				
				//IS it exact or not
				if (null != matchingCell4) {
					if ( query.notValCells[4] ) {
						//Exact val match
						if (matchingCell4.intValue() == cell1Key.intValue()) return;
					} else {
						//Not Exact val 
						if (matchingCell4.intValue() != cell1Key.intValue()) return;
					}
				} else {
					//Either range or IN
					if ( query.inValCells[4]) {
						//IN
						boolean isMatched = false;
						//LOOKING FOR ONE MATCHING
						for ( Object obj : query.inValuesAO[4]) {
							Integer objI = (Integer) obj;
							isMatched = cell1Key.intValue() == objI.intValue();
							
							//ONE MATCHED, NO NEED TO PROCESS
							if ( query.notValCells[4] ) { 
								if (!isMatched ) break; 
							} else {
								if (isMatched ) break;
							}
						}
						if ( !isMatched ) return; //NONE MATCHED
						
					} else {
						//RANGE
						boolean isMatched = cell1Key.intValue() < cellMin4.intValue() || 
											cell1Key.intValue() > cellMax4.intValue();
						if ( query.notValCells[4] ) {
							//Not Exact Range
							if (!isMatched ) return;
						} else {
							//Exact Range
							if (isMatched ) return;
						}
					}
				}
			}

			if (null != plugin) {
            	switch (this.mode) {
            		case MODE_COLS :
            			tablePartsCallback.onRowCols(cell5Key, cell4Key, cell3Key, cell2Key, cell1Key, cell1Val);
            			break;

            		case MODE_KEY :
            			tablePartsCallback.onRowKey(cell1Key);
            			break;
            		case MODE_KEYVAL :
            			tablePartsCallback.onRowKeyValue(cell1Key, cell1Val);
            			break;
            		case MODE_VAL:
            			tablePartsCallback.onRowValue(cell1Val);
            			break;
            	}
			} 
		}
	}	
	
	///////////////////////////////////////////////////////////////////	

	Cell6<Integer,Integer, byte[], Integer, Integer, Integer> table = createBlankTable();

	public HSearchTableDocuments() {
	}
	
	public final Cell6<Integer,Integer, byte[], Integer, Integer, Integer> createBlankTable() {
		return new Cell6<Integer,Integer, byte[], Integer, Integer, Integer>
			(
				SortedBytesUnsignedShort.getInstanceShort().setMinimumValueLimit((short) -32768.0 ) ,
				SortedBytesUnsignedShort.getInstanceShort().setMinimumValueLimit((short) -32768.0 ) ,
				SortedBytesArray.getInstanceArr(),
				SortedBytesInteger.getInstance(),
				SortedBytesInteger.getInstance(),
				SortedBytesUnsignedShort.getInstanceShort().setMinimumValueLimit((short) -32768.0 ) 
			);
	}

	public final byte[] toBytes() throws IOException {
		if ( null == table) return null;
		return table.toBytes(new IntegerComparator<Integer>());
	}

	public final void put (Integer doctype, Integer wordtype, byte[] payload, Integer hashcode, Integer docid, Integer frequency) {
		table.put( doctype, wordtype, payload, hashcode, docid, frequency );
	}
	
    @Override
    public final void get(final byte[] input, final HSearchQuery query, final IHSearchPlugin pluginI) throws IOException, NumberFormatException {
    	iterate(input, query, pluginI, MODE_COLS);
    }

    @Override
    public final void keySet(final byte[] input, final HSearchQuery query, final IHSearchPlugin pluginI) throws IOException {
    	iterate(input, query, pluginI, MODE_KEY);
    }

    public final void values(final byte[] input, final HSearchQuery query, final IHSearchPlugin pluginI) throws IOException {
    	iterate(input, query, pluginI, MODE_VAL);
    }

    public final void keyValues(final byte[] input, final HSearchQuery query, final IHSearchPlugin pluginI) throws IOException {
    	iterate(input, query, pluginI, MODE_KEYVAL);
    }
    
    private final void iterate(final byte[] input, final HSearchQuery query, final IHSearchPlugin pluginI, final int mode) throws IOException, NumberFormatException {
    	
        PluginDocumentsBase plugin = castPlugin(pluginI);
        PluginDocumentsBase.TablePartsCallback callback = plugin.getPart();

        Cell2FilterVisitor cell2Visitor = new Cell2FilterVisitor(query, pluginI, callback, mode);

        query.parseValuesConcurrent(new String[]{"Integer", "Integer", "byte[]", "Integer", "Integer", "Integer"});

		Integer matchingCell0 = ( query.filterCells[0] ) ? (Integer) query.exactValCellsO[0]: null;
		Integer matchingCell1 = ( query.filterCells[1] ) ? (Integer) query.exactValCellsO[1]: null;
		 byte[] matchingCell2 = ( query.filterCells[2] ) ? (byte[]) query.exactValCellsO[2]: null;
		Integer matchingCell3 = ( query.filterCells[3] ) ? (Integer) query.exactValCellsO[3]: null;
		cell2Visitor.matchingCell4 = ( query.filterCells[4] ) ? (Integer) query.exactValCellsO[4]: null;
		Integer matchingCell5 = ( query.filterCells[5] ) ? (Integer) query.exactValCellsO[5]: null;


		Integer cellMin0 = ( query.minValCells[0] == HSearchQuery.DOUBLE_MIN_VALUE) ? null : new Double(query.minValCells[0]).intValue();
		Integer cellMin1 = ( query.minValCells[1] == HSearchQuery.DOUBLE_MIN_VALUE) ? null : new Double(query.minValCells[1]).intValue();
		 byte[] cellMin2 = null;
		Integer cellMin3 = ( query.minValCells[3] == HSearchQuery.DOUBLE_MIN_VALUE) ? null : new Double(query.minValCells[3]).intValue();
		cell2Visitor.cellMin4 = ( query.minValCells[4] == HSearchQuery.DOUBLE_MIN_VALUE) ? null : new Double(query.minValCells[4]).intValue();
		Integer cellMin5 = ( query.minValCells[5] == HSearchQuery.DOUBLE_MIN_VALUE) ? null : new Double(query.minValCells[5]).intValue();


		Integer cellMax0 =  (query.maxValCells[0] == HSearchQuery.DOUBLE_MAX_VALUE) ? null : new Double(query.maxValCells[0]).intValue();
		Integer cellMax1 =  (query.maxValCells[1] == HSearchQuery.DOUBLE_MAX_VALUE) ? null : new Double(query.maxValCells[1]).intValue();
		 byte[] cellMax2 = null;
		Integer cellMax3 =  (query.maxValCells[3] == HSearchQuery.DOUBLE_MAX_VALUE) ? null : new Double(query.maxValCells[3]).intValue();
		cell2Visitor.cellMax4 =  (query.maxValCells[4] == HSearchQuery.DOUBLE_MAX_VALUE) ? null : new Double(query.maxValCells[4]).intValue();
		Integer cellMax5 =  (query.maxValCells[5] == HSearchQuery.DOUBLE_MAX_VALUE) ? null : new Double(query.maxValCells[5]).intValue();


		Integer[] inValues0 =  (query.inValCells[0]) ? (Integer[])query.inValuesAO[0]: null;
		Integer[] inValues1 =  (query.inValCells[1]) ? (Integer[])query.inValuesAO[1]: null;
		byte[][] inValues2 =  (query.inValCells[2]) ? (byte[][])query.inValuesAO[2]: null;
		Integer[] inValues3 =  (query.inValCells[3]) ? (Integer[])query.inValuesAO[3]: null;
		cell2Visitor.inValues4 =  (query.inValCells[4]) ? (Integer[])query.inValuesAO[4]: null;
		Integer[] inValues5 =  (query.inValCells[5]) ? (Integer[])query.inValuesAO[5]: null;


		Cell5Map cell5L = new Cell5Map(query, cell2Visitor,matchingCell5, cellMin5, cellMax5, inValues5,matchingCell3, cellMin3, cellMax3, inValues3,matchingCell2, cellMin2, cellMax2, inValues2,matchingCell1, cellMin1, cellMax1, inValues1);

        this.table.data = new BytesSection(input);
        if (query.filterCells[0]) {
			if(query.notValCells[0])
				this.table.getNotMap(matchingCell0, cell5L);
			else if(query.inValCells[0])
				this.table.getInMap(inValues0, cell5L);
			else	
				this.table.getMap(matchingCell0, cellMin0, cellMax0, cell5L);
        } else {
            this.table.sortedList = cell5L;
            this.table.parseElements();
        }

        if (null != callback) callback.onReadComplete();
        if (null != plugin) plugin.onReadComplete();
    }

    public final PluginDocumentsBase castPlugin(final IHSearchPlugin pluginI)
            throws IOException {
        PluginDocumentsBase plugin = null;
        if (null != pluginI) {
            if (pluginI instanceof PluginDocumentsBase) {
                plugin = (PluginDocumentsBase) pluginI;
            }
            if (null == plugin) {
                throw new IOException("Invalid plugin Type :" + pluginI);
            }
        }
        return plugin;
    }

    /**
     * Free the cube data
     */
    public final void clear() throws IOException {
        if ( null !=  table.sortedList) table.sortedList.clear();
    }

}
	
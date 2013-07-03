package com.bizosys.hsearch.kv.dao.donotmodify;

import java.io.IOException;

import com.bizosys.hsearch.byteutils.SortedBytesFloat;
import com.bizosys.hsearch.byteutils.SortedBytesInteger;
import com.bizosys.hsearch.kv.MapperKVBase;
import com.bizosys.hsearch.treetable.BytesSection;
import com.bizosys.hsearch.treetable.Cell2;
import com.bizosys.hsearch.treetable.Cell2Visitor;
import com.bizosys.hsearch.treetable.CellComparator.FloatComparator;
import com.bizosys.hsearch.treetable.client.HSearchQuery;
import com.bizosys.hsearch.treetable.client.IHSearchPlugin;
import com.bizosys.hsearch.treetable.client.IHSearchTable;

public class HSearchTableKVFloat implements IHSearchTable {

    public static boolean DEBUG_ENABLED = false;
    
    public static final int MODE_COLS = 0;
    public static final int MODE_KEY = 1;
    public static final int MODE_VAL = 2;
    public static final int MODE_KEYVAL = 3;

    public static final class Cell2FilterVisitor implements Cell2Visitor<Integer, Float> {

        public HSearchQuery query;
        public IHSearchPlugin plugin;
        public MapperKVBase.TablePartsCallback tablePartsCallback = null;

        public Integer matchingCell0;
        public Integer cellMin0;
        public Integer cellMax0;
        public Integer[] inValues0;

        public int cellFoundKey;

        public int mode = MODE_COLS;

        public Cell2FilterVisitor(HSearchQuery query,
                IHSearchPlugin plugin, MapperKVBase.TablePartsCallback tablePartsCallback, int mode) {

            this.query = query;
            this.plugin = plugin;
            this.tablePartsCallback = tablePartsCallback;
            this.mode = mode;

        }

        public void set(Integer matchingCell2, Integer cellMin2, Integer cellMax2, Integer[] inValues2) {
            this.matchingCell0 = matchingCell2;
            this.cellMin0 = cellMin2;
            this.cellMax0 = cellMax2;
            this.inValues0 = inValues2;
        }

        @Override
        public final void visit(Integer cell1Key, Float cell1Val) {
            			//Is it all or not.
			if (query.filterCells[0]) {
				
				//IS it exact or not
				if (null != matchingCell0) {
					if ( query.notValCells[0] ) {
						//Exact val match
						if (matchingCell0.intValue() == cell1Key.intValue()) return;
					} else {
						//Not Exact val 
						if (matchingCell0.intValue() != cell1Key.intValue()) return;
					}
				} else {
					//Either range or IN
					if ( query.inValCells[0]) {
						//IN
						boolean isMatched = false;
						//LOOKING FOR ONE MATCHING
						for ( Object obj : query.inValuesAO[0]) {
							Integer objI = (Integer) obj;
							isMatched = cell1Key.intValue() == objI.intValue();
							
							//ONE MATCHED, NO NEED TO PROCESS
							if ( query.notValCells[0] ) { 
								if (!isMatched ) break; 
							} else {
								if (isMatched ) break;
							}
						}
						if ( !isMatched ) return; //NONE MATCHED
						
					} else {
						//RANGE
						boolean isMatched = cell1Key.intValue() < cellMin0.intValue() || 
											cell1Key.intValue() > cellMax0.intValue();
						if ( query.notValCells[0] ) {
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
        			tablePartsCallback.onRowCols(cell1Key, cell1Val);                                
        			break;
        		case MODE_KEY :
        			tablePartsCallback.onRowKey(cell1Key);
        			break;
            	}
            }
        }
    }
    ///////////////////////////////////////////////////////////////////	
    Cell2<Integer,Float> table = createBlankTable();

    public HSearchTableKVFloat() {
    }

    public Cell2<Integer,Float> createBlankTable() {
        return new Cell2<Integer,Float>(SortedBytesInteger.getInstance(),
				SortedBytesFloat.getInstance());
    }

    public byte[] toBytes() throws IOException {
        if (null == table) {
            return null;
        }
        table.sort(new FloatComparator<Integer>());
        return table.toBytesOnSortedData();
    }

    public void put(Integer key, Float value) {
        table.add(key, value);
    }

    @Override
    public void get(byte[] input, HSearchQuery query, IHSearchPlugin pluginI) throws IOException, NumberFormatException {
    	iterate(input, query, pluginI, MODE_COLS);
    }

    @Override
    public void keySet(byte[] input, HSearchQuery query, IHSearchPlugin pluginI) throws IOException {
    	iterate(input, query, pluginI, MODE_KEY);
    }

    public void values(byte[] input, HSearchQuery query, IHSearchPlugin pluginI) throws IOException {
    	iterate(input, query, pluginI, MODE_VAL);
    }

    public void keyValues(byte[] input, HSearchQuery query, IHSearchPlugin pluginI) throws IOException {
    	iterate(input, query, pluginI, MODE_KEYVAL);
    }
    
    private void iterate(byte[] input, HSearchQuery query, IHSearchPlugin pluginI, int mode) throws IOException, NumberFormatException {
    	
        MapperKVBase plugin = castPlugin(pluginI);
        MapperKVBase.TablePartsCallback callback = plugin.getPart();

        Cell2FilterVisitor cell2Visitor = new Cell2FilterVisitor(query, pluginI, callback, mode);

        query.parseValuesConcurrent(new String[]{"Integer", "Float"});

		cell2Visitor.matchingCell0 = ( query.filterCells[0] ) ? (Integer) query.exactValCellsO[0]: null;
		Float matchingCell1 = ( query.filterCells[1] ) ? (Float) query.exactValCellsO[1]: null;


		cell2Visitor.cellMin0 = ( query.minValCells[0] == HSearchQuery.DOUBLE_MIN_VALUE) ? null : new Double(query.minValCells[0]).intValue();
		Float cellMin1 = ( query.minValCells[1] == HSearchQuery.DOUBLE_MIN_VALUE) ? null : new Double(query.minValCells[1]).floatValue();


		cell2Visitor.cellMax0 =  (query.maxValCells[0] == HSearchQuery.DOUBLE_MAX_VALUE) ? null : new Double(query.maxValCells[0]).intValue();
		Float cellMax1 =  (query.maxValCells[1] == HSearchQuery.DOUBLE_MAX_VALUE) ? null : new Double(query.maxValCells[1]).floatValue();


		cell2Visitor.inValues0 =  (query.inValCells[0]) ? (Integer[])query.inValuesAO[0]: null;
		Float[] inValues1 =  (query.inValCells[1]) ? (Float[])query.inValuesAO[1]: null;

	
		this.table.data = new BytesSection(input);
		if (query.filterCells[1]) {
			if(query.notValCells[1])
				this.table.processNot(matchingCell1, cell2Visitor);
			else if(query.inValCells[1])
				this.table.processIn(inValues1, cell2Visitor);
			else 
				this.table.process(matchingCell1, cellMin1, cellMax1,cell2Visitor);
		} else {
			this.table.process(cell2Visitor);
		}
		
        if (null != callback) {
            callback.onReadComplete();
        }
        if (null != plugin) {
            plugin.onReadComplete();
        }    	
    }

    public MapperKVBase castPlugin(IHSearchPlugin pluginI)
            throws IOException {
        MapperKVBase plugin = null;
        if (null != pluginI) {
            if (pluginI instanceof MapperKVBase) {
                plugin = (MapperKVBase) pluginI;
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
    public void clear() throws IOException {
        table.getMap().clear();
    }

}

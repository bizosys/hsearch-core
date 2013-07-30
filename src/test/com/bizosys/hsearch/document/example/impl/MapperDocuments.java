package com.bizosys.hsearch.document.example.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.bizosys.hsearch.byteutils.Storable;
import com.bizosys.hsearch.federate.BitSetOrSet;
import com.bizosys.hsearch.treetable.client.HSearchProcessingInstruction;

import com.bizosys.hsearch.document.example.impl.donotmodify.PluginDocumentsBase;

public final class MapperDocuments extends PluginDocumentsBase {

    public static String EMPTY = "";
    static boolean DEBUG_ENABLED = false;
    static byte[] bytesFor1 = Storable.putInt(1);

    HSearchProcessingInstruction instruction = null;

    public static char FLD_SEPARATOR = '\t';
    public static char ROW_SEPARATOR = '\n';

    public static enum TYPE{
    	LIST,
    	COUNT,
    	MINMAX
    }
    
    public TYPE switchType = TYPE.LIST;
    
    public double minValue = Integer.MAX_VALUE;
    public double maxValue = Integer.MIN_VALUE;

    public int totalCount = 0;
    List<String> foundRows= new ArrayList<String>();
    
	public static class MinMax{
	    public double minVal;
	    public double maxVal;

	    public MinMax(){}
	    public MinMax(double minVal, double maxVal) {
	        this.minVal = minVal;
	        this.maxVal = maxVal;
	    }
	}

    @Override
    public final void setOutputType(final HSearchProcessingInstruction outputTypeCode) {
        this.instruction = outputTypeCode;
        String type = this.instruction.getProcessingHint();

        if(type.equals(TYPE.LIST.name()))
        	switchType = TYPE.LIST;
        else if(type.equals(TYPE.COUNT.name()))
        	switchType = TYPE.COUNT;
        else if(type.equals(TYPE.MINMAX.name()))
        	switchType = TYPE.MINMAX;
    }

    /**
     * Merge is called when all threads finish their processing.
     * It can so happen that all threads may call at same time.
     * Maintain thread concurrency in the code.
     * Don't remove <code>this.parts.remove();</code> as after merging, it clears the ThreadLocal object. 
     */
    protected final void merge(Object data) {
        synchronized (this) {
        	switch (switchType) {
			case LIST:
	        	foundRows.add((String)data);
				break;
			case COUNT:
				totalCount += (Integer)data;
				break;
			case MINMAX:
	            MinMax m = (MinMax)data;
				if (m.minVal < this.minValue) {
	                this.minValue = m.minVal;
	            }
	            if (m.maxVal > this.maxValue) {
	                this.maxValue = m.maxVal;
	            }
	            
				break;
			}
        }
        this.parts.remove();
    }

    /**
     * When all parts are completed, finally it is called.
     * By this time, the result of all parts is available for final processing.
     * 
     */
    @Override
    public final void onReadComplete() {
    	switch (switchType) {
		case LIST:
        	for (String element : foundRows) {
				System.out.println(element);
			}
			break;
		case COUNT:
			System.out.println("Total Count is " +totalCount);
			break;
		case MINMAX:
			System.out.println("Min is " + minValue + " and max is " + maxValue);
			break;
		}
    }


    /**
     * For multi queries, we need to provide matching documents for 
     * intersection. For sinle query this is having no usage and can be passed null to save computing.
     *     	BitSetOrSet sets = new BitSetOrSet();  sets.setDocumentIds(this.rows.keySet());
     *      OR,
     *      Set Document Positions. 
     */
    @Override
    public final BitSetOrSet getUniqueMatchingDocumentIds() throws IOException {
        return null;
    }

    /**
     * Collects the results for rows level aggregation.
     */
    @Override
    public final void getResultSingleQuery(final Collection<byte[]> container) throws IOException {
    }

    /**
     * Collects the results for rows level aggregation.
     */
    @Override
    public final void getResultMultiQuery(final BitSetOrSet matchedIds, final Collection<byte[]> container) throws IOException {
    }

    /**
     * This method is finally called which cleans up all the resources.
     * If not cleaned up the results will be contaminated
     */
    @Override
    public final void clear() {
        this.minValue = Integer.MAX_VALUE;
        this.maxValue = Integer.MIN_VALUE;
        this.totalCount = 0;
        this.foundRows.clear();
    }

    /**
     * Each thread will have 1 instance of this class.
     * For all found rows, the onRowCols/onRowKeys/... are called as we have set in the instructor.
     * @see Client.execute
     * When all are processed, this reports back to the main class for agrregating the result via merge.
     * @see Mapper.whole.merge
     * @author abinash
     *
     */
    public static final class RowReader implements TablePartsCallback {

        public MapperDocuments whole = null;

        public double minValue = Integer.MAX_VALUE;
        public double maxValue = Integer.MIN_VALUE;
        
        public int count = 0;
        
        StringBuilder appender = new StringBuilder(65536); 

        public RowReader(final MapperDocuments whole) {
            this.whole = whole;
        }

        public final boolean onRowKey(final int id) {
            return true;
        }

        public final boolean onRowCols( final int doctype,  final int wordtype,  final String metadata,  final int hashcode,  final int docid,  final boolean flag) {
        	switch (whole.switchType) {
			case LIST:
				
				appender.append(doctype);
				appender.append(FLD_SEPARATOR).append(wordtype);
				appender.append(FLD_SEPARATOR).append(metadata);
				appender.append(FLD_SEPARATOR).append(hashcode);
				appender.append(FLD_SEPARATOR).append(docid);
				appender.append(FLD_SEPARATOR).append(flag);
				appender.append(ROW_SEPARATOR);

	        	
	        	break;
	        	
			case COUNT:
				
				count++;
				
				break;
				
			case MINMAX:
				
				try {
	throw new Exception("Min Max cannot be calculated for boolean datatype");
} catch (Exception e) {
e.printStackTrace();
}	        	
				
				break;
			}
        	
            return true;
        }

        @Override
        public final boolean onRowKeyValue(final int key, final boolean value) {
            return true;
        }

        @Override
        public final boolean onRowValue(final boolean value) {
            return true;
        }

        @Override
        public final void onReadComplete() {
        	switch (whole.switchType) {
			case LIST:
				
				this.whole.merge(appender.toString());
	    		appender.delete(0, 65536);
				
				break;
			case COUNT:
				
				this.whole.merge(count);
				count = 0;
				
				break;
			case MINMAX:
				
				this.whole.merge(new MinMax(minValue, maxValue));
	            this.minValue = Integer.MAX_VALUE;
	            this.maxValue = Integer.MIN_VALUE;
	            
				break;
			}
        }
    }

    /*******************************************************************************************
     * The below sections are generic in nature and no need to be changed.
     */
    /**
     * Do not modify this section as we need to create indivisual instances per thread.
     */
    public final ThreadLocal<PluginDocumentsBase.TablePartsCallback> parts = 
        	new ThreadLocal<PluginDocumentsBase.TablePartsCallback>();
    @Override
    public final PluginDocumentsBase.TablePartsCallback getPart() {
        PluginDocumentsBase.TablePartsCallback part = parts.get();
        if (null == part) {
            parts.set(new RowReader(this));
            return parts.get();
        } 
        return part;
    }

	@Override
	public void setMergeId(byte[] mergeId) throws IOException {
	}

    /**
     * sample ser/deser. Use Storable class.
     public static byte[] ser(int findings, int groups, int studies) {
    	byte[] output = new byte[12];
        System.arraycopy(Storable.putInt(cell1), 0, output, 0, 4);
        System.arraycopy(Storable.putFloat(cell2), 0, output, 4, 4);
        return output;
    }
     */
}

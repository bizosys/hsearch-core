package com.bizosys.hsearch.treetable.example.impl;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import com.bizosys.hsearch.byteutils.Storable;
import com.bizosys.hsearch.federate.BitSetOrSet;
import com.bizosys.hsearch.treetable.client.HSearchProcessingInstruction;

import com.bizosys.hsearch.treetable.example.impl.donotmodify.PluginExamResultBase;
import com.bizosys.hsearch.util.LineReaderUtil;

public final class MapperExamResult extends PluginExamResultBase {

    public static String EMPTY = "";
    static boolean DEBUG_ENABLED = false;
    static byte[] bytesFor1 = Storable.putInt(1);
    public static char SEPARATOR = '\t';

    HSearchProcessingInstruction instruction = null;
    Map<Integer, String> foundRows= new HashMap<Integer, String>(); 
    
    @Override
    public final void setOutputType(final HSearchProcessingInstruction outputTypeCode) {
        this.instruction = outputTypeCode;
    }

    /**
     * Merge is called when all threads finish their processing.
     * It can so happen that all threads may call at same time.
     * Maintain thread concurrency in the code.
     * Don't remove <code>this.parts.remove();</code> as after merging, it clears the ThreadLocal object. 
     */
    protected final void merge(Map<Integer, String> records) {
        synchronized (this) {
        	foundRows.putAll(records);
        	records.clear();
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
    	BitSetOrSet uniqueRows = new BitSetOrSet();
    	uniqueRows.setDocumentIds(this.foundRows.keySet());
    	return uniqueRows;
    }

    /**
     * Collects the results for rows level aggregation.
     */
    @Override
    public final void getResultSingleQuery(final Collection<byte[]> container) throws IOException {
    	for (String row : this.foundRows.values()) {
        	container.add( row.getBytes());
		}
    }

    /**
     * Collects the results for rows level aggregation.
     */
    @Override
    public final void getResultMultiQuery(final BitSetOrSet matchedIds, final Collection<byte[]> container) throws IOException {
		System.out.println("Matched Ids:" + matchedIds.getDocumentIds().toString());
    	for (Object matchedId : matchedIds.getDocumentIds()) {
        	if (this.foundRows.containsKey(matchedId)) {
            	container.add( this.foundRows.get(matchedId).getBytes());
    		}
		}

    }

    /**
     * This method is finally called which cleans up all the resources.
     * If not cleaned up the results will be contaminated
     */
    @Override
    public final void clear() {
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

        public MapperExamResult whole = null;
        Map<Integer, String> partTable = new HashMap<Integer, String>();
        StringBuilder appender = new StringBuilder(65536); 

        public RowReader(final MapperExamResult whole) {
            this.whole = whole;
        }

        public final boolean onRowKey(final int id) {
            return true;
        }

        public final boolean onRowCols( final int age,  final String role,  final String location,  final int empid,  final float mark) {

        	appender.append(age);
        	appender.append(SEPARATOR).append(role);
        	appender.append(SEPARATOR).append(location);
        	appender.append(SEPARATOR).append(empid);
        	appender.append(SEPARATOR).append(mark);
    		
        	partTable.put(empid, appender.toString());
    		
    		appender.delete(0, 65536);
        	return true;
        }

        @Override
        public final boolean onRowKeyValue(final int key, final float value) {
            return true;
        }

        @Override
        public final boolean onRowValue(final float value) {
            return true;
        }

        @Override
        public final void onReadComplete() {
            this.whole.merge(partTable);
        }
    }

    /*******************************************************************************************
     * The below sections are generic in nature and no need to be changed.
     */
    /**
     * Do not modify this section as we need to create indivisual instances per thread.
     */
    public final ThreadLocal<PluginExamResultBase.TablePartsCallback> parts = 
        	new ThreadLocal<PluginExamResultBase.TablePartsCallback>();
    @Override
    public final PluginExamResultBase.TablePartsCallback getPart() {
        PluginExamResultBase.TablePartsCallback part = parts.get();
        if (null == part) {
            parts.set(new RowReader(this));
            return parts.get();
        } 
        return part;
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

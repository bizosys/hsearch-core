package com.bizosys.hsearch.treetable.storage.sampleImpl;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.bizosys.hsearch.byteutils.SortedBytesArray;
import com.bizosys.hsearch.byteutils.SortedBytesString;
import com.bizosys.hsearch.byteutils.Storable;
import com.bizosys.hsearch.federate.FederatedFacade;
import com.bizosys.hsearch.treetable.Cell2;
import com.bizosys.hsearch.treetable.client.HSearchProcessingInstruction;
import com.bizosys.hsearch.treetable.storage.sampleImpl.donotmodify.PluginExamResultBase;

public class MapperExamResult extends PluginExamResultBase {

    public static String EMPTY = "";
    static boolean DEBUG_ENABLED = false;
    static byte[] bytesFor1 = Storable.putInt(1);

    HSearchProcessingInstruction instruction = null;
    
    Map<String, String> rows = new HashMap<String, String>();

    @Override
    public void setOutputType(HSearchProcessingInstruction outputTypeCode) {
        this.instruction = outputTypeCode;
    }

    /**
     * Merge is called when all threads finish their processing.
     * It can so happen that all threads may call at same time.
     * Maintain thread concurrency in the code.
     * Don't remove <code>this.parts.remove();</code> as after merging, it clears the ThreadLocal object. 
     */
    protected void merge(Map<String, String> rows) {
        synchronized (this) {
            this.rows.putAll(rows);
        }
        System.out.println("Number of Rows :" +  this.rows.size());
        this.parts.remove();
    }

    /**
     * When all parts are completed, finally it is called.
     * By this time, the result of all parts is available for final processing.
     * 
     */
    @Override
    public void onReadComplete() {
    }


    /**
     * For multi queries, we need to provide matching documents for 
     * intersection. For sinle query this is having no usage and can be passed null to save computing.
     */
    @Override
    public Collection<String> getUniqueMatchingDocumentIds() throws IOException {
        return this.rows.keySet();
    }

    /**
     * Collects the results for rows level aggregation.
     */
    @Override
    public void getResultSingleQuery(Collection<byte[]> container) throws IOException {
    	Cell2<String, String> cell2 = new Cell2<String, String>(SortedBytesString.getInstance(),
    			SortedBytesString.getInstance());
    	
    	byte[] keyVal = cell2.toBytesOnSortedData(rows);
    	container.add(keyVal);
    }

    /**
     * Collects the results for rows level aggregation.
     */
    @Override
    public void getResultMultiQuery(List<FederatedFacade<String, String>.IRowId> matchedIds,
            Collection<byte[]> container) throws IOException {
    	
    	Cell2<String, String> cell2 = new Cell2<String, String>(SortedBytesString.getInstance(),
    			SortedBytesString.getInstance());

    	for (FederatedFacade<String, String>.IRowId row : matchedIds) {
    		String docId = row.getDocId();
    		if ( ! rows.containsKey(docId) ) continue;
    		cell2.add(docId, rows.get(docId));
		}
    	container.add(cell2.toBytesOnSortedData());    	
    }

    /**
     * This method is finally called which cleans up all the resources.
     * If not cleaned up the results will be contaminated
     */
    @Override
    public void clear() {
    	this.rows.clear();
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
    public static class RowReader implements TablePartsCallback {

    	Map<String, String> rows = new HashMap<String, String>();
        public MapperExamResult whole = null;

        public RowReader(MapperExamResult whole) {
            this.whole = whole;
        }

        public final boolean onRowKey(int id) {
            return true;
        }

        public final boolean onRowCols(int cell1, String cell2, String cell3, int studentId, float cell5) {
        	String rowAsStr = "" + cell1 + "|" + cell2 + "|" + cell3 + "|" + studentId + "|" + cell5;
        	rows.put(new Integer(studentId).toString(), rowAsStr);
        	System.out.println(rowAsStr);
        	return true;
        }

        @Override
        public final boolean onRowKeyValue(int key, float value) {
            return true;
        }

        @Override
        public final boolean onRowValue(float value) {
            return true;
        }

        @Override
        public void onReadComplete() {
            this.whole.merge(rows);
            rows.clear();
            /**
             * Clean up resources for reuse.
             */
        }
    }

    /*******************************************************************************************
     * The below sections are generic in nature and no need to be changed.
     */
    /**
     * Do not modify this section as we need to create indivisual instances per thread.
     */
    public ThreadLocal<PluginExamResultBase.TablePartsCallback> parts = 
        	new ThreadLocal<PluginExamResultBase.TablePartsCallback>();
    @Override
    public PluginExamResultBase.TablePartsCallback getPart() {
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

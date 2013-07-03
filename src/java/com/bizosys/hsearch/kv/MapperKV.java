package com.bizosys.hsearch.kv;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import com.bizosys.hsearch.byteutils.SortedBytesInteger;
import com.bizosys.hsearch.byteutils.Storable;
import com.bizosys.hsearch.federate.BitSetOrSet;
import com.bizosys.hsearch.kv.MapperKVBase;
import com.bizosys.hsearch.kv.impl.ComputeFactory;
import com.bizosys.hsearch.kv.impl.ICompute;
import com.bizosys.hsearch.treetable.client.HSearchProcessingInstruction;


public final class MapperKV extends MapperKVBase {

    public static String EMPTY = "";
    static boolean DEBUG_ENABLED = false;
    static byte[] bytesFor1 = Storable.putInt(1);

    HSearchProcessingInstruction instruction = null;

    ICompute compute = null;
    Set<Integer> ids = new HashSet<Integer>();
    
    @Override
    public final void setOutputType(final HSearchProcessingInstruction outputTypeCode) {
        this.instruction = outputTypeCode;
    	this.compute = ComputeFactory.getInstance().getCompute(this.instruction.getProcessingHint());
    	this.compute.setCallBackType(this.instruction.getOutputType());
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
        return null;
    }

    /**
     * Collects the results for rows level aggregation.
     */
    @Override
    public final void getResultSingleQuery(final Collection<byte[]> container) throws IOException {
    	
    	if(this.instruction.getCallbackType() == HSearchProcessingInstruction.PLUGIN_CALLBACK_COLS)
    		container.add(this.compute.toBytes());
    	else
    		container.add(SortedBytesInteger.getInstance().toBytes(this.ids));
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
        this.compute.clear();
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

        public MapperKV whole = null;
        ICompute computation = null;
        public RowReader(final MapperKV whole) {
            this.whole = whole;
        	this.computation = whole.compute;
        }

        public final boolean onRowCols( final int key,  final Object value) {
        	computation.put(key, value);
            return true;
        }

		@Override
		public boolean onRowKey(int id) {
			this.whole.ids.add(id);
			return true;
		}

        @Override
        public final void onReadComplete() {
        }
    }

    @Override
    public final MapperKVBase.TablePartsCallback getPart() {
        return new RowReader(this);
    }

}

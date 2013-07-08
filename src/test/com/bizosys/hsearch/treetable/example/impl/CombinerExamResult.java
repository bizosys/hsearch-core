package com.bizosys.hsearch.treetable.example.impl;

import java.io.IOException;
import java.util.Collection;

import com.bizosys.hsearch.federate.BitSetOrSet;
import com.bizosys.hsearch.treetable.client.HSearchProcessingInstruction;
import com.bizosys.hsearch.treetable.example.impl.donotmodify.PluginExamResultBase;

public final class CombinerExamResult extends PluginExamResultBase<Object> {

    HSearchProcessingInstruction instruction = null;

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
    public final void collect(Object data) {
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
    }

	@Override
	public TablePartsCallback createMapper(PluginExamResultBase<Object> whole) {
		return new MapperExamResult<Object>(this);
	}

}

package com.bizosys.hsearch.treetable.example.impl.donotmodify;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.bizosys.hsearch.byteutils.Storable;
import com.bizosys.hsearch.federate.BitSetOrSet;
import com.bizosys.hsearch.treetable.client.HSearchProcessingInstruction;
import com.bizosys.hsearch.treetable.example.impl.donotmodify.CountMapperExamResult.Counter;

public final class CountCombinerExamResult extends PluginExamResultBase<Map<Integer, Counter>> {

    HSearchProcessingInstruction instruction = null;
    Map<Integer, Counter> idsWithCount = new HashMap<Integer, CountMapperExamResult.Counter>();

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
    public final void collect(Map<Integer, Counter> data) {
    	System.out.println( "\n\n\n Collect :" + data.size());
    	
    	synchronized (idsWithCount) {
    		for ( Integer key : data.keySet()) {
    			if ( idsWithCount.containsKey(key)) 
    				idsWithCount.get(key).counter += data.get(key).counter;
    			else 
    				idsWithCount.put(key, data.get(key));
    		}
		}
    	data.clear();
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
    	BitSetOrSet ids = new BitSetOrSet();
    	ids.setDocumentIds(this.idsWithCount.keySet());
        return ids;
    }

    /**
     * Collects the results for rows level aggregation.
     */
    @Override
    public final void getResultSingleQuery(final Collection<byte[]> container) throws IOException {
    	int finalResult = 0;
    	for (Counter counter : this.idsWithCount.values()) {
    		finalResult += counter.counter; 
		}
    	container.add(Storable.putInt(finalResult));
    }

    /**
     * Collects the results for rows level aggregation.
     */
    @Override
    public final void getResultMultiQuery(final BitSetOrSet matchedIds, final Collection<byte[]> container) throws IOException {
    	@SuppressWarnings("unchecked")
		Set<Integer> matchingIds = matchedIds.getDocumentIds();
    	
    	int finalResult = 0;
    	for (Integer key : this.idsWithCount.keySet()) {
    		if ( matchingIds.contains(key)) {
    			finalResult += this.idsWithCount.get(key).counter;
    		}
		}
    	container.add(Storable.putInt(finalResult));
    }

    /**
     * This method is finally called which cleans up all the resources.
     * If not cleaned up the results will be contaminated
     */
    @Override
    public final void clear() {
    	this.idsWithCount.clear();
    }

	@Override
	public TablePartsCallback createMapper(PluginExamResultBase<Map<Integer, Counter>> whole) {
		return new CountMapperExamResult(this);
	}

}

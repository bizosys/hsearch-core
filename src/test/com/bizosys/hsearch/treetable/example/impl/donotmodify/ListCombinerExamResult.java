package com.bizosys.hsearch.treetable.example.impl.donotmodify;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.bizosys.hsearch.byteutils.SortedBytesInteger;
import com.bizosys.hsearch.byteutils.SortedBytesString;
import com.bizosys.hsearch.federate.BitSetOrSet;
import com.bizosys.hsearch.treetable.Cell2;
import com.bizosys.hsearch.treetable.client.HSearchProcessingInstruction;

public final class ListCombinerExamResult extends PluginExamResultBase<Map<Integer, String>> {

    HSearchProcessingInstruction instruction = null;
    Map<Integer, String> rows = new HashMap<Integer, String>();

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
    public final void collect(Map<Integer, String> data) {
    	synchronized (rows) {
	    	System.out.println( "Collect total rows : " + data.size());
    		for ( Integer key : data.keySet()) {
    			if ( !rows.containsKey(key)) 
    				rows.put( key, data.get(key) + "\n");
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
    	ids.setDocumentIds(this.rows.keySet());
    	return ids;
    }

    /**
     * Collects the results for rows level aggregation.
     */
    @Override
    public final void getResultSingleQuery(final Collection<byte[]> container) throws IOException {

    	if(0 == this.rows.size())
    		return;
    	
    	StringBuilder finalResult = new StringBuilder();
    	for (Integer key : this.rows.keySet()) {
    		finalResult.append( this.rows.get(key) );
    	}
    	
    	container.add(finalResult.toString().getBytes());
    }

    /**
     * Collects the results for rows level aggregation.
     */
    @Override
    public final void getResultMultiQuery(final BitSetOrSet matchedIds, final Collection<byte[]> container) throws IOException {
    	@SuppressWarnings("unchecked")
		Set<Integer> matchingIds = matchedIds.getDocumentIds();
    	if(0 == matchedIds.size())
    		return;
    	
    		
    	Cell2<Integer, String> cellData = new Cell2<Integer, String>(SortedBytesInteger.getInstance(),SortedBytesString.getInstance());
    	for (Integer key : this.rows.keySet()) {
    		if ( matchingIds.contains(key)) {
    			cellData.add(key, this.rows.get(key) );
    		}
		}
    	container.add(cellData.toBytesOnSortedData());
    }

    /**
     * This method is finally called which cleans up all the resources.
     * If not cleaned up the results will be contaminated
     */
    @Override
    public final void clear() {
    	this.rows.clear();
    }

	@Override
	public TablePartsCallback createMapper(PluginExamResultBase<Map<Integer, String>> whole) {
		return new ListMapperExamResult(this);
	}

}

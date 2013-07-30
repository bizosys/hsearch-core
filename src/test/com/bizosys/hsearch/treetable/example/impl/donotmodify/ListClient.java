package com.bizosys.hsearch.treetable.example.impl.donotmodify;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.bizosys.hsearch.byteutils.SortedBytesArray;
import com.bizosys.hsearch.treetable.client.HSearchProcessingInstruction;
import com.bizosys.hsearch.treetable.storage.HSearchGenericFilter;
import com.bizosys.hsearch.treetable.storage.HSearchTableReader;

public class ListClient extends HSearchTableReader {

	   HSearchGenericFilter filter = null;
	    
	   public List<String> finalOutput = new ArrayList<String>();

	    public ListClient() throws IOException {
	        HBaseTableSchema.getInstance();
	    }

	    @Override
	    public HSearchGenericFilter getFilter(String multiQuery,
	            Map<String, String> multiQueryParts, HSearchProcessingInstruction outputType) {
	        filter = new ListFilter(outputType, multiQuery, multiQueryParts);
	        return filter;
	    }

	    @Override
	    public void rows(Collection<byte[]> results, HSearchProcessingInstruction instruction) {

	        try {

	            for (byte[] data : results) {
	            	
		           	SortedBytesArray arr = SortedBytesArray.getInstanceArr();
		        	arr.parse(data);

		        	int size = arr.getSize();
		        	SortedBytesArray.Reference ref = new SortedBytesArray.Reference();

		        	for ( int i=0; i<size; i++) {
		        		arr.getValueAtReference(i,ref);
		            	String row = new String ( new String(data, ref.offset, ref.length));
		        		System.out.println ( row);
		            	finalOutput.add(row);
		        	}	            	

	            }

	        } catch (IOException ex) {
	            ex.printStackTrace(System.out);
	        }

	    }

	    public void execute(String query, Map<String, String> qPart) throws IOException, ParseException {
	    	HSearchProcessingInstruction instruction = 
	    		new HSearchProcessingInstruction(HSearchProcessingInstruction.PLUGIN_CALLBACK_COLS);
	        read(HBaseTableSchema.getInstance().TABLE_NAME, query, qPart, instruction , true, true);
	    }
}

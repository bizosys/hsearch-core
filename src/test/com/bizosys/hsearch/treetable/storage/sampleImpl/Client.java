package com.bizosys.hsearch.treetable.storage.sampleImpl;

import java.awt.List;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import com.bizosys.hsearch.byteutils.SortedBytesArray;
import com.bizosys.hsearch.byteutils.SortedBytesBase.Reference;
import com.bizosys.hsearch.byteutils.SortedBytesString;
import com.bizosys.hsearch.treetable.BytesSection;
import com.bizosys.hsearch.treetable.Cell2;
import com.bizosys.hsearch.treetable.CellKeyValue;
import com.bizosys.hsearch.treetable.client.HSearchProcessingInstruction;
import com.bizosys.hsearch.treetable.storage.HSearchGenericFilter;
import com.bizosys.hsearch.treetable.storage.HSearchTableReader;
import com.bizosys.hsearch.treetable.storage.sampleImpl.donotmodify.HBaseTableSchema;

public class Client extends HSearchTableReader {

    HSearchGenericFilter filter = null;
    
    public Map<String, String> finalOutput = new HashMap<String, String>();

    public Client() throws IOException {
        HBaseTableSchema.getInstance();
    }

    @Override
    public HSearchGenericFilter getFilter(String multiQuery,
            Map<String, String> multiQueryParts, HSearchProcessingInstruction outputType) {
        filter = new Filter(outputType, multiQuery, multiQueryParts);
        return filter;
    }

    @Override
    public void rows(Map<byte[], byte[]> results, HSearchProcessingInstruction instruction) {

        try {

            for (Map.Entry<byte[], byte[]> entry : results.entrySet()) {
            	
            	byte[] data = entry.getValue();
            	SortedBytesArray arr = SortedBytesArray.getInstanceArr();
            	arr.parse(data);
            	
            	int size = arr.getSize();
            	SortedBytesArray.Reference ref = new SortedBytesArray.Reference();
            	for ( int i=0; i<size; i++) {
            		arr.getValueAtReference(i,ref);
                	Cell2<String, String> cell2 = new Cell2<String, String>(
                		SortedBytesString.getInstance(), SortedBytesString.getInstance(),
                		new BytesSection(data, ref.offset, ref.length));

                	cell2.populate(finalOutput);
            	}
            }

        } catch (IOException ex) {
            ex.printStackTrace(System.out);
        }

    }

    public void execute(String query, Map<String, String> qPart) throws IOException, ParseException {
    	HSearchProcessingInstruction instruction = 
    		new HSearchProcessingInstruction(HSearchProcessingInstruction.PLUGIN_CALLBACK_COLS, HSearchProcessingInstruction.OUTPUT_COLS);
        read(query, qPart, instruction , true, true);
    }
}

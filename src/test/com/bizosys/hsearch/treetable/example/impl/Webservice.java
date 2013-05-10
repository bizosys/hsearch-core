package com.bizosys.hsearch.treetable.example.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import com.bizosys.hsearch.hbase.HDML;
import com.bizosys.hsearch.hbase.HWriter;
import com.bizosys.hsearch.hbase.NV;
import com.bizosys.hsearch.hbase.RecordScalar;
import com.bizosys.hsearch.treetable.client.HSearchProcessingInstruction;
import com.bizosys.hsearch.treetable.client.partition.IPartition;
import com.bizosys.hsearch.treetable.example.impl.donotmodify.*;

public class Webservice {
	/**
	 *   m|1|[1:7432]|[1:4]|*|[1:758320]|*
	 *  Range Query - [1:7432]
	 *  Match All - *
	 *  Exact match - m
	 */
    
    /**
    public static void main(String[] args) throws Exception {
    
    	HDML.truncate("PriceTable", new NV("Price".getBytes(), "1".getBytes() ));
    	
    	HSearchTablePrice devTable = new HSearchTablePrice();
    	
	    devTable.put((byte)26,23,24,36,38,(byte)18);
    	RecordScalar devRecords = new RecordScalar(
    		"199801011000".getBytes(), new NV("Price".getBytes(), "1".getBytes() , devTable.toBytes() ) ) ;
    	
    	List<RecordScalar> records = new ArrayList<RecordScalar>();
    	records.add(devRecords);
    	
    	HWriter.getInstance(true).insertScalar("PriceTable", records);
    	
    	
    	Client ht = new Client();
        Map<String, String> multiQueryParts = new HashMap<String, String>();
        multiQueryParts.put("Price:1", "*|*|*|*|*|*");
        
        long start = System.currentTimeMillis();
        ht.execute("Price:1", multiQueryParts);
        long end = System.currentTimeMillis();
        System.out.println(" finished in  " + (end - start) + " millis ");	
	}
	*/
}

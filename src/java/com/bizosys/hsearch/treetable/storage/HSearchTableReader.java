/*
* Copyright 2010 Bizosys Technologies Limited
*
* Licensed to the Bizosys Technologies Limited (Bizosys) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The Bizosys licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package com.bizosys.hsearch.treetable.storage;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.bizosys.hsearch.hbase.ColumnFamName;
import com.bizosys.hsearch.hbase.HBaseFacade;
import com.bizosys.hsearch.hbase.HReader;
import com.bizosys.hsearch.hbase.HTableWrapper;
import com.bizosys.hsearch.hbase.HbaseLog;
import com.bizosys.hsearch.hbase.IScanCallBack;
import com.bizosys.hsearch.treetable.client.OutputType;

public abstract class HSearchTableReader {
	
	public static boolean DEBUG_ENABLED = HbaseLog.l.isDebugEnabled();
	
	//public static ParallelHReader parallelReader = new ParallelHReader(10);
	
	public abstract HSearchGenericFilter getFilter(String multiQuery, Map<String, String> multiQueryParts, OutputType outputType); 
	public abstract IScanCallBack getResultCollector();

	public abstract void counts(Map<byte[], long[]> results);
	public abstract void agreegates(Map<byte[], double[]> results, OutputType aggregateType);
	public abstract void rows(Map<byte[], byte[]> results, OutputType rowType);

	public void read( String multiQuery, Map<String, String> multiQueryParts, 
			OutputType outputType, boolean isPartitioned, boolean isParallel) 
			throws IOException, ParseException {
		
		HSearchGenericFilter filter = getFilter(multiQuery, multiQueryParts, outputType);
		
		Set<String> uniqueFamilies = new HashSet<String>(3);
		
		for ( String colNameQuolonId : multiQueryParts.keySet() ) {
			
			int colNameAndQIdSplitIndex = colNameQuolonId.indexOf(':');
			if ( -1 == colNameAndQIdSplitIndex || colNameQuolonId.length() - 1 == colNameAndQIdSplitIndex) {
				throw new IOException("Sub queries expected as  X:Y eg.\n" + 
						 "structured:A OR unstructured:B\nstructured:A=f|1|1|1|c|*|*\nunstructured:B=*|*|*|*|*|*");
			}
			String family = colNameQuolonId.substring(0,colNameAndQIdSplitIndex);
			uniqueFamilies.add(family);
			
		}

		List<ColumnFamName> families = new ArrayList<ColumnFamName>();
		for (String  family : uniqueFamilies) {
			if ( DEBUG_ENABLED ) HbaseLog.l.debug("HSearchTableReader > Adding Family: " + family);
			families.add(new ColumnFamName(family.getBytes(), HBaseTableSchemaDefn.COL_NAME_BYTES));
		}
	
		IScanCallBack recordsCollector = getResultCollector();
		String tableName = HBaseTableSchemaDefn.getInstance().tableName;
		
		if ( isParallel ) {
			if ( DEBUG_ENABLED ) HbaseLog.l.debug("HSearchTableReader > Searching in parallel.");
			/**
			 * OLD Version
			 * parallelReader.getAllValues(tableName, families, filter, recordsCollector);
			 */
			HTableWrapper table = HBaseFacade.getInstance().getTable(tableName);
			
	        try {
	        	switch ( outputType.getOutputType()) {
	        		case OutputType.OUTPUT_COUNT:
	    		        counts(new HSearchGenericCoProcessorFactory(
	    		        		families, filter).execCoprocessorCounts(table));
	    		        break;

	    			case OutputType.OUTPUT_MIN:
	    			case OutputType.OUTPUT_MAX:
	    			case OutputType.OUTPUT_AVG:
	    			case OutputType.OUTPUT_SUM:
	    			case OutputType.OUTPUT_MIN_MAX:
	    			case OutputType.OUTPUT_MIN_MAX_AVG:
	    			case OutputType.OUTPUT_MIN_MAX_COUNT:
	    			case OutputType.OUTPUT_MIN_MAX_AVG_COUNT:
	    			case OutputType.OUTPUT_MIN_MAX_SUM:
	    			case OutputType.OUTPUT_MIN_MAX_SUM_AVG:
	    			case OutputType.OUTPUT_MIN_MAX_SUM_COUNT:
	    			case OutputType.OUTPUT_MIN_MAX_AVG_SUM_COUNT:
	    				agreegates(new HSearchGenericCoProcessorFactory(
	    		        		families, filter).execCoprocessorAggregates(table), outputType );
	    		        break;
	    		        
	    			case OutputType.OUTPUT_ID:
	    			case OutputType.OUTPUT_IDVAL:
	    			case OutputType.OUTPUT_VAL:
	    			case OutputType.OUTPUT_COLS:
	    				rows(new HSearchGenericCoProcessorFactory(
	    		        		families, filter).execCoprocessorRows(table), outputType );
	    		        break;	    		        

	    			default:
	        			new IOException("Type is not supported.");
	        			
	        	}
	        } catch (Throwable th) {
	            throw new IOException(th);
	        }
			
		} else {
			if ( DEBUG_ENABLED ) HbaseLog.l.debug("HSearchTableReader > Searching in Sequential.");
			HReader.getAllValues(tableName,families, filter, recordsCollector);
		}
	}	
}


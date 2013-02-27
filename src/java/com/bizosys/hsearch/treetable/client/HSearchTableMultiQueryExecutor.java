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

package com.bizosys.hsearch.treetable.client;

import java.util.List;
import java.util.Map;

import com.bizosys.hsearch.byteutils.ByteArrays;
import com.bizosys.hsearch.byteutils.ByteArrays.ArrayInt.Builder;
import com.bizosys.hsearch.federate.FederatedFacade;
import com.bizosys.hsearch.federate.FederatedFacade.IRowId;
import com.bizosys.hsearch.federate.QueryPart;

/**
 * Concurrent: 
 * 	Process each filter in threads - De-serialization is parallel
 *  Process each parts in threads
 *  
 *  Final Joiner. 
 * @author abinash
 */
public class HSearchTableMultiQueryExecutor {

	static boolean DEBUG_ENABLED = false;
	
	public static final String OUTPUT_TYPE = "outputType";
	public static final String PLUGIN = "plugin";
	public static final String TABLE_PARTS = "tableParts";
	public static final int OUTPUT_ID = 0;
	public static final int OUTPUT_IDVAL = 1;
	public static final int OUTPUT_VAL = 2;
	public static final int OUTPUT_COLS = 3;

	IHSearchTableMultiQueryProcessor processor = null;
	
	public HSearchTableMultiQueryExecutor(IHSearchTableMultiQueryProcessor processor) {
		this.processor = processor;
	}
	
	public byte[] executeForIds (Map<String, HSearchTableParts> tableParts, String multiQueryStmt, 
			Map<String,QueryPart> multiQueryParts ) throws Exception {
		
		return execute (tableParts, multiQueryStmt, multiQueryParts, HSearchTableMultiQueryExecutor.OUTPUT_ID);
	}	

	public byte[] executeForValues (Map<String, HSearchTableParts> tableParts, String multiQueryStmt, 
			Map<String,QueryPart> multiQueryParts ) throws Exception {
		
		return execute (tableParts, multiQueryStmt, multiQueryParts, HSearchTableMultiQueryExecutor.OUTPUT_VAL);
	}	


	public byte[] executeForIdValues (Map<String, HSearchTableParts> tableParts, String multiQueryStmt, 
			Map<String,QueryPart> multiQueryParts) throws Exception {

		return execute (tableParts, multiQueryStmt, multiQueryParts, HSearchTableMultiQueryExecutor.OUTPUT_IDVAL);
		
	}	

	public byte[] executeForCols (Map<String, HSearchTableParts> tableParts, String multiQueryStmt, 
			Map<String,QueryPart> multiQueryParts ) throws Exception {
		
		return execute (tableParts, multiQueryStmt, multiQueryParts, HSearchTableMultiQueryExecutor.OUTPUT_COLS);
	}	
	
	public byte[] execute (Map<String, HSearchTableParts> tableParts, String multiQueryStmt, 
			Map<String,QueryPart> multiQueryParts, int resultType) throws Exception {
		
		if ( null == tableParts) {
			System.err.println("Warning : TableParts is not found. Input bytes are not set.");
			return null;
		}
		
		for (String queryId : multiQueryParts.keySet()) {
			HSearchTableParts part = tableParts.get(queryId); 
			
			if ( null == part) System.err.println("Warning : Null table part bytes for " + queryId + "\n" + 
					queryId + " is not in the supplied set :" + tableParts.keySet().toString());
			
			multiQueryParts.get(queryId).setParam(HSearchTableMultiQueryExecutor.TABLE_PARTS, part);
			multiQueryParts.get(queryId).setParam(HSearchTableMultiQueryExecutor.OUTPUT_TYPE, resultType);
		}
		
		FederatedFacade<Long, Integer> ff = processor.getProcessor();
		List<FederatedFacade<Long, Integer>.IRowId> matchingIds = ff.execute(multiQueryStmt, multiQueryParts);
		
		if  ( DEBUG_ENABLED ) {
			StringBuilder sb = new StringBuilder();
			
			for (@SuppressWarnings("rawtypes") IRowId iRowId : matchingIds) {
				if ( sb.length() > 0) sb.append(',');
				sb.append(iRowId.getDocId().toString());
			}
			L.getInstance().logDebug(" MultiQuery Output Ids :" + sb.toString());
		}
		
		return serializeMatchingIds(matchingIds);
	}
	
	/**
	 * Stream out data of only the  final matching ids of all 
	 * @param ids
	 * @return
	 * @throws NotImplementedException
	 */
	public static byte[] serializeMatchingIds(List<FederatedFacade<Long, Integer>.IRowId> ids) throws Exception {
		if ( DEBUG_ENABLED ) L.getInstance().logDebug( " getRowKeys > serializeMatchingIds." );
		Builder idL = ByteArrays.ArrayInt.newBuilder();
		StringBuffer sb = null;
		if ( DEBUG_ENABLED ) sb = new StringBuffer();
		
		for (FederatedFacade<Long, Integer>.IRowId iRowId : ids) {
			if ( null == iRowId) {
				L.getInstance().logWarning(" HSearch Plugin - iRowId : is null." );
				continue;
			}
			Integer docId = iRowId.getDocId();
			if ( null == docId) {
				L.getInstance().logWarning( " HSearch Plugin - DocId : is null." );
				continue;
			}
			idL.addVal(docId);
			if ( DEBUG_ENABLED ) sb.append(docId.toString()).append(',');
		}
		if ( DEBUG_ENABLED ) L.getInstance().logDebug( "Ids :" + sb.toString() );
		return idL.build().toByteArray();
	}	
	
	public static List<Integer> deSerializeMatchingIds(byte[] input) throws Exception {
		return ByteArrays.ArrayInt.parseFrom(input).getValList();
	}		
}
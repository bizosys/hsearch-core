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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.Filter;

import com.bizosys.hsearch.byteutils.SortedBytesDouble;
import com.bizosys.hsearch.byteutils.SortedBytesLong;
import com.bizosys.hsearch.byteutils.SortedBytesString;
import com.bizosys.hsearch.federate.FederatedFacade;
import com.bizosys.hsearch.federate.QueryPart;
import com.bizosys.hsearch.hbase.HbaseLog;
import com.bizosys.hsearch.treetable.client.HSearchTableMultiQueryExecutor;
import com.bizosys.hsearch.treetable.client.HSearchTableParts;
import com.bizosys.hsearch.treetable.client.IHSearchPlugin;
import com.bizosys.hsearch.treetable.client.L;
import com.bizosys.hsearch.treetable.client.OutputType;

/**
 * @author abinash
 *
 */
public abstract class HSearchGenericFilter implements Filter {

	public static boolean DEBUG_ENABLED = HbaseLog.l.isDebugEnabled();
	
	String multiQuery = null;
	Map<String, String> queryFilters = null;
	Map<String,QueryPart> queryPayload = new HashMap<String, QueryPart>();
	Map<String, String> colIdWithType = new HashMap<String, String>();
	boolean hasMatchingIds = false;
	
	OutputType outputType = new OutputType();

	public HSearchGenericFilter(){
	}
	
	public HSearchGenericFilter(OutputType outputType, String query, Map<String, String> details){
		this.multiQuery = query;
		this.queryFilters = details;
		this.outputType = outputType;
	}
	
	public int getTotalQueryParts() throws IOException {
		if ( null != this.queryFilters) return this.queryFilters.size();
		if ( null != this.queryPayload) return this.queryPayload.size();
		throw new IOException("Unable to find total queries inside the multi query.");
	}
	
	
	/**
	 * output type
	 * structured:A OR unstructured:B
	 * structured:A=f|1|1|1|c|*|*
	 * unstructured:B=*|*|*|*|*|*
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		StringBuilder sb = new StringBuilder();
		
		sb.append(outputType.toString()).append('\n');
		sb.append(this.multiQuery);
		
		if ( null != queryFilters) {
			for (String queryP : queryFilters.keySet()) {
				String input = queryFilters.get(queryP);
				sb.append('\n').append(queryP).append('=').append(input.toString());
			}
		}
		
		if ( DEBUG_ENABLED ) {
			HbaseLog.l.debug("Sending to HBase : " + sb.toString());
		}
		
		byte[] ser = sb.toString().getBytes();
		out.writeInt(ser.length);
		out.write(ser);
	}	

	/**
	 * output type
	 * structured:A OR unstructured:B
	 * structured:A=f|1|1|1|c|*|*
	 * unstructured:B=*|*|*|*|*|*
	 * TODO:// Replace with Fast Split.
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		try {
			int length = in.readInt();
			if ( 0 == length) throw new IOException("Invalid Query");
			
			byte[] ser = new byte[length];
			in.readFully(ser, 0, length);

			StringTokenizer stk = new StringTokenizer(new String(ser), "\n");
			
			int lineNo = -1;
			while ( stk.hasMoreTokens() ) {
				
				lineNo++;
				
				switch ( lineNo ) {
					case 0:
						String output = stk.nextToken();
						if (output.length() == 0  ) throw new IOException("Unknown result output type.");
						this.outputType = new OutputType(output);
						break;
						
					case 1:
						this.multiQuery = stk.nextToken();
						this.queryPayload = new HashMap<String, QueryPart>();

						if ( DEBUG_ENABLED ) {
							HbaseLog.l.debug("HBase Region Server: Multi Query" +  this.multiQuery);
						}
						break;

					default:
						String line = stk.nextToken();
						int splitIndex = line.indexOf('=');
						if ( -1 == splitIndex) throw new IOException("Expecting [=] in line " + line);
						
						String colNameQuolonId = line.substring(0,splitIndex);
						String filtersPipeSeparated =  line.substring(splitIndex+1);
						
						int colNameAndQIdSplitIndex = colNameQuolonId.indexOf(':');
						if ( -1 == colNameAndQIdSplitIndex || colNameQuolonId.length() - 1 == colNameAndQIdSplitIndex) {
							throw new IOException("Sub queries expected as  X:Y eg.\n" + 
									 "family1:A OR family2:B\nfamily1:A=f|1|1|1|c|*|*\nfamily2:B=*|*|*|*|*|*");
						}
						String colName = colNameQuolonId.substring(0,colNameAndQIdSplitIndex);
						String qId =  colNameQuolonId.substring(colNameAndQIdSplitIndex+1);
						
						if ( DEBUG_ENABLED ) {
							HbaseLog.l.debug("colName:qId = " + colName + "/" + qId);
						}
						
						colIdWithType.put(qId, colName);
						
						IHSearchPlugin plugin = createPlugIn(colName) ;
						plugin.setOutputType(this.outputType);
						
						this.queryPayload.put(
								colNameQuolonId, new QueryPart(filtersPipeSeparated,
									HSearchTableMultiQueryExecutor.PLUGIN, plugin) );
						

						if ( DEBUG_ENABLED ) {
							HbaseLog.l.debug("HBase Region Server: Query Payload" +  line);
						}
						break;
				}
			}
		} catch (Exception ex) {
			L.getInstance().flush();
		} finally {
			L.getInstance().clear();
		}
	}
	
	@Override
	public void filterRow(List<KeyValue> kvL) {
		if ( null == kvL) return;
		int kvT = kvL.size();
		if ( 0 == kvT) return;
		
		if ( DEBUG_ENABLED ) {
			HbaseLog.l.debug("Processing @ Region Server : filterRow" );
		}
		
		
		try {
			byte[] row = null;
			byte[] firstFamily = null;
			byte[] firstCol = null;
			
			Map<String, HSearchTableParts> colParts = new HashMap<String, HSearchTableParts>();
			//colParts.put("structured:A", bytes);
			
			HSearchTableMultiQueryExecutor intersector = createExector();

			for (KeyValue kv : kvL) {
				if ( null == kv) continue;

				byte[] inputData = kv.getValue();
				if ( null == inputData) continue;
				
				String fName = new String(kv.getFamily());
				
				int fNameI = fName.indexOf('_');
				if ( fNameI > -1 ) fName = fName.substring(0, fNameI - 1);
				
				HSearchTableParts parts =  null;
				if ( colParts.containsKey(fName)) {
					parts = colParts.get(fName);
				} else {
					parts = new HSearchTableParts();
					colParts.put(fName, parts);
				}

				parts.collect(inputData);
				if ( null == row ) {
					firstFamily = kv.getFamily();
					firstCol = kv.getQualifier();
					row = kv.getRow();
				}
			}
			
			if ( DEBUG_ENABLED ) {
				HbaseLog.l.debug("queryData HSearchTableParts creation. ");
			}
			
			Map<String, HSearchTableParts> queryData = new HashMap<String, HSearchTableParts>();
			for (String queryId : colIdWithType.keySet()) { //A
				String queryType = colIdWithType.get(queryId); //structured
				HSearchTableParts parts = colParts.get(queryType);
				
				String queryTypeWithId = queryType + ":" + queryId;
				HbaseLog.l.debug(queryTypeWithId);
				if ( DEBUG_ENABLED ) {
					HbaseLog.l.debug("Query Parts for " + queryTypeWithId);
				}
				
				queryData.put(queryTypeWithId, parts);
			}
			colParts.clear();
			colParts = null;

			if ( DEBUG_ENABLED ) HbaseLog.l.debug("HSearchGenericFilter: Filteration Starts");
			List<FederatedFacade<String, String>.IRowId> intersectedIds = federatedQueryExec(row, intersector, queryData);
			
			kvL.clear(); //Clear all data
			if ( hasMatchingIds) {
				kvL.add(new KeyValue(row, firstFamily, firstCol, serializeOutput(intersectedIds, this.queryPayload) ) );
			}
			
		} catch (Exception ex) {
			ex.printStackTrace(System.err);
		
		} finally {
			L.getInstance().flush();
		}
	}

	private List<FederatedFacade<String, String>.IRowId> federatedQueryExec(byte[] row,
			HSearchTableMultiQueryExecutor intersector,
			Map<String, HSearchTableParts> queryData) throws Exception,
			IOException {
		
		List<FederatedFacade<String, String>.IRowId> intersectedIds = null;
		intersectedIds = intersector.execute(queryData, this.multiQuery, this.queryPayload, outputType);
		
		if ( DEBUG_ENABLED ) {
			HbaseLog.l.debug( new String(row) + " has ids of :" + intersectedIds.size());
		}
		
		hasMatchingIds = ( null != intersectedIds && intersectedIds.size() > 0 );
		
		HbaseLog.l.debug("Generaic filter hasMatchingIds :" + hasMatchingIds);
		return intersectedIds;
	}
	

	@Override
	public void reset() {
		hasMatchingIds = false;
	}	
	
	@Override
	public boolean hasFilterRow() {
		return true;
	}	
	
	@Override
	public KeyValue getNextKeyHint(KeyValue arg0) {
		return null;
	}	
	
	@Override
	public boolean filterRowKey(byte[] rowKey, int offset, int length) {
		return false;
	}
	
	@Override
	public boolean filterAllRemaining() {
		return false;
	}
	
	@Override
	public boolean filterRow() {
		return false;
	}
	
	@Override
	public ReturnCode filterKeyValue(KeyValue arg0) {
		return ReturnCode.INCLUDE;
	}	
	
	/**
	 * Version 0.94 FIX
	 */
	@Override
	public KeyValue transform(KeyValue arg0) {
		return arg0;
	}
	
	
	/**
	 *******************************************************************************************
	 * COMPUTATIONS 
	 *******************************************************************************************
	 */

	public byte[] serializeOutput(List<FederatedFacade<String, String>.IRowId> matchedIds,
		Map<String,QueryPart> queryPayload) throws IOException {

		if ( DEBUG_ENABLED ) L.getInstance().logDebug( " getRowKeys > serializeMatchingIds." );
		
		switch (outputType.getOutputType()) {
			
			case OutputType.OUTPUT_COUNT:
				return serializeCounts(matchedIds, queryPayload);
			
			case OutputType.OUTPUT_MIN:
			case OutputType.OUTPUT_MAX:
			case OutputType.OUTPUT_AVG:
			case OutputType.OUTPUT_SUM:
				Collection<Double> outputL = new ArrayList<Double>(4);
				serializeAggvValues(matchedIds, queryPayload, outputType.getOutputType(), outputL);
				return SortedBytesDouble.getInstance().toBytes(outputL);
				
			case OutputType.OUTPUT_MIN_MAX:
				return serializeAggvValuesChained(matchedIds, queryPayload, 
					new int[] {OutputType.OUTPUT_MIN, OutputType.OUTPUT_MAX});

			case OutputType.OUTPUT_MIN_MAX_AVG:
				return serializeAggvValuesChained(matchedIds, queryPayload, 
					new int[] {OutputType.OUTPUT_MIN, OutputType.OUTPUT_MAX, OutputType.OUTPUT_AVG});

			case OutputType.OUTPUT_MIN_MAX_COUNT:
				return serializeAggvValuesChained(matchedIds, queryPayload, 
					new int[] {OutputType.OUTPUT_MIN, OutputType.OUTPUT_MAX, OutputType.OUTPUT_COUNT});

			case OutputType.OUTPUT_MIN_MAX_AVG_COUNT:
				return serializeAggvValuesChained(matchedIds, queryPayload, 
					new int[] {OutputType.OUTPUT_MIN, OutputType.OUTPUT_MAX, OutputType.OUTPUT_AVG, OutputType.OUTPUT_COUNT});
				
			case OutputType.OUTPUT_MIN_MAX_SUM:
				return serializeAggvValuesChained(matchedIds, queryPayload, 
					new int[] {OutputType.OUTPUT_MIN, OutputType.OUTPUT_MAX, OutputType.OUTPUT_SUM});

			case OutputType.OUTPUT_MIN_MAX_SUM_AVG:
				return serializeAggvValuesChained(matchedIds, queryPayload, 
					new int[] {OutputType.OUTPUT_MIN, OutputType.OUTPUT_MAX, 
						OutputType.OUTPUT_SUM, OutputType.OUTPUT_AVG});

			case OutputType.OUTPUT_MIN_MAX_SUM_COUNT:
				return serializeAggvValuesChained(matchedIds, queryPayload, 
					new int[] {OutputType.OUTPUT_MIN, OutputType.OUTPUT_MAX, 
						OutputType.OUTPUT_SUM, OutputType.OUTPUT_COUNT});

			case OutputType.OUTPUT_MIN_MAX_AVG_SUM_COUNT:
				return serializeAggvValuesChained(matchedIds, queryPayload, 
					new int[] {OutputType.OUTPUT_MIN, OutputType.OUTPUT_MAX, OutputType.OUTPUT_AVG,
						OutputType.OUTPUT_SUM, OutputType.OUTPUT_COUNT});

			case OutputType.CALLBACK_ID:
				return serializeDocIds(matchedIds, queryPayload);

			case OutputType.CALLBACK_IDVAL:
				return serializeIdAndValues(matchedIds, queryPayload);
				
			case OutputType.CALLBACK_VAL:
				return serializeValues(matchedIds, queryPayload);

			case OutputType.CALLBACK_COLS:
				return serializeColumns(matchedIds, queryPayload);
		}
		
		throw new IOException("Serialization, Type not implemented :" + this.outputType.getOutputType());
		
	}

	private byte[] serializeCounts(
			List<FederatedFacade<String, String>.IRowId> matchedIds,
			Map<String, QueryPart> queryPayload) throws IOException {
		
		List<Long> countL = new ArrayList<Long>();
		long multiQueryCount = ( null == matchedIds ) ? 0: matchedIds.size();
		countL.add(multiQueryCount);
		
		for (QueryPart part : queryPayload.values()) {
			
			Object pluginO =  part.getParams().get(HSearchTableMultiQueryExecutor.PLUGIN);
			if ( null == pluginO) throw new IOException("Plugin object is not found, NULL");
			IHSearchPlugin plugin =  (IHSearchPlugin) pluginO;
			countL.add(plugin.getCount(matchedIds));
		}
		return SortedBytesLong.getInstance().toBytes(countL);
	}
	
	private byte[] serializeAggvValuesChained( List<FederatedFacade<String, String>.IRowId> matchedIds,
			Map<String, QueryPart> queryPayload, int[] outputCode) throws IOException {
		
		Collection<Double> outputList = new ArrayList<Double>();
		for (int code : outputCode) {
			serializeAggvValues(matchedIds, queryPayload, code, outputList);
		}
		return SortedBytesDouble.getInstance().toBytes(outputList);
		
	}
	
	private void serializeAggvValues( List<FederatedFacade<String, String>.IRowId> matchedIds,
			Map<String, QueryPart> queryPayload, int outputCode,
			Collection<Double> outputL) throws IOException {

		double outputMultiQuery = -1;
		double[] outputQueryParts = new double[queryPayload.size()];
		
		switch(outputCode) {
			case OutputType.OUTPUT_COUNT:
			case OutputType.OUTPUT_SUM:
			case OutputType.OUTPUT_AVG:
				outputMultiQuery = 0; 
				Arrays.fill(outputQueryParts, 0);
				break;
			case OutputType.OUTPUT_MAX:
				outputMultiQuery = Long.MIN_VALUE; 
				Arrays.fill(outputQueryParts, Long.MIN_VALUE);
				break;
			case OutputType.OUTPUT_MIN:
				outputMultiQuery = Long.MAX_VALUE; 
				Arrays.fill(outputQueryParts, Long.MAX_VALUE);
				break;
				
			default:
				throw new IOException("Not imeplemented Yet");
		}
		
		int seq = 0;
		
		for (QueryPart part : queryPayload.values()) {
			
			Object pluginO =  part.getParams().get(HSearchTableMultiQueryExecutor.PLUGIN);
			if ( null == pluginO) throw new IOException("Plugin object is not found, NULL");
			IHSearchPlugin plugin =  (IHSearchPlugin) pluginO;
			
			switch(outputCode) {
				case OutputType.OUTPUT_COUNT:
					outputQueryParts[seq] = plugin.getCount(matchedIds);
					outputMultiQuery += outputQueryParts[seq]; 
					break;
				case OutputType.OUTPUT_AVG:
					outputQueryParts[seq] = plugin.getAvg(matchedIds);
					outputMultiQuery += outputQueryParts[seq]; 
					outputMultiQuery = outputMultiQuery / 2;
					break;
				case OutputType.OUTPUT_MAX:
					outputQueryParts[seq] = plugin.getMax(matchedIds);
					if ( outputMultiQuery < outputQueryParts[seq]) outputMultiQuery = outputQueryParts[seq];
					break;
				case OutputType.OUTPUT_MIN:
					outputQueryParts[seq] = plugin.getMin(matchedIds);
					if ( outputMultiQuery > outputQueryParts[seq]) outputMultiQuery = outputQueryParts[seq];
					break;
				case OutputType.OUTPUT_SUM:
					outputQueryParts[seq] = plugin.getSum(matchedIds);
					outputMultiQuery += outputQueryParts[seq]; 
					break;
			}
			seq++;
		}

		outputL.add(outputMultiQuery);
		for (double outputQueryPart : outputQueryParts) {
			outputL.add(outputQueryPart);	
		}
	}
	
	protected byte[] serializeDocIds( List<FederatedFacade<String, String>.IRowId> matchedIds,
			Map<String, QueryPart> queryPayload) throws IOException {			
		Collection<String> idL = new ArrayList<String>(1024);
		StringBuffer sb = null;
		if ( DEBUG_ENABLED ) sb = new StringBuffer();
		
		for (FederatedFacade<String, String>.IRowId iRowId : matchedIds) {
			if ( null == iRowId) {
				L.getInstance().logWarning(" HSearch Plugin - iRowId : is null." );
				continue;
			}
			String docId = iRowId.getDocId();
			if ( null == docId) {
				L.getInstance().logWarning( " HSearch Plugin - DocId : is null." );
				continue;
			}
			idL.add(docId);
			if ( DEBUG_ENABLED ) sb.append(docId.toString()).append(',');
		}
		if ( DEBUG_ENABLED ) L.getInstance().logDebug( "Ids :" + sb.toString() );
		return SortedBytesString.getInstance().toBytes(idL);
	}
	
	@SuppressWarnings("rawtypes")
	public Collection deSerializeOutput(byte[] input, Collection values) throws IOException {
		
		if ( DEBUG_ENABLED ) L.getInstance().logDebug(
			"HSearch Generic Filter getRowKeys > de-serializeMatchingIds : " + outputType.getCallbackType() );
		
		switch (outputType.getOutputType()) {
			
			case OutputType.OUTPUT_COUNT:
				return SortedBytesLong.getInstance().parse(input).values(values);
				
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
				return SortedBytesDouble.getInstance().parse(input).values(values);

			case OutputType.CALLBACK_ID:
				return SortedBytesString.getInstance().parse(input).values(values);

			case OutputType.CALLBACK_IDVAL:
				return deserializeIdAndValues(input, values);
				
			case OutputType.CALLBACK_VAL:
				return deserializeValues(input, values);

			case OutputType.CALLBACK_COLS:
				return deserializeColumns(input, values);
		}
		
		throw new IOException("Deserialization Failute, type not implemented :" + this.outputType.getOutputType());
	}			
	
	public abstract HSearchTableMultiQueryExecutor createExector();
	public abstract IHSearchPlugin createPlugIn(String type) throws IOException ;
	
	public abstract byte[] serializeValues(List<FederatedFacade<String, String>.IRowId> matchedIds,
			Map<String,QueryPart> queryPayload) throws IOException;
	
	@SuppressWarnings("rawtypes")
	public abstract List deserializeValues(byte[] input, Collection values) throws IOException;

	public abstract byte[] serializeIdAndValues(List<FederatedFacade<String, String>.IRowId> matchedIds,
			Map<String,QueryPart> queryPayload) throws IOException;
	
	@SuppressWarnings("rawtypes")
	public abstract List deserializeIdAndValues(byte[] input, Collection values) throws IOException;
	
	public abstract byte[] serializeColumns(List<FederatedFacade<String, String>.IRowId> matchedIds,
			Map<String,QueryPart> queryPayload) throws IOException;

	@SuppressWarnings("rawtypes")
	public abstract List deserializeColumns(byte[] input, Collection values) throws IOException;
	
	
}
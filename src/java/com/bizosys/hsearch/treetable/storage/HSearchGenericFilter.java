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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.Filter;

import com.bizosys.hsearch.byteutils.SortedBytesArray;
import com.bizosys.hsearch.byteutils.SortedBytesDouble;
import com.bizosys.hsearch.byteutils.SortedBytesInteger;
import com.bizosys.hsearch.byteutils.SortedBytesString;
import com.bizosys.hsearch.federate.FederatedFacade;
import com.bizosys.hsearch.federate.QueryPart;
import com.bizosys.hsearch.hbase.HbaseLog;
import com.bizosys.hsearch.treetable.client.HSearchPluginPoints;
import com.bizosys.hsearch.treetable.client.HSearchTableMultiQueryExecutor;
import com.bizosys.hsearch.treetable.client.HSearchTableMultiQueryProcessor;
import com.bizosys.hsearch.treetable.client.HSearchTableParts;
import com.bizosys.hsearch.treetable.client.IHSearchPlugin;
import com.bizosys.hsearch.treetable.client.L;
import com.bizosys.hsearch.util.LineReaderUtil;

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
	
	HSearchPluginPoints outputType = new HSearchPluginPoints();

	public HSearchGenericFilter(){
	}
	
	public HSearchGenericFilter(HSearchPluginPoints outputType, String query, Map<String, String> details){
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
						this.outputType = new HSearchPluginPoints(output);
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

			//colParts.put("structured:A", bytes);
			Map<String, HSearchTableParts> colNamesWithPartitionBytes = new HashMap<String, HSearchTableParts>();
			
			HSearchTableMultiQueryExecutor intersector = createExecutor();

			//HBase Family Name = schema column name + "_" + partition
			String columnNameWithParition = null;
			String colName = null;
			for (KeyValue kv : kvL) {
				if ( null == kv) continue;

				byte[] inputData = kv.getValue();
				if ( null == inputData) continue;
				columnNameWithParition = new String(kv.getFamily());
				
				int partitionIndex = columnNameWithParition.indexOf('_');
				colName = ( partitionIndex == -1 ) ? columnNameWithParition : 
					columnNameWithParition.substring(0, partitionIndex);
				
				HSearchTableParts tableParts =  null;
				if ( colNamesWithPartitionBytes.containsKey(colName)) {
					tableParts = colNamesWithPartitionBytes.get(colName);
				} else {
					tableParts = new HSearchTableParts();
					colNamesWithPartitionBytes.put(colName, tableParts);
				}
				tableParts.put(inputData);

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
				HSearchTableParts parts = colNamesWithPartitionBytes.get(queryType);
				
				String queryTypeWithId = queryType + ":" + queryId;

				if ( DEBUG_ENABLED ) {
					HbaseLog.l.debug(queryTypeWithId);
					HbaseLog.l.debug("Query Parts for " + queryTypeWithId);
				}
				
				queryData.put(queryTypeWithId, parts);
			}
			colNamesWithPartitionBytes.clear();
			colNamesWithPartitionBytes = null;

			if ( DEBUG_ENABLED ) HbaseLog.l.debug("HSearchGenericFilter: Filteration Starts");
			List<FederatedFacade<String, String>.IRowId> intersectedIds = 
				federatedQueryExec(row, intersector, queryData);
			
			kvL.clear(); //Clear all data
			if ( hasMatchingIds) {
				kvL.add(new KeyValue(row, firstFamily, firstCol, 
					serializeOutput(intersectedIds, this.queryPayload) ) );
			}
			
			
			if ( null != HSearchTableMultiQueryProcessor.processor) 
				HSearchTableMultiQueryProcessor.processor.objectFactory.putprimaryKeyRowId(intersectedIds);
			
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
		
		hasMatchingIds = ( null != intersectedIds && intersectedIds.size() > 0 );

		if ( DEBUG_ENABLED ) {
			HbaseLog.l.debug("Generaic filter hasMatchingIds :" + hasMatchingIds);
			if ( hasMatchingIds ) HbaseLog.l.debug( new String(row) + " has ids of :" + intersectedIds.size());
		}
		
		
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
			
			case HSearchPluginPoints.OUTPUT_COUNT:
				return serializeCounts(matchedIds, queryPayload);
			
			case HSearchPluginPoints.OUTPUT_MIN:
			case HSearchPluginPoints.OUTPUT_MAX:
			case HSearchPluginPoints.OUTPUT_AVG:
			case HSearchPluginPoints.OUTPUT_SUM:
				Collection<Double> outputL = new ArrayList<Double>(4);
				serializeAggvValues(matchedIds, queryPayload, outputType.getOutputType(), outputL);
				return SortedBytesDouble.getInstance().toBytes(outputL);
				
			case HSearchPluginPoints.OUTPUT_MIN_MAX:
				return serializeAggvValuesChained(matchedIds, queryPayload, 
					new int[] {HSearchPluginPoints.OUTPUT_MIN, HSearchPluginPoints.OUTPUT_MAX});

			case HSearchPluginPoints.OUTPUT_MIN_MAX_AVG:
				return serializeAggvValuesChained(matchedIds, queryPayload, 
					new int[] {HSearchPluginPoints.OUTPUT_MIN, HSearchPluginPoints.OUTPUT_MAX, HSearchPluginPoints.OUTPUT_AVG});

			case HSearchPluginPoints.OUTPUT_MIN_MAX_COUNT:
				return serializeAggvValuesChained(matchedIds, queryPayload, 
					new int[] {HSearchPluginPoints.OUTPUT_MIN, HSearchPluginPoints.OUTPUT_MAX, HSearchPluginPoints.OUTPUT_COUNT});

			case HSearchPluginPoints.OUTPUT_MIN_MAX_AVG_COUNT:
				return serializeAggvValuesChained(matchedIds, queryPayload, 
					new int[] {HSearchPluginPoints.OUTPUT_MIN, HSearchPluginPoints.OUTPUT_MAX, HSearchPluginPoints.OUTPUT_AVG, HSearchPluginPoints.OUTPUT_COUNT});
				
			case HSearchPluginPoints.OUTPUT_MIN_MAX_SUM:
				return serializeAggvValuesChained(matchedIds, queryPayload, 
					new int[] {HSearchPluginPoints.OUTPUT_MIN, HSearchPluginPoints.OUTPUT_MAX, HSearchPluginPoints.OUTPUT_SUM});

			case HSearchPluginPoints.OUTPUT_AVG_SUM_COUNT:
				return serializeAggvValuesChained(matchedIds, queryPayload, 
					new int[] {HSearchPluginPoints.OUTPUT_AVG, HSearchPluginPoints.OUTPUT_SUM, HSearchPluginPoints.OUTPUT_COUNT});

			case HSearchPluginPoints.OUTPUT_MIN_MAX_SUM_AVG:
				return serializeAggvValuesChained(matchedIds, queryPayload, 
					new int[] {HSearchPluginPoints.OUTPUT_MIN, HSearchPluginPoints.OUTPUT_MAX, 
						HSearchPluginPoints.OUTPUT_SUM, HSearchPluginPoints.OUTPUT_AVG});

			case HSearchPluginPoints.OUTPUT_MIN_MAX_SUM_COUNT:
				return serializeAggvValuesChained(matchedIds, queryPayload, 
					new int[] {HSearchPluginPoints.OUTPUT_MIN, HSearchPluginPoints.OUTPUT_MAX, 
						HSearchPluginPoints.OUTPUT_SUM, HSearchPluginPoints.OUTPUT_COUNT});

			case HSearchPluginPoints.OUTPUT_MIN_MAX_AVG_SUM_COUNT:
				return serializeAggvValuesChained(matchedIds, queryPayload, 
					new int[] {HSearchPluginPoints.OUTPUT_MIN, HSearchPluginPoints.OUTPUT_MAX, HSearchPluginPoints.OUTPUT_AVG,
						HSearchPluginPoints.OUTPUT_SUM, HSearchPluginPoints.OUTPUT_COUNT});

			case HSearchPluginPoints.OUTPUT_FACETS:
				return serializeFacets(matchedIds, queryPayload);
				
			case HSearchPluginPoints.PLUGIN_CALLBACK_ID:
				return serializeDocIds(matchedIds, queryPayload);

			case HSearchPluginPoints.PLUGIN_CALLBACK_IDVAL:
				return serializeIdAndValues(matchedIds, queryPayload);
				
			case HSearchPluginPoints.PLUGIN_CALLBACK_VAL:
				return serializeValues(matchedIds, queryPayload);

			case HSearchPluginPoints.PLUGIN_CALLBACK_COLS:
				return serializeColumns(matchedIds, queryPayload);
		}
		
		throw new IOException("Serialization, Type not implemented :" + this.outputType.getOutputType());
		
	}

	private byte[] serializeCounts(
			List<FederatedFacade<String, String>.IRowId> matchedIds,
			Map<String, QueryPart> queryPayload) throws IOException {
		
		List<Double> countL = new ArrayList<Double>();
		double multiQueryCount = ( null == matchedIds ) ? 0: matchedIds.size();
		countL.add(multiQueryCount);
		
		for (QueryPart part : queryPayload.values()) {
			
			Object pluginO =  part.getParams().get(HSearchTableMultiQueryExecutor.PLUGIN);
			if ( null == pluginO) throw new IOException("Plugin object is not found, NULL");
			IHSearchPlugin plugin =  (IHSearchPlugin) pluginO;
			countL.add(plugin.getCount(matchedIds));
		}
		return SortedBytesDouble.getInstance().toBytes(countL);
	}
	
	private final byte[] serializeFacets(
			List<FederatedFacade<String, String>.IRowId> matchedIds,
			Map<String, QueryPart> queryPayload) throws IOException {
		
		Map<String, Integer> facets = new HashMap<String, Integer>();
		
		for (QueryPart part : queryPayload.values()) {
			Object pluginO =  part.getParams().get(HSearchTableMultiQueryExecutor.PLUGIN);
			if ( null == pluginO) throw new IOException("Plugin object is not found, NULL");
			IHSearchPlugin plugin =  (IHSearchPlugin) pluginO;
			plugin.calculateFacets(facets, matchedIds);
		}
	
		return facetsToBytes(facets);
	}

	public static byte[] facetsToBytes(Map<String, Integer> facets)
			throws IOException {
		List<byte[]> kvB = new ArrayList<byte[]>(2);
		kvB.add( SortedBytesString.getInstance().toBytes(facets.keySet()) );
		kvB.add( SortedBytesInteger.getInstance().toBytes(facets.values()) );
		return SortedBytesArray.getInstance().toBytes(kvB);
	}
	
	private final byte[] serializeAggvValuesChained( List<FederatedFacade<String, String>.IRowId> matchedIds,
			Map<String, QueryPart> queryPayload, int[] outputCode) throws IOException {
		
		Collection<Double> outputList = new ArrayList<Double>();
		for (int code : outputCode) {
			serializeAggvValues(matchedIds, queryPayload, code, outputList);
		}
		
		if ( DEBUG_ENABLED) HbaseLog.l.debug("Output List : " + outputList.toString());
		return SortedBytesDouble.getInstance().toBytes(outputList);
		
	}
	
	private final void serializeAggvValues( List<FederatedFacade<String, String>.IRowId> matchedIds,
			Map<String, QueryPart> queryPayload, int outputCode,
			Collection<Double> outputL) throws IOException {

		double outputMultiQuery = -1;
		double[] outputQueryParts = new double[queryPayload.size()];
		
		switch(outputCode) {
			case HSearchPluginPoints.OUTPUT_COUNT:
			case HSearchPluginPoints.OUTPUT_SUM:
			case HSearchPluginPoints.OUTPUT_AVG:
				outputMultiQuery = 0; 
				Arrays.fill(outputQueryParts, 0);
				break;
			case HSearchPluginPoints.OUTPUT_MAX:
				outputMultiQuery = Long.MIN_VALUE; 
				Arrays.fill(outputQueryParts, Long.MIN_VALUE);
				break;
			case HSearchPluginPoints.OUTPUT_MIN:
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
				case HSearchPluginPoints.OUTPUT_COUNT:
					outputQueryParts[seq] = plugin.getCount(matchedIds);
					outputMultiQuery += outputQueryParts[seq]; 
					break;
				case HSearchPluginPoints.OUTPUT_AVG:
					outputQueryParts[seq] = plugin.getAvg(matchedIds);
					outputMultiQuery += outputQueryParts[seq]; 
					outputMultiQuery = outputMultiQuery / 2;
					break;
				case HSearchPluginPoints.OUTPUT_MAX:
					outputQueryParts[seq] = plugin.getMax(matchedIds);
					if ( outputMultiQuery < outputQueryParts[seq]) outputMultiQuery = outputQueryParts[seq];
					break;
				case HSearchPluginPoints.OUTPUT_MIN:
					outputQueryParts[seq] = plugin.getMin(matchedIds);
					if ( outputMultiQuery > outputQueryParts[seq]) outputMultiQuery = outputQueryParts[seq];
					break;
				case HSearchPluginPoints.OUTPUT_SUM:
					outputQueryParts[seq] = plugin.getSum(matchedIds);
					outputMultiQuery += outputQueryParts[seq]; 
					break;
			}
			seq++;
		}

		//For the top row
		if ( DEBUG_ENABLED ) HbaseLog.l.debug( outputCode + "|" + outputMultiQuery);
		outputL.add(outputMultiQuery);
		
		//For the all queries
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

	public Collection<Double> deSerializeCounts(byte[] input, Collection<Double> output) throws IOException {
		if ( DEBUG_ENABLED ) L.getInstance().logDebug(
				"HSearch Generic Filter getRowKeys > de-serializeMatchingIds : " + outputType.getCallbackType() );
		return SortedBytesDouble.getInstance().parse(input).values(output);
	}
	
	public Collection<Double> deSerializeAgreegates(byte[] input, Collection<Double> output) throws IOException {
		
		if ( DEBUG_ENABLED ) L.getInstance().logDebug(
				"HSearch Generic Filter getRowKeys > de-serializeMatchingIds : " + outputType.getCallbackType() );

		switch (outputType.getOutputType()) {
		
			case HSearchPluginPoints.OUTPUT_MIN:
			case HSearchPluginPoints.OUTPUT_MAX:
			case HSearchPluginPoints.OUTPUT_AVG:
			case HSearchPluginPoints.OUTPUT_SUM:
			case HSearchPluginPoints.OUTPUT_MIN_MAX:
			case HSearchPluginPoints.OUTPUT_MIN_MAX_AVG:
			case HSearchPluginPoints.OUTPUT_MIN_MAX_COUNT:
			case HSearchPluginPoints.OUTPUT_MIN_MAX_AVG_COUNT:
			case HSearchPluginPoints.OUTPUT_MIN_MAX_SUM:
			case HSearchPluginPoints.OUTPUT_AVG_SUM_COUNT:
			case HSearchPluginPoints.OUTPUT_MIN_MAX_SUM_AVG:
			case HSearchPluginPoints.OUTPUT_MIN_MAX_SUM_COUNT:
			case HSearchPluginPoints.OUTPUT_MIN_MAX_AVG_SUM_COUNT:
				return SortedBytesDouble.getInstance().parse(input).values(output);
		}

		throw new IOException("Deserialization Failute, type not implemented :" + this.outputType.getOutputType());
	}
	
	public Collection<String> deSerializeIds(byte[] input, Collection<String> output) throws IOException {
		return SortedBytesString.getInstance().parse(input).values(output);
	}
	
	@SuppressWarnings("rawtypes")
	public Collection deSerializeCells(byte[] input, Collection output) throws IOException {
		
		switch (outputType.getOutputType()) {

			case HSearchPluginPoints.PLUGIN_CALLBACK_IDVAL:
				return deserializeIdAndValues(input, output);
				
			case HSearchPluginPoints.PLUGIN_CALLBACK_VAL:
				return deserializeValues(input, output);

			case HSearchPluginPoints.PLUGIN_CALLBACK_COLS:
				return deserializeColumns(input, output);

		}
		
		throw new IOException("Deserialization Failute, type not implemented :" + this.outputType.getOutputType());
	}			
	
	public Map<String, Integer> deSerializeFacets(byte[] input, Map<String, Integer> output) throws IOException {
		
		if ( null == input) return output;
		if ( 0 == input.length ) return output;
		
		Collection<byte[]> keyValueB = SortedBytesArray.getInstance().parse(input).values();
		
		Collection<String> keys = null;
		Collection<Integer> values = null;
		
		int seq = 0;
		for (byte[] bs : keyValueB) {
			if ( seq == 0 ) {
				keys = SortedBytesString.getInstance().parse(bs).values();
			} else {
				values = SortedBytesInteger.getInstance().parse(bs).values();
			}
			seq++;
		}

		if ( null == keys && null == values ) return output;
		
		if ( null == keys || null == values ) {
			throw new IOException("Corrupted bytes, " + keys + " - " + values);
		}
		
		if ( keys.size() != values.size()) {
			throw new IOException("Corrupted bytes, " + keys.size() + " - " + values.size());
		}
		
		Iterator<Integer> valsI = values.iterator();
		for (String key : keys) {
			output.put(key, valsI.next());
		}
		return output;
	}
	
	
	public abstract HSearchTableMultiQueryExecutor createExecutor();
	public abstract IHSearchPlugin createPlugIn(String type) throws IOException ;
	
	public byte[] serializeValues(List<FederatedFacade<String, String>.IRowId> matchedIds,
			Map<String,QueryPart> queryPayload) throws IOException {
		return serializeWithTsv(matchedIds, queryPayload, HSearchPluginPoints.PLUGIN_CALLBACK_VAL);
	}
	
	@SuppressWarnings("rawtypes")
	public Collection deserializeValues(byte[] input, Collection values) throws IOException {
		return deserializeTsv(input, values);		
	}

	public byte[] serializeIdAndValues(List<FederatedFacade<String, String>.IRowId> matchedIds,
			Map<String,QueryPart> queryPayload) throws IOException {
		return serializeWithTsv(matchedIds, queryPayload, HSearchPluginPoints.PLUGIN_CALLBACK_IDVAL);
	}
	
	@SuppressWarnings("rawtypes")
	public Collection deserializeIdAndValues(byte[] input, Collection values) throws IOException {
		return deserializeTsv(input, values);		
	}
	
	public byte[] serializeColumns(List<FederatedFacade<String, String>.IRowId> matchedIds,
			Map<String,QueryPart> queryPayload) throws IOException {
		
		return serializeWithTsv(matchedIds, queryPayload, HSearchPluginPoints.PLUGIN_CALLBACK_COLS);
	}

	@SuppressWarnings("rawtypes")
	public Collection deserializeColumns(byte[] input, Collection values) throws IOException {
		return deserializeTsv(input, values);
	}

	private byte[] serializeWithTsv( List<FederatedFacade<String, String>.IRowId> matchedIds, 
			Map<String, QueryPart> queryPayload, int outputType) throws IOException {
		if ( DEBUG_ENABLED ) {
			HbaseLog.l.debug("HSearchGenericFilter:serializeColumns : with matchedIds " +  matchedIds.size());
		}
		
		Collection<String> container = new ArrayList<String>(8192);
		for (QueryPart part : queryPayload.values()) {
			Object pluginO = part.getParams().get(HSearchTableMultiQueryExecutor.PLUGIN);
			IHSearchPlugin plugin = (IHSearchPlugin) pluginO;
			switch ( outputType ) {
				case HSearchPluginPoints.OUTPUT_COLS:
					plugin.getMatchingRowsWithTSV(matchedIds, container);
					break;
					
				case HSearchPluginPoints.PLUGIN_CALLBACK_IDVAL:
					plugin.getMatchingIdsAndValuesWithTSV(matchedIds, container);
					break;
					
				case HSearchPluginPoints.PLUGIN_CALLBACK_VAL:
					plugin.getMatchingValuesWithTSV(matchedIds, container);
					break;
			}
		}
		
		//Put it to Bytes
		int totalSize = 0;
		for (String line : container) {
			totalSize += (line.length() + 1);
		}
		
		StringBuilder sb = new StringBuilder(totalSize + 1);
		for (String line : container) {
			sb.append(line).append('\n');
		}

		if ( DEBUG_ENABLED ) {
			HbaseLog.l.debug("HSearchGenericFilter:serializeColumns : Bytes Total " +  totalSize);
		}
		
		return sb.toString().getBytes();
	}

	private Collection deserializeTsv(byte[] input, Collection values) {
		if ( null == input) return values;
		String allLines = new String(input);
		LineReaderUtil.fastSplit(values, allLines, '\n');
		return values;
	}
	
}
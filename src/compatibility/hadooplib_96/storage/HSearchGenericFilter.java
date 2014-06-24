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
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;

import com.bizosys.hsearch.byteutils.SortedBytesArray;
import com.bizosys.hsearch.federate.BitSetOrSet;
import com.bizosys.hsearch.federate.QueryPart;
import com.bizosys.hsearch.functions.HSearchReducer;
import com.bizosys.hsearch.functions.StatementWithOutput;
import com.bizosys.hsearch.treetable.client.HSearchProcessingInstruction;
import com.bizosys.hsearch.treetable.client.HSearchTableMultiQueryExecutor;
import com.bizosys.hsearch.treetable.client.HSearchTableParts;
import com.bizosys.hsearch.treetable.client.IHSearchPlugin;
import com.bizosys.hsearch.treetable.storage.protobuf.generated.HSearchFilterProtos.HSearchGenericFilterMessage;
import com.bizosys.hsearch.treetable.storage.protobuf.generated.HSearchFilterProtos.HSearchGenericFilterMessage.QueryFiltersPair;
import com.bizosys.hsearch.util.HSearchLog;
import com.google.protobuf.ByteString;

/**
 * @author abinash
 *
 */
public abstract class HSearchGenericFilter extends FilterBase {

	public static boolean DEBUG_ENABLED = HSearchLog.l.isDebugEnabled();
	public static boolean INFO_ENABLED = HSearchLog.l.isInfoEnabled();

	String name = null;
	public String getName() {
		if ( null == name) {
			name = this.getClass().getName();
		}
		return this.name;
	}

	public void setName(String name) {
		this.name = name;
	}


	/**
	 * Client side variables
	 */
	Map<String, String> queryFilters = null;

	/**
	 * Server Side Variables
	 */
	String multiQuery = null;
	IHSearchPlugin plugin = null;
	Map<String,QueryPart> queryPayload = new HashMap<String, QueryPart>(3);
	Map<String, String> colIdWithType = new HashMap<String, String>(3);

	public long pluginExecutionTime = 0L;
	public long overallExecutionTime = 0L;


	HSearchProcessingInstruction inputMapperInstructions = new HSearchProcessingInstruction();
	byte[] inputRowsToIncludeB = null;
	List<byte[]>  inputRowsList = null;
	SortedBytesArray rowsToInclude = null;

	Map<String, HSearchTableParts> queryIdWithParts = new HashMap<String, HSearchTableParts>();
	Map<String, HSearchTableParts> colNamesWithPartitionBytes = new HashMap<String, HSearchTableParts>();
	List<byte[]> columnsOfOneRowAfterJoin = new ArrayList<byte[]>();	
	List<Collection<byte[]>> stmtOutputContainers = new LinkedList<Collection<byte[]>>();
	SortedBytesArray rowBytesPacker = SortedBytesArray.getInstanceArr();

	HSearchTableMultiQueryExecutor intersector = null;

	public HSearchGenericFilter(){
	}

	public HSearchGenericFilter(final HSearchProcessingInstruction outputType, 
			final String query, final  Map<String, String> details) {
		this(outputType, query, details, null);
	}

	public HSearchGenericFilter(final HSearchProcessingInstruction outputType, 
			final String query, final  Map<String, String> details, List<byte[]> scopedToRows) {

		this.multiQuery = query;
		this.queryFilters = details;
		this.inputMapperInstructions = outputType;
		this.inputRowsList = scopedToRows;
		this.name = this.getClass().getSimpleName();
	}

	public void setScopedToRows(List<byte[]> scopedToRows) {
		this.inputRowsList = scopedToRows;
	}

	public boolean clientSideAPI_IsSingleQuery() throws IOException {
		if ( null == this.queryFilters) throw new IOException("Genric Filter is not initalized");
		if ( 1 == this.queryFilters.size()) return true;
		return false;
	}

	public String clientSideAPI_getSingleQueryWithScope() throws IOException {
		if ( null == this.queryFilters) throw new IOException("Genric Filter is not initalized");
		if ( 1 != this.queryFilters.size()) throw new IOException("Genric Filter has multi queries");
		return getName() + "/" + this.queryFilters.values().iterator().next();
	}


	/**
	 * @return The filter serialized using pb
	 * @throws IOException 
	 */
	@Override
	public byte [] toByteArray() throws IOException {

		HSearchGenericFilterMessage filterMessage = getGenericFilterMessage(this);

		if ( DEBUG_ENABLED ) {
			HSearchLog.l.debug("Sending to HBase : " + filterMessage.toString());
		}
		
		return filterMessage.toByteArray();		    
	}

	public static HSearchGenericFilterMessage getGenericFilterMessage(HSearchGenericFilter instance)
			throws IOException {
		
		HSearchGenericFilterMessage.Builder builder = HSearchGenericFilterMessage.newBuilder();
		builder.setFilterClassName(instance.getClass().getName());
		builder.setInputMapperInstructions(instance.inputMapperInstructions.toString());
		builder.setMultiQuery(instance.multiQuery);

		if ( null != instance.inputRowsList) {
			if ( instance.inputRowsList.size() > 0 ) {
				instance.inputRowsToIncludeB = SortedBytesArray.getInstanceArr().toBytes(instance.inputRowsList);
				builder.setInputRowsToIncludeB(ByteString.copyFrom(instance.inputRowsToIncludeB));
			}
		}

		if ( null != instance.queryFilters) {
			for (String key : instance.queryFilters.keySet()) {
				String value = instance.queryFilters.get(key);
				QueryFiltersPair.Builder queryBuilder = QueryFiltersPair.newBuilder()
																		.setKey(key)
																		.setValue(value);
				builder.addQueryFilters(queryBuilder);
			}
		}
		return builder.build();
	}

	/**
	 * @param pbBytes A pb serialized {@link PrefixFilter} instance
	 * @return An instance of {@link PrefixFilter} made from <code>bytes</code>
	 * @throws org.apache.hadoop.hbase.exceptions.DeserializationException
	 * @see #toByteArray
	 */
	public static HSearchGenericFilter parseFrom(final byte [] pbBytes)throws DeserializationException {
		HSearchGenericFilter genericFilter = null;
		try {

		    int length = null == pbBytes ? 0 : pbBytes.length;
			if(0 == length)
				throw new IOException("Invalid Query");

			if ( DEBUG_ENABLED) {
				HSearchLog.l.debug("Total bytes Received @ Generic Filter:" + length);
			}

			HSearchGenericFilterMessage filterMessage = HSearchGenericFilterMessage.parseFrom(pbBytes);
			genericFilter = getGenericFilter(filterMessage);
			
		} catch (Exception ex) {
			HSearchLog.l.fatal(ex);
		}

		return genericFilter;
	}

	public static HSearchGenericFilter getGenericFilter(HSearchGenericFilterMessage filterMessage)
			throws InstantiationException, IllegalAccessException,
			ClassNotFoundException, IOException, ParseException {
				
		HSearchGenericFilter genericFilter;
		String className = filterMessage.getFilterClassName();
		//TODO:Create factory for Generic filter instead of instantiating using reflection
		genericFilter = (HSearchGenericFilter) Class.forName(className).newInstance();

		if(filterMessage.hasInputRowsToIncludeB()){
			genericFilter.rowsToInclude = SortedBytesArray.getInstanceArr();
			genericFilter.rowsToInclude.parse(filterMessage.getInputRowsToIncludeB().toByteArray());
		}
		
		genericFilter.multiQuery = filterMessage.getMultiQuery();
		if ( DEBUG_ENABLED ) {
			HSearchLog.l.debug("HBase Region Server: Multi Query" +  genericFilter.multiQuery);
		}
		
		String instruction = filterMessage.getInputMapperInstructions();
		if (instruction.length() == 0  ) throw new IOException("Unknown result output type.");
		
		genericFilter.inputMapperInstructions = new HSearchProcessingInstruction(instruction);
		
		String colNameQuolonId = null;
		String filtersPipeSeparated = null;
		int colNameAndQIdSplitIndex = -1;
		String colName = null;
		String qId = null;
				
		List<QueryFiltersPair> filterPairs = filterMessage.getQueryFiltersList();
		for (QueryFiltersPair filterPair : filterPairs) {

			colNameQuolonId = filterPair.getKey();
			filtersPipeSeparated =  filterPair.getValue();

			colNameAndQIdSplitIndex = colNameQuolonId.indexOf(':');
			if ( -1 == colNameAndQIdSplitIndex || colNameQuolonId.length() - 1 == colNameAndQIdSplitIndex) {
				throw new IOException("Sub queries expected as  X:Y eg.\n" + 
						"family1:A OR family2:B\nfamily1:A=f|1|1|1|c|*|*\nfamily2:B=*|*|*|*|*|*");
			}
			
			colName = colNameQuolonId.substring(0,colNameAndQIdSplitIndex);
			qId =  colNameQuolonId.substring(colNameAndQIdSplitIndex+1);

			genericFilter.colIdWithType.put(qId, colName);

			if ( DEBUG_ENABLED ) {
				HSearchLog.l.debug("colName:qId = " + colName + ":" + qId);
			}
			
			genericFilter.plugin = genericFilter.createPlugIn(colName) ;
			genericFilter.plugin.setOutputType(genericFilter.inputMapperInstructions);
			genericFilter.queryPayload.put(colNameQuolonId, new QueryPart(filtersPipeSeparated,HSearchTableMultiQueryExecutor.PLUGIN, genericFilter.plugin) );

			if ( DEBUG_ENABLED ) {
				HSearchLog.l.debug("HBase Region Server: Query Payload added for " +  colName);
			}
		}

		for (int i = 0 ; i < genericFilter.queryPayload.size() ; i++) {
			genericFilter.stmtOutputContainers.add( new ArrayList<byte[]>() );
		}
		return genericFilter;
	}

	@Override
	public final  void filterRowCells(final List<Cell> cellL) {
		if ( null == cellL) return;
		int cellT = cellL.size();
		if ( 0 == cellT) return;

		if ( DEBUG_ENABLED ) {
			HSearchLog.l.debug("Processing @ Region Server : filterRow" );
		}

		try {

			byte[] row = null;
			byte[] firstFamily = null;
			byte[] firstCol = null;

			//colParts.put("structured:A", bytes);
			colNamesWithPartitionBytes.clear();

			//HBase Family Name = schema column name + "_" + partition
			String columnNameWithParition = null;
			String colName = null;

			for (Cell cell : cellL) {
				if ( null == cell) continue;

				byte[] inputData = CellUtil.cloneValue(cell);
				if ( null == inputData) continue;
				columnNameWithParition = new String(CellUtil.cloneFamily(cell));

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
					firstFamily = CellUtil.cloneFamily(cell);
					firstCol = CellUtil.cloneQualifier(cell);
					row = CellUtil.cloneRow(cell);
				}
			}

			if ( DEBUG_ENABLED ) {
				HSearchLog.l.debug("queryData HSearchTableParts creation. ");
			}

			queryIdWithParts.clear();

			for (String queryId : colIdWithType.keySet()) { //A
				String queryType = colIdWithType.get(queryId); //structured
				HSearchTableParts parts = colNamesWithPartitionBytes.get(queryType);

				String queryTypeWithId = queryType + ":" + queryId;

				if ( DEBUG_ENABLED ) {
					HSearchLog.l.debug(queryTypeWithId);
					HSearchLog.l.debug("Query Parts for " + queryTypeWithId);
				}

				queryIdWithParts.put(queryTypeWithId, parts);
			}
			colNamesWithPartitionBytes.clear();

			if ( DEBUG_ENABLED ) HSearchLog.l.debug("HSearchGenericFilter: Filteration Starts");

			long monitorStartTime = 0L; 
			if ( INFO_ENABLED ) {
				monitorStartTime = System.currentTimeMillis();
			}	

			if ( null == intersector ) intersector = createExecutor();
			this.plugin.setMergeId(row);
			BitSetOrSet intersectedIds = federatedQueryExec(row, intersector, queryIdWithParts);

			if ( INFO_ENABLED ) {
				this.pluginExecutionTime += System.currentTimeMillis() - monitorStartTime;
			}

			cellL.clear(); //Clear all data
			byte[] value = getOneRowBytes(intersectedIds, this.queryPayload);
			cellL.add(new KeyValue(row, firstFamily, firstCol, value) );


		} catch (Exception ex) {
			ex.printStackTrace(System.err);
			HSearchLog.l.fatal(ex);
		}
	}

	private final  BitSetOrSet federatedQueryExec(final byte[] row,
			final HSearchTableMultiQueryExecutor intersector,
			final Map<String, HSearchTableParts> queryData) throws Exception, IOException {

		BitSetOrSet intersectedIds = intersector.execute(
				queryData, this.multiQuery, this.queryPayload, inputMapperInstructions);

		if ( DEBUG_ENABLED ) {
			boolean hasMatchingIds = false;
			hasMatchingIds = ( null != intersectedIds && intersectedIds.size() > 0 );
			HSearchLog.l.debug("Generaic filter hasMatchingIds :" + hasMatchingIds + " objectid=" + intersectedIds.hashCode());
			if ( hasMatchingIds ) HSearchLog.l.debug( new String(row) + " has ids of :" + intersectedIds.size());
		}

		return intersectedIds;
	}

	@Override
	public final boolean hasFilterRow() {
		return true;
	}	

	@Override
	public final boolean filterRowKey(final byte[] rowKey, final int offset, final int length) {

		if (DEBUG_ENABLED) {
			int scopeToTheseRowsT = ( null == rowsToInclude) ? 0 : rowsToInclude.getSize();
			HSearchLog.l.debug("Analyzing row for processing: " + new String(rowKey + " , From a matching set of " + scopeToTheseRowsT));
		}

		if ( null == rowsToInclude) return false;

		byte[] exactRowBytes = new byte[length];
		try {
			System.arraycopy(rowKey, offset, exactRowBytes, 0, length);
			if ( rowsToInclude.getEqualToIndex(exactRowBytes) == -1) return true;
			return false;

		} catch (IOException ex) {
			int scopeToTheseRowsT = ( null == rowsToInclude) ? 0 : rowsToInclude.getSize();
			String rowKeyStr = ( null == rowKey) ? "Null row key" : new String(rowKey);
			String errMsg = "Error while finding fileration criteria for the row , " + rowKeyStr 
					+ "\n" + ex.getMessage() + "\n" + 
					"With search scope inside id count : " + scopeToTheseRowsT;
			System.err.println(errMsg);
			HSearchLog.l.fatal(errMsg, ex);

			return false;
		}
	}

	/**
	 *******************************************************************************************
	 * COMPUTATIONS
	 * Step 1 - HSearch Table merge 
	 *******************************************************************************************
	 */

	/**
	 * *|*|architect|age
	 * AND
	 * *|*|developer|age
	 * 
	 * @param matchedIds
	 * @param queryPayload
	 * @param inputMapperInstructions
	 * @return
	 * @throws IOException
	 */
	public final byte[] getOneRowBytes( final BitSetOrSet matchedIds, final Map<String, QueryPart> queryPayload) throws IOException {

		if ( DEBUG_ENABLED ) {
			int matchedIdsT = ( null == matchedIds) ? 0 : matchedIds.size();
			HSearchLog.l.debug("HSearchGenericFilter:serialize : with matchedIds " +  matchedIdsT + ", Object:" + matchedIds.hashCode());
			if ( null != matchedIds.getDocumentIds()) {
				HSearchLog.l.debug("HSearchGenericFilter: DocumentIds size " +  matchedIds.getDocumentIds().size() + " and matchedId size " + matchedIds.size());
			} else if ( null != matchedIds.getDocumentSequences()) {
				HSearchLog.l.debug("HSearchGenericFilter: DocumentSequences cardinality " +  matchedIds.getDocumentSequences().cardinality());
			}
		}

		/**
		 * - Iterate through all the parts and find the values.
		 * - Collect the data for multiple queries
		 */
		HSearchReducer reducer = getReducer();
		int totalQueries = queryPayload.size();

		columnsOfOneRowAfterJoin.clear();
		long monitorStartTime = 0L; 

		if ( totalQueries == 1) {
			
			if(DEBUG_ENABLED)
				HSearchLog.l.debug("HSearchGenericFilter: processing single query.");
			
			Object pluginO = queryPayload.values().iterator().next().getParams().get(
					HSearchTableMultiQueryExecutor.PLUGIN);
			IHSearchPlugin plugin = (IHSearchPlugin) pluginO;

			if ( INFO_ENABLED ) {
				monitorStartTime = System.currentTimeMillis();
			}

			plugin.getResultSingleQuery(columnsOfOneRowAfterJoin);

			if ( INFO_ENABLED ) {
				this.pluginExecutionTime += System.currentTimeMillis() - monitorStartTime;
			}


		} else {

			if(DEBUG_ENABLED)
				HSearchLog.l.debug("HSearchGenericFilter: processing multiple query.");

			StatementWithOutput[] stmtWithOutputs = new StatementWithOutput[totalQueries];
			int seq = 0;

			for (QueryPart part : queryPayload.values()) {

				Object pluginO = part.getParams().get(HSearchTableMultiQueryExecutor.PLUGIN);
				IHSearchPlugin plugin = (IHSearchPlugin) pluginO;

				if ( INFO_ENABLED ) {
					monitorStartTime = System.currentTimeMillis();
				}	

				Collection<byte[]> queryOutput = this.stmtOutputContainers.get(seq);
				queryOutput.clear(); //Clear to reuse
				
				if(DEBUG_ENABLED)
					HSearchLog.l.debug("HSearchGenericFilter: Calling getResultMultiQuery for " + part.aStmtOrValue);

				plugin.getResultMultiQuery(matchedIds, queryOutput);

				if ( INFO_ENABLED ) {
					this.pluginExecutionTime += System.currentTimeMillis() - monitorStartTime;
				}

				stmtWithOutputs[seq] = new StatementWithOutput(part.aStmtOrValue, queryOutput);
				seq++;
			}

			if ( INFO_ENABLED ) {
				monitorStartTime = System.currentTimeMillis();
			}	
			
			if(DEBUG_ENABLED)
				HSearchLog.l.debug("HSearchGenericFilter: Calling reducer.appendQueries ");

			reducer.appendQueries(columnsOfOneRowAfterJoin, stmtWithOutputs);

			if ( INFO_ENABLED ) {
				this.pluginExecutionTime += System.currentTimeMillis() - monitorStartTime;
			}

			for (StatementWithOutput stmtWithOutput : stmtWithOutputs) {
				if ( null != stmtWithOutput.cells ) stmtWithOutput.cells.clear();
			}
		}

		//Put it to Bytes
		byte[] processedRowBytes = rowBytesPacker.toBytes(columnsOfOneRowAfterJoin);
		columnsOfOneRowAfterJoin.clear();

		return processedRowBytes;
	}

	public final void deserialize(final byte[] input, final Collection<byte[]> output) throws IOException {
		SortedBytesArray.getInstance().parse(input).values(output);
	}

	public abstract HSearchTableMultiQueryExecutor createExecutor();
	public abstract IHSearchPlugin createPlugIn(String type) throws IOException ;
	public abstract HSearchReducer getReducer();

	/**
	 * Override this method if you want to set more filters in processing.
	 * 
	 	FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL);
		RowFilter filter1 = new RowFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("row-22")) );
		list.addFilter(filter1);
		list.addFilter(this);
		return list;

	 * @return
	 */
	public final FilterList getFilters() {
		return null;
	}

	/**
	 * Any information to be configured before starting the filtration process.
	 */
	public final void configure() {
	}

	/**
	 * At the end release the resources.
	 */
	public final void close() {
		if ( null != queryFilters)  queryFilters.clear();
		if ( null != queryPayload)  queryPayload.clear();
		if ( null != colIdWithType)  colIdWithType.clear();
		if ( null != queryIdWithParts)  queryIdWithParts.clear();
		if ( null != colNamesWithPartitionBytes)  colNamesWithPartitionBytes.clear();
		if ( null != columnsOfOneRowAfterJoin)  columnsOfOneRowAfterJoin.clear();
		if ( null != stmtOutputContainers)  stmtOutputContainers.clear();
	}


}
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
import java.util.Collection;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;

import com.bizosys.hsearch.byteutils.SortedBytesArray;
import com.bizosys.hsearch.byteutils.SortedBytesBase.Reference;
import com.bizosys.hsearch.treetable.client.HSearchProcessingInstruction;
import com.bizosys.hsearch.treetable.client.HSearchQuery;
import com.bizosys.hsearch.treetable.client.IHSearchPlugin;
import com.bizosys.hsearch.treetable.client.IHSearchTable;
import com.bizosys.hsearch.util.HSearchLog;

/**
 * @author abinash
 *
 */
public abstract class HSearchScalarFilter implements Filter {
	
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
	 * Input Variables
	 */
	String multiQuery = null;
	
	public long pluginExecutionTime = 0L;
	public long overallExecutionTime = 0L;
	
	
	protected HSearchProcessingInstruction inputMapperInstructions = new HSearchProcessingInstruction();
	byte[] inputRowsToIncludeB = null;
	List<byte[]>  inputRowsList = null;
	SortedBytesArray rowsToInclude = null;
	byte[] matchingIds = null;

	public HSearchScalarFilter(){
	}

	public HSearchScalarFilter(final HSearchProcessingInstruction outputType,final String query) {
		this.multiQuery = query;
		this.inputMapperInstructions = outputType;
	}
	
	public void setMatchingRows(List<byte[]> inputRowsList) {
		this.inputRowsList = inputRowsList;
	}
	
	public void setMatchingIds(byte[] matchingIds) {
		this.matchingIds = matchingIds;
	}

	@Override
	public final void write(final DataOutput out) throws IOException {

		if ( null != inputRowsList) {
			if ( inputRowsList.size() > 0 ) {
				inputRowsToIncludeB = SortedBytesArray.getInstanceArr().toBytes(inputRowsList);
			}
		}
		
		SortedBytesArray sendToRSData = SortedBytesArray.getInstanceArr();
		String querySection = this.inputMapperInstructions.toString()  + "\n" + this.multiQuery;
		
		List<byte[]> values = new ArrayList<byte[]>(3);
		values.add(querySection.getBytes());
		
		if(null != matchingIds)
			values.add(matchingIds);
		else
			values.add(new byte[0]);
		
		if(null != inputRowsToIncludeB)
			values.add(inputRowsToIncludeB);
		
		byte[] ser = sendToRSData.toBytes( values );		

		out.writeInt(ser.length);
		out.write(ser);
	}	

	HSearchQuery query = null; 
	IHSearchTable table = null;
	IHSearchPlugin plugin = null;
	boolean skipFiltering = true;
	Collection<byte[]> dataCarrier = new ArrayList<byte[]>();
			
	@Override
	public final void readFields(final DataInput in) throws IOException {
		try {
			int length = in.readInt();
			if ( 0 == length) throw new IOException("Invalid Query");
			
			byte[] deser = new byte[length];
			in.readFully(deser, 0, length);
			
			if ( DEBUG_ENABLED) {
				HSearchLog.l.debug("Total bytes Received @ Generic Filter:" + length);
			}
			
			SortedBytesArray receiveRSData = SortedBytesArray.getInstanceArr();
			receiveRSData.parse(deser);

			int packedDataSectionsT = receiveRSData.getSize();
			if ( DEBUG_ENABLED) {
				HSearchLog.l.debug("Reading bytes sections of total :" + packedDataSectionsT);
			}
			if ( packedDataSectionsT == 0 ) {
				throw new IOException("Unknown number of fields :" + packedDataSectionsT);
			}
			
			Reference ref = new Reference();
			//Filter Row Section
			if ( packedDataSectionsT == 3) {
				receiveRSData.getValueAtReference(2, ref);
				rowsToInclude = SortedBytesArray.getInstanceArr();
				rowsToInclude.parse(deser, ref.offset, ref.length);
				System.out.println("Total Rows :" + rowsToInclude.values().size());
			}
			
			//matching ids
			receiveRSData.getValueAtReference(1, ref);
			this.matchingIds = new byte[ref.length];
			System.arraycopy(deser, ref.offset, this.matchingIds, 0, ref.length);
			
			//Query Section
			receiveRSData.getValueAtReference(0, ref);
			StringTokenizer stk = new StringTokenizer(new String(deser, ref.offset, ref.length), "\n");
			
			int lineNo = -1;
			while ( stk.hasMoreTokens() ) {
				
				lineNo++;
				
				switch ( lineNo ) {
					case 0:
						String output = stk.nextToken();
						if (output.length() == 0  ) throw new IOException("Unknown result output type.");
						this.inputMapperInstructions = new HSearchProcessingInstruction(output);
						break;
						
					case 1:
						this.multiQuery = stk.nextToken();

						if ( DEBUG_ENABLED ) {
							HSearchLog.l.debug("HBase Region Server: Multi Query" +  this.multiQuery);
						}
						break;
				}
			}			
			
			if ( null != this.multiQuery ) {
				if ( 0 != this.multiQuery.trim().length() ) 
					query = new HSearchQuery(this.multiQuery);

				this.table = createTable();
				if ( null != table) {
					this.plugin =createPlugIn();
					if ( null != this.plugin) {
						this.plugin.setOutputType(this.inputMapperInstructions);
						if(0 != this.matchingIds.length)
							this.plugin.setMergeId(this.matchingIds);
						skipFiltering = false;
					}
				}
			}			
			
		} catch (Exception ex) {
			HSearchLog.l.fatal(ex);
			ex.printStackTrace();
			throw new IOException(ex);
		}
	}
	
	@Override
	public final  void filterRow(final List<KeyValue> kvL) {
		if ( skipFiltering ) return;
		
		if ( null == kvL) return;
		int kvT = kvL.size();
		if ( 0 == kvT) return;
		
		if ( DEBUG_ENABLED ) {
			HSearchLog.l.debug("Processing @ Region Server : filterRow" );
		}
		
		try {
			
			List<KeyValue> kvLFiltered = new ArrayList<KeyValue>();
			
			for (KeyValue kv : kvL) {
				if ( null == kv) continue;

				byte[] inputData = kv.getValue();
				if ( null == inputData) continue;
				
				switch ( this.inputMapperInstructions.getCallbackType()) {
					case HSearchProcessingInstruction.PLUGIN_CALLBACK_COLS:
						table.get(inputData, this.query, plugin);
						break;
					case HSearchProcessingInstruction.PLUGIN_CALLBACK_ID:
						table.keySet(inputData, this.query, plugin);
						break;
					case HSearchProcessingInstruction.PLUGIN_CALLBACK_VAL:
						table.values(inputData, this.query, plugin);
						break;
					case HSearchProcessingInstruction.PLUGIN_CALLBACK_IDVAL:
						table.keyValues(inputData, this.query, plugin);
						break;
					default:
						throw new IOException("Unknown output type:" + this.inputMapperInstructions.getCallbackType());
				}
				
				plugin.getResultSingleQuery(dataCarrier);
				
				kvLFiltered.add(new KeyValue(kv.getKey(), kv.getFamily(), kv.getQualifier(), 
					SortedBytesArray.getInstance().toBytes(dataCarrier)) );
				dataCarrier.clear();
			}
			kvL.clear();
			kvL.addAll(kvLFiltered);
			
			if ( DEBUG_ENABLED ) {
				HSearchLog.l.debug("queryData HSearchTableParts creation. ");
			}
			
			
			
		} catch (Exception ex) {
			ex.printStackTrace(System.err);
			HSearchLog.l.fatal(ex);
		}
	}

	@Override
	public final void reset() {
	}	
	
	@Override
	public final boolean hasFilterRow() {
		return true;
	}	
	
	@Override
	public final KeyValue getNextKeyHint(final KeyValue arg0) {
		return null;
	}	
	
	@Override
	public final boolean filterRowKey(final byte[] rowKey, final int offset, final int length) {
		
		if ( null == rowsToInclude) return false;
		byte[] exactRowBytes = new byte[length];
		try {
			System.arraycopy(rowKey, offset, exactRowBytes, 0, length);
			if ( rowsToInclude.getEqualToIndex(exactRowBytes) >= 0 ) {
				//System.out.println("Allow row:" + new String(exactRowBytes));
				return false;
			} else {
				//System.out.println("Disallow row:" + new String(exactRowBytes));
				return true;
			}
			
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
	
	@Override
	public final boolean filterAllRemaining() {
		return false;
	}
	
	@Override
	public final boolean filterRow() {
		return false;
	}
	
	@Override
	public final ReturnCode filterKeyValue(final KeyValue arg0) {
		return ReturnCode.INCLUDE;
	}	
	
	/**
	 * Version 0.94 FIX
	 */
	@Override
	public final KeyValue transform(final KeyValue arg0) {
		return arg0;
	}
	

	public final void deserialize(final byte[] input, final Collection<byte[]> output) throws IOException {
		SortedBytesArray.getInstance().parse(input).values(output);
	}
	
	public abstract IHSearchPlugin createPlugIn() throws IOException ;
	public abstract IHSearchTable createTable();
	
	
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
	}
}
	
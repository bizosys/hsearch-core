package com.bizosys.hsearch.kv;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;

import com.bizosys.hsearch.byteutils.SortedBytesArray;
import com.bizosys.hsearch.byteutils.SortedBytesInteger;
import com.bizosys.hsearch.federate.BitSetOrSet;
import com.bizosys.hsearch.federate.FederatedSearch;
import com.bizosys.hsearch.federate.QueryPart;
import com.bizosys.hsearch.functions.GroupSortedObject;
import com.bizosys.hsearch.functions.GroupSortedObject.FieldType;
import com.bizosys.hsearch.functions.GroupSorter;
import com.bizosys.hsearch.functions.GroupSorter.GroupSorterSequencer;
import com.bizosys.hsearch.hbase.HReader;
import com.bizosys.hsearch.kv.impl.ComputeKV;
import com.bizosys.hsearch.kv.impl.FieldMapping;
import com.bizosys.hsearch.kv.impl.IEnricher;
import com.bizosys.hsearch.kv.impl.KVDataSchemaRepository;
import com.bizosys.hsearch.kv.impl.KVDataSchemaRepository.KVDataSchema;
import com.bizosys.hsearch.kv.impl.KVRowI;
import com.bizosys.hsearch.treetable.client.HSearchProcessingInstruction;
import com.bizosys.hsearch.util.HSearchLog;

public class Searcher {

	public String dataRepository = "kv-store";
	List<KVRowI> resultset = new ArrayList<KVRowI>();
	String schemaRepositoryName = "xmlFields";
	KVDataSchemaRepository repository = KVDataSchemaRepository.getInstance();
	
	public static boolean DEBUG_ENABLED = HSearchLog.l.isDebugEnabled();
	public static boolean INFO_ENABLED = HSearchLog.l.isInfoEnabled();
	
	
	private Searcher(){
	}
	
	public Searcher(final FieldMapping fm){
		repository.add(schemaRepositoryName, fm);
	}
	
	public void searchRegex(final String dataRepository,
			final String mergeIdPattern, String selectQuery, String whereQuery,
			KVRowI blankRow, IEnricher... enrichers) throws IOException  {

		List<String> rowIds = HReader.getMatchingRowIds(dataRepository, mergeIdPattern);
		Set<String> mergeIds = new HashSet<String>();
		
		for (String mergeIdWithFieldId : rowIds) {
			if ( DEBUG_ENABLED ) HSearchLog.l.debug("Analyzing rowId :" + mergeIdWithFieldId);
			int lastIndex = mergeIdWithFieldId.lastIndexOf('_');
			mergeIds.add(  mergeIdWithFieldId.substring(0, lastIndex) );
		}
		
		for (String mergeId : mergeIds) {
			if ( DEBUG_ENABLED ) HSearchLog.l.debug("Searching in mergeId :" + mergeId);
			search(dataRepository, mergeId, selectQuery, whereQuery, blankRow, enrichers);
		}
		
	}
	
	/**
	 * Select : mergeid/field,mergeid/field,mergeid/field
	 * Where  : (mergeid/field = 34 AND mergeid/field = 39) OR (mergeid/field = [2-34])
	 * 
	 * @param compositeQuery
	 */
	public void searchAcross() {
	}
		
	@SuppressWarnings("unchecked")
	public void search(final String dataRepository, 
			final String mergeId, String selectQuery, String whereQuery, 
			KVRowI blankRow, IEnricher... enrichers){

		StringBuilder foundIds = null;

		boolean isEmpty = ( null == whereQuery) ? true : (whereQuery.length() == 0);
		if(!isEmpty){
			String skeletonQuery = whereQuery.replaceAll("\\s+", " ").replaceAll("\\(", "").replaceAll("\\)", "");
			
			String[] splittedQueries = skeletonQuery.split("( AND | OR | NOT )");
			int index = -1;
			int colonIndex = -1;
			int totalQueries = 0;
			String fieldName = "";
			String fieldText = "";
			Map<String, QueryPart> queryDetails = new HashMap<String, QueryPart>();
			
			for (String splittedQuery : splittedQueries) {
				splittedQuery = splittedQuery.trim();
				index = whereQuery.indexOf(splittedQuery);
				String queryId = "q" + totalQueries++; 
				whereQuery = whereQuery.substring(0, index) + queryId +  whereQuery.substring(index + splittedQuery.length());
				colonIndex = splittedQuery.indexOf(':');
				fieldName = splittedQuery.substring(0,colonIndex);
				fieldText = splittedQuery.substring(colonIndex + 1,splittedQuery.length());
				QueryPart qpart = new QueryPart(mergeId + "_" + fieldName);
				qpart.setParam("query", "*|" + fieldText);
				queryDetails.put(queryId, qpart);
			}
			
			this.dataRepository = dataRepository;
			FederatedSearch ff = createFederatedSearch();

			//get ids first
			BitSetOrSet mixedQueryMatchedIds = null;
			try {
				mixedQueryMatchedIds = ff.execute(whereQuery, queryDetails);
			} catch (Exception e) {
				System.err.println("Error in Searcher: could not execute " + e.getMessage());
			}
			
			if(null == mixedQueryMatchedIds) return;
			if ( null == mixedQueryMatchedIds.getDocumentIds()) return;
			if ( DEBUG_ENABLED ) {
				HSearchLog.l.debug("Matching ids " + mixedQueryMatchedIds.getDocumentIds().toString());
			}
			
			for (Object matchedId : mixedQueryMatchedIds.getDocumentIds()) {
				if ( null == foundIds) {
					foundIds = new StringBuilder("{");
					foundIds.append(matchedId.toString());
				} else {
					foundIds.append(',').append(matchedId.toString());
				}
			}
			if(null != foundIds)
				foundIds.append('}');
			else 
				foundIds = new StringBuilder("*");
		}
		else{
			foundIds = new StringBuilder("*");
		}
		
		//get all the values based on ids
		String[] selectFields = selectQuery.split(",");
		String filterQuery = null;
		String rowId = null;
		Map<String, Object> individualResults = new HashMap<String, Object>();
		
		for (String field : selectFields) {
			filterQuery = foundIds.toString() + "|*";
			rowId = mergeId + "_" + field;
			Map<Integer, Object> readingIdWithValue = (Map<Integer, Object>) readStorage(dataRepository, rowId, filterQuery, HSearchProcessingInstruction.PLUGIN_CALLBACK_COLS);
			individualResults.put(field, readingIdWithValue);
		}
		
		Map<Integer,KVRowI> mergedResult = new HashMap<Integer, KVRowI>();
		for (String field : individualResults.keySet()) {
			Map<Integer, Object> res = (Map<Integer, Object>) individualResults.get(field);
			for (Integer id : res.keySet()) {
				if (mergedResult.containsKey(id)){
					KVRowI aRow = mergedResult.get(id);
					aRow.setValue(field, res.get(id));
				}
				else {
					KVRowI aRow = blankRow.create(repository.get(schemaRepositoryName));
					aRow.setValue(field, res.get(id));
					aRow.setId((Integer)id);
					mergedResult.put((Integer)id, aRow);
				}
			}
		}

		resultset.addAll(mergedResult.values());
		
		if ( null != enrichers) {
			for (IEnricher enricher : enrichers) {
				if ( null != enricher) enricher.enrich(this.resultset);
			}
		}
	}

	public List<KVRowI> sort (String... sorters) {
	
		GroupSorterSequencer[] sortSequencer = new GroupSorterSequencer[sorters.length];

		int index = 0;
		int fieldSeq = 0;
		FieldType fldType = null;
		boolean sortType = false;

		KVDataSchema dataSchema = repository.get(schemaRepositoryName); 
		
		for (String sorterName : sorters) {
			char firstChar = sorterName.charAt(0);
			if('^' == firstChar){
				sortType = true;
				sorterName = sorterName.substring(1);
			}
			else{
				sortType = false;				
			}
			fieldSeq = dataSchema.nameToSeqMapping.get(sorterName);
			fldType = dataSchema.dataTypeMapping.get(sorterName);
			GroupSorterSequencer seq = new GroupSorterSequencer(fldType,fieldSeq,index,sortType);
			
			sortSequencer[index++] = seq;
		}
		GroupSorter gs = new GroupSorter();
		
		for (GroupSorterSequencer seq : sortSequencer) {
			gs.setSorter(seq);
		}
		
		GroupSortedObject[] sortedContainer = new GroupSortedObject[resultset.size()];
		resultset.toArray(sortedContainer);
		
		gs.sort(sortedContainer);

		ListIterator<KVRowI> i = resultset.listIterator();
		for (int j=0; j<sortedContainer.length; j++) {
		    i.next();
		    i.set((KVRowI)sortedContainer[j]);
		}

		return this.resultset;
	}
	
	public List<KVRowI> getResult() {
		return this.resultset;
	}
	
	public void clear() {
		resultset.clear();
	}
	
	private FederatedSearch createFederatedSearch() {
		
		FederatedSearch ff = new FederatedSearch(2) {

			@SuppressWarnings("unchecked")
			@Override
			public BitSetOrSet populate(String type, String queryId,
					String rowId, Map<String, Object> filterValues) {
				
				String filterQuery = filterValues.values().iterator().next().toString();
				Set<Integer> readingIds = (Set<Integer>) new Searcher().readStorage(dataRepository, rowId, filterQuery, HSearchProcessingInstruction.PLUGIN_CALLBACK_ID);
				BitSetOrSet rows = new BitSetOrSet();
				rows.setDocumentIds(readingIds);
				return rows;
			}

		};
		return ff;
	}
	
	public Object readStorage(final String tableName, final String rowId, final String filter, final int callBackType) {
		
		byte[] data = null;
		try {
			
			String fieldName = rowId.substring(rowId.lastIndexOf("_") + 1,rowId.length());
			KVDataSchema dataScheme = repository.get(schemaRepositoryName);
			int outputType = dataScheme.dataTypeMapping.get(fieldName).ordinal();
			
			if(callBackType == HSearchProcessingInstruction.PLUGIN_CALLBACK_COLS){
				ComputeKV compute = null;	
				compute = new ComputeKV();
				compute.kvType = outputType; 
				compute.rowContainer = new HashMap<Integer, Object>();

				data = KVRowReader.getAllValues(tableName, rowId.getBytes(), filter, callBackType, compute.kvType);
				compute.put(data);
				
				return compute.rowContainer;
				
			}else if(callBackType == HSearchProcessingInstruction.PLUGIN_CALLBACK_ID){
				data = KVRowReader.getAllValues(tableName, rowId.getBytes(), filter, callBackType, outputType);
				for (byte[] dataChunk : SortedBytesArray.getInstanceArr().parse(data).values()) {
					Set<Integer> ids = new HashSet<Integer>();
					SortedBytesInteger.getInstance().parse(dataChunk).values(ids);
					return ids;
				}
			}
			
		} catch (Exception e) {
			System.err.println("Exception " + e.getMessage());
		}
		
		return null;
	}
}
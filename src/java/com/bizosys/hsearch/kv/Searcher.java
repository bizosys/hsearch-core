package com.bizosys.hsearch.kv;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import org.apache.lucene.queryParser.standard.parser.ParseException;

import com.bizosys.hsearch.federate.BitSetOrSet;
import com.bizosys.hsearch.federate.FederatedSearch;
import com.bizosys.hsearch.federate.FederatedSearchException;
import com.bizosys.hsearch.federate.QueryPart;
import com.bizosys.hsearch.functions.GroupSortedObject;
import com.bizosys.hsearch.functions.GroupSortedObject.FieldType;
import com.bizosys.hsearch.functions.GroupSorter;
import com.bizosys.hsearch.functions.GroupSorter.GroupSorterSequencer;
import com.bizosys.hsearch.kv.impl.ComputeKV;
import com.bizosys.hsearch.kv.impl.IEnricher;
import com.bizosys.hsearch.kv.impl.KVDataSchemaRepository;
import com.bizosys.hsearch.kv.impl.KVRowI;
import com.bizosys.hsearch.kv.impl.KVDataSchemaRepository.KVDataSchema;

public class Searcher {

	public String dataRepository = "kv-store";
	List<KVRowI> resultset = new ArrayList<KVRowI>();
	String schemaRepositoryName = "xmlFields";
	KVDataSchemaRepository repository = KVDataSchemaRepository.getInstance();
	
	private Searcher(){
	}
	
	public Searcher(String schemaPath){
		repository.add(schemaRepositoryName, schemaPath);
	}
		
	@SuppressWarnings("unchecked")
	public void search(final String dataRepository, 
			final String mergeId, String query, 
			KVRowI blankRow, IEnricher... enrichers)
						throws FederatedSearchException, IOException, InterruptedException, ParseException {
		
		String skeletonQuery = query.replaceAll("\\s+", " ").replaceAll("\\(", "").replaceAll("\\)", "");
		
		String[] splittedQueries = skeletonQuery.split("(AND|OR|NOT)");
		int index = -1;
		int colonIndex = -1;
		int totalQueries = 0;
		String fieldName = "";
		String fieldText = "";
		Map<String, QueryPart> queryDetails = new HashMap<String, QueryPart>();
		
		for (String splittedQuery : splittedQueries) {
			splittedQuery = splittedQuery.trim();
			index = query.indexOf(splittedQuery);
			String queryId = "q" + totalQueries++; 
			query = query.substring(0, index) + queryId +  query.substring(index + splittedQuery.length());
			colonIndex = splittedQuery.indexOf(':');
			fieldName = splittedQuery.substring(0,colonIndex);
			fieldText = splittedQuery.substring(colonIndex + 1,splittedQuery.length());
			QueryPart qpart = new QueryPart(mergeId + "_" + fieldName);
			qpart.setParam("query", "*|" + fieldText);
			queryDetails.put(queryId, qpart);
		}
		
		this.dataRepository = dataRepository;
		FederatedSearch ff = createFederatedSearch();

		BitSetOrSet mixedQueryMatchedIds = ff.execute(query, queryDetails);
		
		System.out.println("matching ids " + mixedQueryMatchedIds.getDocumentIds().toString());
		
		if(mixedQueryMatchedIds.isEmpty())return;
		
		Map<Integer,KVRowI> mergedResult = new HashMap<Integer, KVRowI>();
		for (String partQueryVariable : queryDetails.keySet()) {

			QueryPart qp = queryDetails.get(partQueryVariable);
			String rowid = qp.aStmtOrValue;
			String field = rowid.substring(rowid.lastIndexOf("_") + 1,rowid.length());
			Map<Integer, Object> res = (Map<Integer, Object>) (qp.getParams().get("result"));
			
			for (Object matchedId : mixedQueryMatchedIds.getDocumentIds()) {
				if (res.containsKey(matchedId)){
					if (mergedResult.containsKey(matchedId)){
						KVRowI toxCol = mergedResult.get(matchedId);
						toxCol.setValue(field, res.get(matchedId));
					}
					else {
						KVRowI toxCol = blankRow.create(repository.get(schemaRepositoryName));
						toxCol.setValue(field, res.get(matchedId));
						toxCol.setId((Integer)matchedId);
						mergedResult.put((Integer)matchedId, toxCol);
					}
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

			@Override
			public BitSetOrSet populate(String type, String queryId,
					String rowId, Map<String, Object> filterValues) {
				
				Map<Integer, Object> readingIdWithValue = new Searcher().readStorage(
						dataRepository, rowId, filterValues.values().iterator().next().toString());
				
				BitSetOrSet rows = new BitSetOrSet();
				filterValues.put("result", readingIdWithValue);
				rows.setDocumentIds(readingIdWithValue.keySet());
				return rows;
			}

		};
		return ff;
	}
	
	public Map<Integer, Object> readStorage(String tableName, String rowId,String filter) {
		
		byte[] data = null;
		ComputeKV compute = null;
		try {
	
			String fieldName = rowId.substring(rowId.lastIndexOf("_") + 1,rowId.length());
			compute = new ComputeKV();
			
			KVDataSchema dataScheme = repository.get(schemaRepositoryName);
			compute.kvType = dataScheme.dataTypeMapping.get(fieldName).ordinal();
			compute.rowContainer = new HashMap<Integer, Object>();
	
			data = KVRowReader.getAllValues(tableName, rowId.getBytes(), filter, compute.kvType);
	
			compute.put(data);
			
		} catch (Exception e) {
			System.err.println("Exception " + e.getMessage());
		}
		
		return compute.rowContainer;
	}
}
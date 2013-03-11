package com.bizosys.hsearch.treetable.client.partition;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import com.bizosys.hsearch.treetable.client.HSearchQuery;

public interface IPartition<T> {
	void setPartitionsAndRange(String colName, String familyNames, String ranges, int partitionIndex) throws IOException;
	void getMatchingFamilies(HSearchQuery query, Set<String> uniqueFamilies) throws IOException;
	String getColumnFamily(T exactVal) throws IOException;
	void getColumnFamilies(T startVal, T endVal, Set<String> families) throws IOException;
	List<String> getPartitionNames();
}

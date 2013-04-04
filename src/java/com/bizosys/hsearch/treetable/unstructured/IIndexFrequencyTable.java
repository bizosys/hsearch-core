package com.bizosys.hsearch.treetable.unstructured;

import java.io.IOException;

import com.bizosys.hsearch.treetable.client.IHSearchTable;

public interface IIndexFrequencyTable extends IHSearchTable {

	void put(Integer docType, Integer fieldType, Integer hashCode, Integer docId, Integer frequencies);		
	byte[] toBytes() throws IOException;
	void clear() throws IOException;
}
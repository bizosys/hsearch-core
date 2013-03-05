package com.bizosys.hsearch.byteutils;

import java.io.IOException;
import java.util.Collection;

public interface ISortedByte<T> {
	
	byte[] toBytes(Collection<T> sortedCollection) throws IOException;
	
	
	ISortedByte<T> parse(byte[] bytes) throws IOException;
	ISortedByte<T> parse(byte[] bytes, int offset, int length) throws IOException;
	
	int getSize() throws IOException;

	void addAll(Collection<T> vals) throws IOException;
	Collection<T> values() throws IOException;
	Collection<T> values(Collection<T> container) throws IOException;
	T getValueAt(int pos) throws IOException;

	int getEqualToIndex(T matchNo) throws IOException;
	void getEqualToIndexes(T matchNo, Collection<Integer> matchings) throws IOException;
	Collection<Integer> getEqualToIndexes(T matchNo) throws IOException;
	
	void getGreaterThanIndexes(T matchingNo, Collection<Integer> matchingPos) throws IOException;
	Collection<Integer> getGreaterThanIndexes(T matchingNo) throws IOException;
	void getGreaterThanEqualToIndexes(T matchingNo, Collection<Integer> matchingPos) throws IOException;
	Collection<Integer> getGreaterThanEqualToIndexes(T matchingNo) throws IOException;
	
	void getLessThanIndexes(T matchingNo, Collection<Integer> matchingPos ) throws IOException;
	void getLessThanEqualToIndexes(T matchingNo, Collection<Integer> matchingPos) throws IOException;
	Collection<Integer> getLessThanIndexes(T matchingNo) throws IOException;
	Collection<Integer> getLessThanEqualToIndexes(T matchingNo) throws IOException;

	void getRangeIndexes(T matchNoStart, T matchNoEnd, Collection<Integer> matchings) throws IOException;
	Collection<Integer> getRangeIndexes(T matchNoStart, T matchNoEnd) throws IOException;
	
	void getRangeIndexesInclusive(T matchNoStart, T matchNoEnd, Collection<Integer> matchings) throws IOException;
	Collection<Integer> getRangeIndexesInclusive(T matchNoStart, T matchNoEnd) throws IOException;
	
	void getRangeIndexesInclusive(T matchNoStart, boolean startMatch, T matchNoEnd, boolean endMatch, Collection<Integer> matchings) throws IOException;
	Collection<Integer> getRangeIndexesInclusive(T matchNoStart, boolean startMatch, T matchNoEnd, boolean endMatch) throws IOException;
	
}

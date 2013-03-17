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
	T getValueAt(int pos) throws IndexOutOfBoundsException;

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

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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public final class SortedBytesBoolean extends SortedBytesBase<Boolean>{

	public static final ISortedByte<Boolean> getInstance() {
		return new SortedBytesBoolean();
	}
	
	List<Boolean> parsedBooleans = null;
	private SortedBytesBoolean() {
		this.dataSize = -1;
	}

	@Override
	public final byte[] toBytes(final Collection<Boolean> sortedCollection) throws IOException {
		int available = sortedCollection.size();
		int packed = available/8;
		int remaining = available - packed * 8;
		
		int neededBytes = packed;
		if ( remaining > 0 ) neededBytes++;
		
		neededBytes = neededBytes + 4; // How many
		
		byte[] out = new byte[neededBytes];
		
		
		System.arraycopy(Storable.putInt(available), 0, out, 0, 4);
		
		Iterator<Boolean> itr = sortedCollection.iterator();
		for ( int i=0; i<packed; i++) {
			out[4+i]  = ByteUtil.fromBits(new boolean[] {
					itr.next(), itr.next(), itr.next(), itr.next(),
					itr.next(), itr.next(), itr.next(), itr.next()});
		}
		
		if ( remaining > 0 ) {
			boolean[] remainingBits = new boolean[8];
			Arrays.fill(remainingBits, true);
			for ( int j=0; j<remaining; j++) {
				remainingBits[j] = itr.next();
			}
				
			out[4+packed] = ByteUtil.fromBits(remainingBits);
		}
		return out;
	}

	@Override
	public final int getSize() throws IOException {
		if ( null == this.inputBytes ) return 0;
		return Storable.getInt(this.offset, this.inputBytes);
		
	}

	@Override
	public final void addAll(final Collection<Boolean> vals) throws IOException {
		if ( null != this.parsedBooleans ) parse();
		if ( null == this.parsedBooleans ) this.parsedBooleans = new ArrayList<Boolean>(); 
		this.parsedBooleans.addAll(vals);
	}

	@Override
	public final Boolean getValueAt(final int pos) throws IndexOutOfBoundsException {
		initialize();
		return this.parsedBooleans.get(pos);
	}

	@Override
	public final int getEqualToIndex(final Boolean matchNo) throws IOException {
		initialize();

		boolean matchNoL = matchNo.booleanValue();
		int pos = -1;
		for (boolean entity : this.parsedBooleans) {
			pos++;
			if ( matchNoL == entity) return pos;
		}
		return -1;
	}

	@Override
	public final void getEqualToIndexes(final Boolean matchNo, final Collection<Integer> matchings) throws IOException {

		initialize();

		boolean matchNoL = matchNo.booleanValue();
		int pos = -1;
		for (boolean entity : this.parsedBooleans) {
			pos++;
			if ( matchNoL == entity) matchings.add(pos);
		}
	}

	@Override
	public final void getNotEqualToIndexes(final Boolean matchNo, final Collection<Integer> matchings) throws IOException {

		initialize();

		boolean matchNoL = matchNo.booleanValue();
		int pos = -1;
		for (boolean entity : this.parsedBooleans) {
			pos++;
			if ( matchNoL != entity) matchings.add(pos);
		}
	}

	@Override
	public void getGreaterThanIndexes(Boolean matchingNo, Collection<Integer> matchingPos) throws IOException {
		throw new RuntimeException("Not implemented");
	}

	@Override
	public void getGreaterThanEqualToIndexes(Boolean matchingNo, Collection<Integer> matchingPos) throws IOException {
		throw new RuntimeException("Not implemented");
	}

	@Override
	public void getLessThanIndexes(Boolean matchingNo, Collection<Integer> matchingPos) throws IOException {
		throw new RuntimeException("Not implemented");
	}

	@Override
	public void getLessThanEqualToIndexes(Boolean matchingNo, Collection<Integer> matchingPos) throws IOException {
		throw new RuntimeException("Not implemented");
	}

	@Override
	public void getRangeIndexes(Boolean matchNoStart, Boolean matchNoEnd, Collection<Integer> matchings) throws IOException {
		throw new RuntimeException("Not implemented");
	}

	@Override
	public void getRangeIndexesInclusive(Boolean matchNoStart, Boolean matchNoEnd, Collection<Integer> matchings) throws IOException {
		throw new RuntimeException("Not implemented");
	}
	
	private final void parse() throws IndexOutOfBoundsException {
		if ( null == this.inputBytes) return;
		
		int available = Storable.getInt(offset, this.inputBytes);
		int packed = available/8;
		int remaining = available - packed * 8;
		
		this.parsedBooleans = new ArrayList<Boolean>(available);
		int seq = 0;
		for (int i=0; i<packed; i++) {
			for (boolean val : Storable.byteToBits(this.inputBytes[offset + 4 + i])) {
				seq++;
				if ( available < seq) break;
				this.parsedBooleans.add(val);
			}
		}
		
		if ( remaining > 0 ) {
			boolean[] x = Storable.byteToBits(this.inputBytes[offset + 4 + packed]);
			for ( int i=0; i<remaining; i++) {
				seq++;
				if ( available < seq) break;
				parsedBooleans.add(x[i]);
 			}
		}
	}

	public final void initialize() throws IndexOutOfBoundsException {
		if ( null == parsedBooleans ) parse();
		if ( null == parsedBooleans ) throw new IndexOutOfBoundsException("SortedBytesBoolean - No data exists");
	}

	@Override
	protected final int compare(final byte[] inputB, final int offset, final Boolean matchNo) {
		throw new RuntimeException("Not implemented");
	}

}

package com.bizosys.hsearch.byteutils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.google.protobuf.InvalidProtocolBufferException;

public final class SortedBytesBoolean extends SortedBytesBase<Boolean>{

	public static ISortedByte<Boolean> getInstance() {
		return new SortedBytesBoolean();
	}
	
	List<Boolean> parsedBooleans = null;
	private SortedBytesBoolean() {
		this.dataSize = -1;
	}

	@Override
	public byte[] toBytes(Collection<Boolean> sortedCollection) throws IOException {
		ByteArrays.ArrayBool.Builder booleanBuilder = ByteArrays.ArrayBool.newBuilder();
		booleanBuilder.addAllVal(sortedCollection);
		return booleanBuilder.build().toByteArray();
	}

	@Override
	public int getSize() throws IOException {
		if ( null == parsedBooleans ) parse();
		if ( null == parsedBooleans ) return 0;
		return this.parsedBooleans.size();
		
	}

	@Override
	public void addAll(Collection<Boolean> vals) throws IOException {
		if ( null != this.parsedBooleans ) parse();
		if ( null == this.parsedBooleans ) this.parsedBooleans = new ArrayList<Boolean>(); 
		this.parsedBooleans.addAll(vals);
	}

	@Override
	public Boolean getValueAt(int pos) throws IOException {
		initialize();
		return this.parsedBooleans.get(pos);
	}

	@Override
	public int getEqualToIndex(Boolean matchNo) throws IOException {
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
	public void getEqualToIndexes(Boolean matchNo, Collection<Integer> matchings) throws IOException {

		initialize();

		boolean matchNoL = matchNo.booleanValue();
		int pos = -1;
		for (boolean entity : this.parsedBooleans) {
			pos++;
			if ( matchNoL == entity) matchings.add(pos);
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
	
	private void parse() throws IOException {
		if ( null == this.inputBytes) return;
		
		byte[] bytesSubset = null;
		if ( offset == 0 && length == this.inputBytes.length) {
			bytesSubset = this.inputBytes;
		} else {
			bytesSubset = new byte[length];
			System.arraycopy(this.inputBytes, offset, bytesSubset, 0, this.length);
		}
		
		try {
			parsedBooleans = ByteArrays.ArrayBool.parseFrom(bytesSubset).getValList();
		} catch (InvalidProtocolBufferException e) {
			e.printStackTrace(System.err);
			throw new IOException(e);
		}
	}

	public void initialize() throws IOException {
		if ( null == parsedBooleans ) parse();
		if ( null == parsedBooleans ) throw new IOException("SortedBytesBoolean - No data exists");
	}

	@Override
	protected int compare(byte[] inputB, int offset, Boolean matchNo) {
		throw new RuntimeException("Not implemented");
	}

}

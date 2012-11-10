package com.bizosys.hsearch.byteutils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.google.protobuf.InvalidProtocolBufferException;

public final class SortedBytesLongCompressed extends SortedBytesBase<Long>{

	public static ISortedByte<Long> getInstance() {
		return new SortedBytesLongCompressed();
	}
	
	List<Long> parsedLongs = null;
	private SortedBytesLongCompressed() {
		this.dataSize = -1;
	}

	@Override
	public byte[] toBytes(Collection<Long> sortedCollection) throws IOException {
		ByteArrays.ArrayLong.Builder longBuilder = ByteArrays.ArrayLong.newBuilder();
		longBuilder.addAllVal(sortedCollection);
		return longBuilder.build().toByteArray();
	}

	@Override
	public int getSize() throws IOException {
		if ( null == parsedLongs ) parse();
		if ( null == parsedLongs ) return 0;
		return this.parsedLongs.size();
		
	}

	@Override
	public void addAll(Collection<Long> vals) throws IOException {
		if ( null != this.parsedLongs ) parse();
		if ( null == this.parsedLongs ) this.parsedLongs = new ArrayList<Long>(); 
		this.parsedLongs.addAll(vals);
	}

	@Override
	public Long getValueAt(int pos) throws IOException {
		initialize();
		return this.parsedLongs.get(pos);
	}

	@Override
	public int getEqualToIndex(Long matchNo) throws IOException {
		initialize();

		long matchNoL = matchNo.longValue();
		int pos = -1;
		for (long entity : this.parsedLongs) {
			pos++;
			if ( matchNoL == entity) return pos;
		}
		return -1;
	}

	@Override
	public void getEqualToIndexes(Long matchNo, Collection<Integer> matchings) throws IOException {

		initialize();

		long matchNoL = matchNo.longValue();
		int pos = -1;
		for (long entity : this.parsedLongs) {
			pos++;
			if ( matchNoL == entity) matchings.add(pos);
		}
	}

	@Override
	public void getGreaterThanIndexes(Long matchingNo, Collection<Integer> matchingPos) throws IOException {
		initialize();

		long matchNoL = matchingNo.longValue();
		int pos = -1;
		for (long entity : this.parsedLongs) {
			pos++;
			if ( matchNoL < entity) matchingPos.add(pos);
		}
	}

	@Override
	public void getGreaterThanEqualToIndexes(Long matchingNo, Collection<Integer> matchingPos) throws IOException {
		initialize();

		long matchNoL = matchingNo.longValue();
		int pos = -1;
		for (long entity : this.parsedLongs) {
			pos++;
			if ( matchNoL <= entity) matchingPos.add(pos);
		}
	}

	@Override
	public void getLessThanIndexes(Long matchingNo, Collection<Integer> matchingPos) throws IOException {
		
		initialize();

		long matchNoL = matchingNo.longValue();
		int pos = -1;
		for (long entity : this.parsedLongs) {
			pos++;
			if ( matchNoL > entity) matchingPos.add(pos);
		}
	}

	@Override
	public void getLessThanEqualToIndexes(Long matchingNo, Collection<Integer> matchingPos) throws IOException {

		initialize();

		long matchNoL = matchingNo.longValue();
		int pos = -1;
		for (long entity : this.parsedLongs) {
			pos++;
			if ( matchNoL >= entity) matchingPos.add(pos);
		}
	}

	@Override
	public void getRangeIndexes(Long matchNoStart, Long matchNoEnd, Collection<Integer> matchings) throws IOException {

		initialize();

		long matchNoSL = matchNoStart.longValue();
		long matchNoEL = matchNoEnd.longValue();
		int pos = -1;
		for (long entity : this.parsedLongs) {
			pos++;
			if ( matchNoSL > entity && matchNoEL < entity ) matchings.add(pos);
		}
	}

	@Override
	public void getRangeIndexesInclusive(Long matchNoStart, Long matchNoEnd, Collection<Integer> matchings) throws IOException {
		
		initialize();

		long matchNoSL = matchNoStart.longValue();
		long matchNoEL = matchNoEnd.longValue();
		int pos = -1;
		for (long entity : this.parsedLongs) {
			pos++;
			if ( matchNoSL >= entity && matchNoEL <= entity ) matchings.add(pos);
		}
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
			parsedLongs = ByteArrays.ArrayLong.parseFrom(bytesSubset).getValList();
		} catch (InvalidProtocolBufferException e) {
			e.printStackTrace(System.err);
			throw new IOException(e);
		}
	}

	public void initialize() throws IOException {
		if ( null == parsedLongs ) parse();
		if ( null == parsedLongs ) throw new IOException("SortedBytesLongCompressed - No data exists");
	}

	@Override
	protected int compare(byte[] inputB, int offset, Long matchNo) {
		throw new RuntimeException("Not implemented");
	}

}

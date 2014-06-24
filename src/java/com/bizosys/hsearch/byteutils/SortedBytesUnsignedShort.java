
package com.bizosys.hsearch.byteutils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

public final class SortedBytesUnsignedShort extends SortedBytesBase<Integer>{

	private short MINIMUM_ALLOWED_LIMIT = 0;
	private int MAXIMUM_ALLOWED_LIMIT = new Integer(Short.MAX_VALUE) - (Short.MIN_VALUE - MINIMUM_ALLOWED_LIMIT);
	

	public static final ISortedByte<Integer> getInstance() {
		return new SortedBytesUnsignedShort();
	}
	
	public static final SortedBytesUnsignedShort getInstanceShort() {
		return new SortedBytesUnsignedShort();
	}	

	public ISortedByte<Integer> getInstanceInt() {
		return this;
	}
	
	private SortedBytesUnsignedShort() {
		this.dataSize = 2;
	}
	
	@Override
	public final int getSize() {
		if ( null == this.inputBytes) return 0;
		return this.length / this.dataSize;
	}
	
	public final SortedBytesUnsignedShort setMinimumValueLimit(final short minVal) {
		SortedBytesUnsignedShort newProcessor = new SortedBytesUnsignedShort();
		newProcessor.MINIMUM_ALLOWED_LIMIT = minVal;
		newProcessor.MAXIMUM_ALLOWED_LIMIT = new Integer(Short.MAX_VALUE) - (Short.MIN_VALUE - newProcessor.MINIMUM_ALLOWED_LIMIT);
		return newProcessor;
	}
	
	@Override
	protected final int compare(final byte[] inputB, int offset, final Integer matchNo) {
		int val = (inputB[offset] << 8) + (inputB[++offset] & 0xff);
		if ( matchNo == val) return 0;
		if ( val > matchNo) return 1;
		return -1;
	}
	
	public final short getShort(Integer aVal) throws IOException {
		if ( aVal > MAXIMUM_ALLOWED_LIMIT) throw new IOException("Suplied Value " + 
				aVal.toString() + " is greater than maximum limit : " + MAXIMUM_ALLOWED_LIMIT);

		if (aVal < MINIMUM_ALLOWED_LIMIT) throw new IOException("Suplied Value " + 
				aVal.toString() + " is less than minimum limit." + MINIMUM_ALLOWED_LIMIT);

		aVal = aVal + Short.MIN_VALUE;
		aVal = aVal - MINIMUM_ALLOWED_LIMIT;
		
		return aVal.shortValue();
	}
	
	@Override
	public final Integer getValueAt(final int pos) {
		int val = Storable.getShort(pos*2 + offset, this.inputBytes) - Short.MIN_VALUE + MINIMUM_ALLOWED_LIMIT;
		return (val);
	}	
	
	@Override
	public final void addAll(final Collection<Integer>  vals) throws IOException {
		int total = getSize();
		if ( 0 == total ) return;
		
		for ( int pos=0; pos<total; pos++) {
			vals.add( Storable.getShort(pos*2 + offset, this.inputBytes) - Short.MIN_VALUE + MINIMUM_ALLOWED_LIMIT );
		}
	}	
	
	@Override
	public final byte[] toBytes(final Collection<Integer> sortedList) throws IOException {

		int sortedListAT = sortedList.size();
		byte[] inputsB = new byte[sortedListAT * 2];
		
		int index = 0;
		
		for (Integer aVal : sortedList) {
			Short aValS = getShort(aVal);
			System.arraycopy(Storable.putShort(aValS), 0, inputsB, index * 2, 2);
			index++;
		}
		return inputsB;
	}
	
	@Override
	public final int getEqualToIndex(final Integer aVal) throws IOException {
		int matchNo = getShort(aVal);
		return super.getEqualToIndex(matchNo);
		
	}
	
	@Override
	public final void getEqualToIndexes(final Integer aVal, final Collection<Integer> matchings) throws IOException {
		
		if ( null == this.inputBytes) return;

		short matchingNo = getShort(aVal);
		byte[] matchingNoB = Storable.putShort(matchingNo);

		int index = super.getEqualToIndex((int) matchingNo);
		if ( index == -1) return;
		
		int intBT = this.inputBytes.length / 2;
		
		//Include all matching indexes from left
		matchings.add(index);
		//System.out.println("found:" + index);
		for ( int i=index-1; i>=0; i--) {
			int pos = i * 2;
			if ( this.inputBytes[pos] !=  matchingNoB[0]) break;
			if ( this.inputBytes[pos+1] !=  matchingNoB[1]) break;
			matchings.add(i);
		}
		
		//Include all matching indexes from right
		for ( int i=index+1; i<intBT; i++) {
			int pos = i * 2;
			if ( this.inputBytes[pos] !=  matchingNoB[0]) break;
			if ( this.inputBytes[pos+1] !=  matchingNoB[1]) break;
			matchings.add(i);	
			
		}
	}	

	@Override
	public final void getNotEqualToIndexes(final Integer aVal, final Collection<Integer> matchings) throws IOException {
		
		if ( null == this.inputBytes) return;

		short matchingNo = getShort(aVal);
		byte[] matchingNoB = Storable.putShort(matchingNo);

		int index = super.getEqualToIndex((int) matchingNo);
		int intBT = this.inputBytes.length / 2;

		//Add all
		if ( index == -1) {
			for ( int i=0; i<intBT; i++) matchings.add(i);
			return;
		}
		
		//Skip all matching indexes from left
		int i=index-1;
		for ( ; i>=0; i--) {
			int pos = i * 2;
			if ( this.inputBytes[pos] !=  matchingNoB[0]) break;
			if ( this.inputBytes[pos+1] !=  matchingNoB[1]) break;
		}
		for ( int start=0; start<=i; start++) matchings.add(i);
		
		//Include all matching indexes from right
		i=index+1;
		for ( ;i<intBT; i++) {
			int pos = i * 2;
			if ( this.inputBytes[pos] !=  matchingNoB[0]) break;
			if ( this.inputBytes[pos+1] !=  matchingNoB[1]) break;
		}
		for ( int start=i; start<intBT; start++) matchings.add(i);
	}	

	@Override
	public final Collection<Integer> getGreaterThanIndexes(final Integer matchNo) throws IOException {
		Collection<Integer> matchingPos = new ArrayList<Integer>();
		int compensatedMatchingValS = getShort(matchNo);
		getGreaterThanIndexes(compensatedMatchingValS, matchingPos);
		return matchingPos;
	}
	
	@Override
	public final void getGreaterThanIndexes(final Integer matchNo, final Collection<Integer> matchingPos) throws IOException {
		int compensatedMatchingValS = getShort(matchNo);
		this.computeGTGTEQIndexes(compensatedMatchingValS, matchingPos, false);
	}

	@Override
	public final Collection<Integer> getGreaterThanEqualToIndexes(final Integer matchNo) throws IOException {
		Collection<Integer> matchingPos = new ArrayList<Integer>();
		int compensatedMatchingValS = getShort(matchNo);
		getGreaterThanEqualToIndexes(compensatedMatchingValS, matchingPos);
		return matchingPos;
	}
	
	@Override
	public final void getGreaterThanEqualToIndexes(final Integer matchNo, final Collection<Integer> matchingPos) throws IOException {
		int compensatedMatchingValS = getShort(matchNo);
		this.computeGTGTEQIndexes(compensatedMatchingValS, matchingPos, true);
	}
	
	@Override
	public final Collection<Integer> getLessThanIndexes(final Integer matchingNo) throws IOException {
		int compensatedMatchingValS = getShort(matchingNo);
		Collection<Integer> matchingPos = new ArrayList<Integer>();
		computeLTLTEQIndexes(compensatedMatchingValS, matchingPos, false);
		return matchingPos;
		
	}
	
	@Override
	public final void getLessThanIndexes(final Integer matchingNo, final Collection<Integer> matchingPos ) throws IOException {
		int compensatedMatchingValS = getShort(matchingNo);
		computeLTLTEQIndexes(compensatedMatchingValS, matchingPos, false);
	}
	
	
	@Override
	public final Collection<Integer> getLessThanEqualToIndexes(final Integer matchingNo) throws IOException {
		Collection<Integer> matchingPos = new ArrayList<Integer>();
		int compensatedMatchingValS = getShort(matchingNo);
		getLessThanEqualToIndexes(compensatedMatchingValS,matchingPos) ;
		return matchingPos;
	}

	@Override
	public final void getLessThanEqualToIndexes(final Integer matchingNo, final Collection<Integer> matchingPos) throws IOException {
		int compensatedMatchingValS = getShort(matchingNo);
		computeLTLTEQIndexes(compensatedMatchingValS, matchingPos, true);
	}



	@Override
	public final Collection<Integer> getRangeIndexes(final Integer matchNoStart, final Integer matchNoEnd) throws IOException {
		Collection<Integer> matchingPos = new ArrayList<Integer>();
		int compensatedMatchingValS = getShort(matchNoStart);
		int compensatedMatchingValE = getShort(matchNoEnd);
		getRangeIndexes(compensatedMatchingValS, compensatedMatchingValE, matchingPos);
		return matchingPos;
		
	}

	@Override
	public final void getRangeIndexes(final Integer matchNoStart, final Integer matchNoEnd, final Collection<Integer> matchings) throws IOException {
		int compensatedMatchingValS = getShort(matchNoStart);
		int compensatedMatchingValE = getShort(matchNoEnd);
		computeRangeIndexes(compensatedMatchingValS, compensatedMatchingValE, false, false, matchings);		
	}
	
	@Override
	public final Collection<Integer> getRangeIndexesInclusive(final Integer matchNoStart, final Integer matchNoEnd) throws IOException {
		int compensatedMatchingValS = getShort(matchNoStart);
		int compensatedMatchingValE = getShort(matchNoEnd);
		Collection<Integer> matchingPos = new ArrayList<Integer>();
		getRangeIndexesInclusive(compensatedMatchingValS, compensatedMatchingValE);
		return matchingPos;
	}
	
	public final void getRangeIndexesInclusive(final Integer matchNoStart, final Integer matchNoEnd, final Collection<Integer> matchings) throws IOException {
		int compensatedMatchingValS = getShort(matchNoStart);
		int compensatedMatchingValE = getShort(matchNoEnd);
		computeRangeIndexes(compensatedMatchingValS, compensatedMatchingValE, true, true, matchings);		
	}
	

	@Override
	public final Collection<Integer> getRangeIndexesInclusive(final Integer matchNoStart, 
			final boolean startMatch, final Integer matchNoEnd, final boolean endMatch) throws IOException {
		
		int compensatedMatchingValS = getShort(matchNoStart);
		int compensatedMatchingValE = getShort(matchNoEnd);
		Collection<Integer> matchingPos = new ArrayList<Integer>();
		getRangeIndexesInclusive(compensatedMatchingValS, startMatch, compensatedMatchingValE, endMatch, matchingPos);		
		return matchingPos;
	}

	@Override
	public final void getRangeIndexesInclusive(final Integer matchNoStart, final boolean startMatch, 
			final Integer matchNoEnd, final boolean endMatch, final Collection<Integer> matchings) throws IOException {
		int compensatedMatchingValS = getShort(matchNoStart);
		int compensatedMatchingValE = getShort(matchNoEnd);
		computeRangeIndexes(compensatedMatchingValS, compensatedMatchingValE, startMatch, endMatch, matchings);		
	}
		
}

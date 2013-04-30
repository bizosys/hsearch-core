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
package com.bizosys.hsearch.functions;

import com.bizosys.hsearch.functions.GroupSorter.GroupSorterSequencer;

public abstract class GroupSortedObject implements  Comparable<GroupSortedObject>{
	
	public static enum FieldType {
		BOOLEAN, BYTE, SHORT, INTEGER, FLOAT, LONG, DOUBLE, STRING, OBJECT
	}
	
	public abstract boolean getBooleanField(int fldSequence);
	public abstract byte getByteField(int fldSequence);
	public abstract short getShortField(int fldSequence);
	public abstract int getIntegerField(int fldSequence);
	public abstract float getFloatField(int fldSequence);
	public abstract double getDoubleField(int fldSequence);
	public abstract long getLongField(int fldSequence);
	public abstract String getStringField(int fldSequence);
	public abstract Object getObjectField(int fldSequence);
	
	
	GroupSorterSequencer[] sortedSequences = null;
	int sortedSequencesT = 0;
	
	public void setSortedSequencers(GroupSorterSequencer[] sortedSequences) throws ArrayIndexOutOfBoundsException{
		if (null == sortedSequences) throw new 
			ArrayIndexOutOfBoundsException("Null sorted Sequences in " + GroupSortedObject.class.getName());
		this.sortedSequences = sortedSequences;
		this.sortedSequencesT = sortedSequences.length;
	}
	
	@Override
	public int compareTo(GroupSortedObject o) {

		int compareFieldRes = 0;
		for (int i=0; i<sortedSequencesT; i++) {
			GroupSorterSequencer sequencer = sortedSequences[i];
			compareFieldRes = compareField(sequencer,o);
			if ( compareFieldRes == 0 ) { 
				if ( i == (sortedSequencesT-1) ) return 0;
				else continue;
			} else {
				return compareFieldRes;
			}
		}
		return 0;
	}
	
	public final int compareField(final GroupSorterSequencer aSortFld, final GroupSortedObject o) {
		switch ( aSortFld.fldType) {
			case  BOOLEAN:
			{
					boolean l = this.getBooleanField(aSortFld.fldSeq);
					boolean r =  o.getBooleanField(aSortFld.fldSeq);
					return compareBoolean(l, r);
			}
			case  BYTE:
			{
					byte l = this.getByteField(aSortFld.fldSeq);
					byte r =  o.getByteField(aSortFld.fldSeq);
					return compareByte(l, r);
			}
			case  SHORT:
			{
					short l = this.getShortField(aSortFld.fldSeq);
					short r =  o.getShortField(aSortFld.fldSeq);
					return compareShort(l, r);
			}
			case  INTEGER:
			{
					int l = this.getIntegerField(aSortFld.fldSeq);
					int r =  o.getIntegerField(aSortFld.fldSeq);
					return compareInteger(l, r);
			}
			case  FLOAT:
			{
					float l = this.getFloatField(aSortFld.fldSeq);
					float r =  o.getFloatField(aSortFld.fldSeq);
					return compareFloat(l, r);
			}
			case  LONG:
			{
					long l = this.getLongField(aSortFld.fldSeq);
					long r =  o.getLongField(aSortFld.fldSeq);
					return compareLong(l, r);
			}
			case  DOUBLE:
			{
					double l = this.getDoubleField(aSortFld.fldSeq);
					double r =  o.getDoubleField(aSortFld.fldSeq);
					return compareDouble(l, r);
			}
			case  STRING:
			{
					String l = this.getStringField(aSortFld.fldSeq);
					String r =  o.getStringField(aSortFld.fldSeq);
					return compareString(l, r);
			}
			default:
				throw new ArrayIndexOutOfBoundsException(aSortFld.fldType.toString() +" not valid field type.");
		}
	}

	
	public final static int compareBoolean(final boolean o1, final boolean o2) {
		if ( o1 == o2 ) return 0;
		else return 1;
	}
	public final static int compareByte(final byte o1, final byte o2) {
		if ( o1 == o2 ) return 0;
		if ( o1 < o2 ) return -1;
		else return 1;
	}
	public final static int compareShort(final short o1, final short o2) {
		if ( o1 == o2 ) return 0;
		if ( o1 < o2 ) return -1;
		else return 1;
	}
	public final static int compareInteger(final int o1, final int o2) {
		if ( o1 == o2 ) return 0;
		if ( o1 < o2 ) return -1;
		else return 1;
	}
	public int compareFloat(final float o1, final float o2) {
		if ( o1 == o2 ) return 0;
		if ( o1 < o2 ) return -1;
		else return 1;
	}

	public final static int compareLong(final long o1, final long o2) {
		if ( o1 == o2 ) return 0;
		if ( o1 < o2 ) return -1;
		else return 1;
	}
	public int compareDouble(final double o1, final double o2) {
		if ( o1 == o2 ) return 0;
		if ( o1 < o2 ) return -1;
		else return 1;
	}
	public final static int compareString(final String o1, final String o2) {
		return ( o1.compareTo(o2) );
	}

	
}

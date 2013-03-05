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
package com.bizosys.hsearch.treetable.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.StringTokenizer;

import com.bizosys.hsearch.byteutils.ByteArrays;
import com.bizosys.hsearch.byteutils.SortedBytesDouble;
import com.bizosys.hsearch.hbase.HbaseLog;
import com.bizosys.hsearch.treetable.CellBase;

public class HSearchTablePartition {
	
	public static boolean DEBUG_ENABLED = HbaseLog.l.isDebugEnabled();
	
	private static HSearchTablePartition singleton =  new HSearchTablePartition();
	
	public static HSearchTablePartition getInstance() {
		return singleton;
	}
	
	public List<Double> distBiundaries = new ArrayList<Double>();
	private HSearchTablePartition() {
	}
	
	public void initialize(String partitionPoints) {
		StringTokenizer stk = new StringTokenizer(partitionPoints, ",");
		while ( stk.hasMoreElements()) {
			Double partPoint = Double.parseDouble(stk.nextToken());
			distBiundaries.add(partPoint);
		}
	}
	
	public void setOnePartitionOnly() {
		distBiundaries.clear();
		distBiundaries.add( new Double(Long.MAX_VALUE));
	}
	
	public List<Integer> getPartitionSequences(double start, double end) {
		
		if ( DEBUG_ENABLED ) HbaseLog.l.debug("Start :" + start + " , End : " + end);
		
		List<Integer> positions = new ArrayList<Integer>();
	
		int startPos = -1;
		for (Double boundary : distBiundaries) {
			startPos++;
			if ( start > boundary ) continue;
			break;
		}
		
		int endPos = -1;
		for (Double boundary : distBiundaries) {
			endPos++;
			if ( end < boundary) break; //Not for Equal
		}
		
		for ( int pos=startPos ; pos <= endPos; pos++) {
			positions.add(pos);
		}
		
		if ( DEBUG_ENABLED ) HbaseLog.l.debug( start + " > startPos:" + distBiundaries.get(startPos) + "  , endpos:" + end + " > " + + distBiundaries.get(endPos) );
		return positions;
	}
	
	public List<Integer> getAllSequences() {
		List<Integer> positions = new ArrayList<Integer>();
		int total = distBiundaries.size();
		for ( int i=0; i<total; i++)  positions.add(i);
		return positions;
	}

	
	public byte[] addToDistribution(int distPos, List<HSearchTablePartitionComparator> input) throws IOException {
	
		int pos = -1;
		double boundaryLeft = Long.MIN_VALUE;
		double boundaryRight = boundaryLeft;
		
		for (Double boundary : distBiundaries) {
			boundaryLeft = boundaryRight; 
			boundaryRight = boundary;
			pos++;
			
			if ( pos != distPos) continue;

			ByteArrays.ArrayLong.Builder keyBuilder = ByteArrays.ArrayLong.newBuilder();
			
			Collection<Double> values = new ArrayList<Double>();
			
			for (HSearchTablePartitionComparator entry : input) {
				Double val = entry.value;
				boolean isLeft = ( val > boundaryLeft);
				//if ( !isLeft) continue;

				boolean isRight = ( entry.value <= boundaryRight);
				
				if ( DEBUG_ENABLED ) {
					if ( DEBUG_ENABLED ) HbaseLog.l.debug( "Key :" + entry.key + " : " +
						boundaryLeft + "<" + val + "<" +  boundaryRight + "   :  "+  isLeft + "&&" + isRight );
				}
				
				if ( isLeft && isRight) {
					keyBuilder.addVal(entry.key);
					values.add(entry.value);
				}
			}

			if ( values.size() == 0 ) return null;
			
			return CellBase.serializeKV(keyBuilder.build().toByteArray() , SortedBytesDouble.getInstance().toBytes(values));
		}
		return null;
	}
}

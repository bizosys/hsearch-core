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

package com.bizosys.hsearch.distribution;
import java.util.Arrays;
import java.util.Collection;

public final class DistributionPartionDouble {

	protected double[] distributes(Collection<Double> inputs, int parts) {
		
		double[] output = new double[parts - 1];
		double[] ranges = new double[]{Long.MIN_VALUE, Long.MAX_VALUE};
		GravityCenter gc = distribute(inputs, ranges);
		output[0] = gc.avgValue;
		System.out.println(output[0]);
		ranges = new double[]{Long.MIN_VALUE, gc.avgValue};
		for ( int i=1; i< (parts/2); i++) {
			GravityCenter mingc = distribute(inputs, ranges);
			output[i] = mingc.avgValue;
			ranges = new double[]{Long.MIN_VALUE, mingc.avgValue};
		}

		ranges = new double[]{gc.avgValue, Long.MAX_VALUE};
		for ( int i=parts/2; i< parts - 1; i++) {
			GravityCenter maxgc = distribute(inputs, ranges);
			output[i] = maxgc.avgValue;
			ranges = new double[]{maxgc.avgValue, Long.MAX_VALUE};
		}
		
		Arrays.sort(output);
		return output;
	}

	private GravityCenter distribute(Collection<Double> inputs, double[] ranges) {
		GravityCenter wts = new GravityCenter();

		getAverage(inputs, wts, ranges);
		int loops = 0;
		while (true) {
			loops++;
			if ( loops > 30) break;

			/**
			System.out.println("Min:" + wts.minValue + " Avg:" + wts.avgValue
					+ " Max:" + wts.maxValue + " LeftW:" + wts.leftWeight
					+ " RightW:" + wts.rightWeight);
			*/
			getWeights(inputs, wts, ranges);

			if (wts.leftWeight > wts.rightWeight) {
				double avgValue = wts.avgValue;
				wts.avgValue = avgValue - (avgValue/2 - wts.minValue/2);
				wts.maxValue = avgValue;
			} else {
				double avgValue = wts.avgValue;
				wts.avgValue = avgValue + (wts.maxValue/2 - avgValue/2);
				wts.minValue = avgValue;
			}
			
			int diff = (wts.leftWeight > wts.rightWeight) ? (wts.leftWeight - wts.rightWeight) : (wts.rightWeight - wts.leftWeight);
			if ( diff <= 1 ) {
				/**
				System.out.println("Min:" + wts.minValue + " Avg:" + wts.avgValue
						+ " Max:" + wts.maxValue + " LeftW:" + wts.leftWeight
						+ " RightW:" + wts.rightWeight);
				*/
				break;
			}
		}
		return wts;
		
	}

	
	
	private void getAverage(Collection<Double> inputs, GravityCenter wts, double[] ranges) {
		double min = Long.MAX_VALUE;
		double max = Long.MIN_VALUE;
		for (double i : inputs) {
			if ( i < ranges[0] || i > ranges[1] ) continue;
			if (i < min) min = i;
			else if (i > max) max = i;
		}
		wts.minValue = min;
		wts.maxValue = max;
		wts.avgValue = (max/2 - min/2);
	}

	private void getWeights(Collection<Double> inputs, GravityCenter weights, double[] ranges) {
		int left=0, right=0;
		double avgValue = weights.avgValue;
		for (double i : inputs) {
			if ( i < ranges[0] || i > ranges[1] ) continue;
			if (i >= avgValue) right++;
			else left++;
		}
		weights.leftWeight = left;
		weights.rightWeight = right;
	}
	
	
	private class GravityCenter {
		public double minValue;
		public double maxValue;
		public double avgValue;
		public int leftWeight;
		public int rightWeight;
		
		public GravityCenter() {
		}
	}	

	
}

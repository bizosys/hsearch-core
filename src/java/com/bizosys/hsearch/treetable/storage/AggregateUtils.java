package com.bizosys.hsearch.treetable.storage;

import java.io.IOException;
import java.util.List;

import com.bizosys.hsearch.treetable.client.HSearchPluginPoints;

public class AggregateUtils {
	
	public static final int getMergedCount(int outputCode) throws IOException {
		int merged = 1;
		switch ( outputCode) {
			case HSearchPluginPoints.OUTPUT_MIN:
			case HSearchPluginPoints.OUTPUT_MAX:
			case HSearchPluginPoints.OUTPUT_AVG:
			case HSearchPluginPoints.OUTPUT_SUM:
				merged = 1;
				break;
			case HSearchPluginPoints.OUTPUT_MIN_MAX:
				merged = 2;
				break;

			case HSearchPluginPoints.OUTPUT_MIN_MAX_AVG:
			case HSearchPluginPoints.OUTPUT_MIN_MAX_COUNT:
			case HSearchPluginPoints.OUTPUT_MIN_MAX_SUM:
			case HSearchPluginPoints.OUTPUT_AVG_SUM_COUNT:
				merged = 3;
				break;
				
			case HSearchPluginPoints.OUTPUT_MIN_MAX_AVG_COUNT:
			case HSearchPluginPoints.OUTPUT_MIN_MAX_SUM_AVG:
			case HSearchPluginPoints.OUTPUT_MIN_MAX_SUM_COUNT:
				merged = 4;
				break;
			case HSearchPluginPoints.OUTPUT_MIN_MAX_AVG_SUM_COUNT:
				merged = 5;
				break;
			default:
				throw new IOException("HSearchCoprocessor Not a agregate type - " + outputCode);
		}
		return merged;
	}
	
	public static final void initializeDefault(int outputType, double[] output, int queries) throws IOException {
		
		switch (outputType) {
			case HSearchPluginPoints.OUTPUT_MIN:
			case HSearchPluginPoints.OUTPUT_MAX:
			case HSearchPluginPoints.OUTPUT_AVG:
			case HSearchPluginPoints.OUTPUT_SUM:
				for (int i=0; i<queries; i++) output[i] = 0;
				break;

			case HSearchPluginPoints.OUTPUT_MIN_MAX:
				for (int i=0; i<queries; i++) output[i] = Long.MAX_VALUE;
				for (int i=queries; i<queries * 2; i++) output[i] = Long.MIN_VALUE;
				break;

			case HSearchPluginPoints.OUTPUT_MIN_MAX_AVG:
			case HSearchPluginPoints.OUTPUT_MIN_MAX_COUNT:
			case HSearchPluginPoints.OUTPUT_MIN_MAX_SUM:
				for (int i=0; i<queries; i++) output[i] = Long.MAX_VALUE;
				for (int i=queries; i<queries * 2; i++) output[i] = Long.MIN_VALUE;
				for (int i=queries * 2; i<queries * 3; i++) output[i] = 0;
				break;
			
			case HSearchPluginPoints.OUTPUT_AVG_SUM_COUNT:
				for (int i=0; i<queries * 3; i++) output[i] = 0;
				break;
				
			case HSearchPluginPoints.OUTPUT_MIN_MAX_AVG_COUNT:
			case HSearchPluginPoints.OUTPUT_MIN_MAX_SUM_AVG:
			case HSearchPluginPoints.OUTPUT_MIN_MAX_SUM_COUNT:
				for (int i=0; i<queries; i++) output[i] = Long.MAX_VALUE;
				for (int i=queries; i<queries * 2; i++) output[i] = Long.MIN_VALUE;
				for (int i=queries * 2; i<queries * 3; i++) output[i] = 0;
				for (int i=queries * 3; i<queries * 4; i++) output[i] = 0;
				break;
			case HSearchPluginPoints.OUTPUT_MIN_MAX_AVG_SUM_COUNT:
				for (int i=0; i<queries; i++) output[i] = Long.MAX_VALUE;
				for (int i=queries; i<queries * 2; i++) output[i] = Long.MIN_VALUE;
				for (int i=queries * 2; i<queries * 3; i++) output[i] = 0;
				for (int i=queries * 3; i<queries * 4; i++) output[i] = 0;
				for (int i=queries * 4; i<queries * 5; i++) output[i] = 0;
				break;
			default:
				throw new IOException("HSearchCoprocessor Not a agregate type - " + outputType);
		}
	}	
	
	public static int computeAgreegates(HSearchGenericFilter filter, int resultBunch,
			double[] queryPartAggvWithTotallingAtTop,
			List<Double> foundAggregates) throws IOException {
		int outputType = filter.outputType.getOutputType();
		switch ( outputType ) {
			case HSearchPluginPoints.OUTPUT_MIN:
			case HSearchPluginPoints.OUTPUT_MAX:
			case HSearchPluginPoints.OUTPUT_AVG:
			case HSearchPluginPoints.OUTPUT_SUM:
				computeAggregate(outputType,
					queryPartAggvWithTotallingAtTop, foundAggregates, resultBunch, 0);
				break;
				
			case HSearchPluginPoints.OUTPUT_MIN_MAX:
				computeAggregate(HSearchPluginPoints.OUTPUT_MIN, queryPartAggvWithTotallingAtTop, foundAggregates,resultBunch, 0);
				computeAggregate(HSearchPluginPoints.OUTPUT_MAX, queryPartAggvWithTotallingAtTop, foundAggregates, resultBunch, 1);
				break;

			case HSearchPluginPoints.OUTPUT_MIN_MAX_AVG:
				computeAggregate(HSearchPluginPoints.OUTPUT_MIN, queryPartAggvWithTotallingAtTop, foundAggregates,resultBunch, 0);
				computeAggregate(HSearchPluginPoints.OUTPUT_MAX, queryPartAggvWithTotallingAtTop, foundAggregates, resultBunch, 1);
				computeAggregate(HSearchPluginPoints.OUTPUT_AVG, queryPartAggvWithTotallingAtTop, foundAggregates, resultBunch, 2);
				break;
			case HSearchPluginPoints.OUTPUT_MIN_MAX_COUNT:
				computeAggregate(HSearchPluginPoints.OUTPUT_MIN, queryPartAggvWithTotallingAtTop, foundAggregates,resultBunch, 0);
				computeAggregate(HSearchPluginPoints.OUTPUT_MAX, queryPartAggvWithTotallingAtTop, foundAggregates, resultBunch, 1);
				computeAggregate(HSearchPluginPoints.OUTPUT_COUNT, queryPartAggvWithTotallingAtTop, foundAggregates, resultBunch, 2);
				break;
			case HSearchPluginPoints.OUTPUT_MIN_MAX_SUM:
				computeAggregate(HSearchPluginPoints.OUTPUT_MIN, queryPartAggvWithTotallingAtTop, foundAggregates,resultBunch, 0);
				computeAggregate(HSearchPluginPoints.OUTPUT_MAX, queryPartAggvWithTotallingAtTop, foundAggregates, resultBunch, 1);
				computeAggregate(HSearchPluginPoints.OUTPUT_SUM, queryPartAggvWithTotallingAtTop, foundAggregates, resultBunch, 2);
				break;
				
			case HSearchPluginPoints.OUTPUT_AVG_SUM_COUNT:
				computeAggregate(HSearchPluginPoints.OUTPUT_AVG, queryPartAggvWithTotallingAtTop, foundAggregates,resultBunch, 0);
				computeAggregate(HSearchPluginPoints.OUTPUT_SUM, queryPartAggvWithTotallingAtTop, foundAggregates, resultBunch, 1);
				computeAggregate(HSearchPluginPoints.OUTPUT_COUNT, queryPartAggvWithTotallingAtTop, foundAggregates, resultBunch, 2);
				break;

			case HSearchPluginPoints.OUTPUT_MIN_MAX_AVG_COUNT:
				computeAggregate(HSearchPluginPoints.OUTPUT_MIN, queryPartAggvWithTotallingAtTop, foundAggregates,resultBunch, 0);
				computeAggregate(HSearchPluginPoints.OUTPUT_MAX, queryPartAggvWithTotallingAtTop, foundAggregates, resultBunch, 1);
				computeAggregate(HSearchPluginPoints.OUTPUT_AVG, queryPartAggvWithTotallingAtTop, foundAggregates, resultBunch, 2);
				computeAggregate(HSearchPluginPoints.OUTPUT_COUNT, queryPartAggvWithTotallingAtTop, foundAggregates, resultBunch, 3);
				break;
			case HSearchPluginPoints.OUTPUT_MIN_MAX_SUM_AVG:
				computeAggregate(HSearchPluginPoints.OUTPUT_MIN, queryPartAggvWithTotallingAtTop, foundAggregates,resultBunch, 0);
				computeAggregate(HSearchPluginPoints.OUTPUT_MAX, queryPartAggvWithTotallingAtTop, foundAggregates, resultBunch, 1);
				computeAggregate(HSearchPluginPoints.OUTPUT_SUM, queryPartAggvWithTotallingAtTop, foundAggregates, resultBunch, 2);
				computeAggregate(HSearchPluginPoints.OUTPUT_AVG, queryPartAggvWithTotallingAtTop, foundAggregates, resultBunch, 3);
				break;
			case HSearchPluginPoints.OUTPUT_MIN_MAX_SUM_COUNT:
				computeAggregate(HSearchPluginPoints.OUTPUT_MIN, queryPartAggvWithTotallingAtTop, foundAggregates,resultBunch, 0);
				computeAggregate(HSearchPluginPoints.OUTPUT_MAX, queryPartAggvWithTotallingAtTop, foundAggregates, resultBunch, 1);
				computeAggregate(HSearchPluginPoints.OUTPUT_SUM, queryPartAggvWithTotallingAtTop, foundAggregates, resultBunch, 2);
				computeAggregate(HSearchPluginPoints.OUTPUT_COUNT, queryPartAggvWithTotallingAtTop, foundAggregates, resultBunch, 3);
				break;
			case HSearchPluginPoints.OUTPUT_MIN_MAX_AVG_SUM_COUNT:
				computeAggregate(HSearchPluginPoints.OUTPUT_MIN, queryPartAggvWithTotallingAtTop, foundAggregates,resultBunch, 0);
				computeAggregate(HSearchPluginPoints.OUTPUT_MAX, queryPartAggvWithTotallingAtTop, foundAggregates, resultBunch, 1);
				computeAggregate(HSearchPluginPoints.OUTPUT_AVG, queryPartAggvWithTotallingAtTop, foundAggregates, resultBunch, 2);
				computeAggregate(HSearchPluginPoints.OUTPUT_SUM, queryPartAggvWithTotallingAtTop, foundAggregates, resultBunch, 3);
				computeAggregate(HSearchPluginPoints.OUTPUT_COUNT, queryPartAggvWithTotallingAtTop, foundAggregates, resultBunch, 4);
				break;
			default:
				throw new IOException("HSearchCoprocessor Not a agregate type - " + filter.outputType.toStringHumanReadable());
		}
		return outputType;
	}


	
	public static void computeAggregate(int output, double[] finalOutputValues,
			List<Double> appendValues, int resultBunch, int aggvIndex ) throws IOException {
		
		int index = -1;
		
		switch(output) {
			case HSearchPluginPoints.OUTPUT_COUNT:
				for ( int i=0; i<resultBunch; i++) {
					finalOutputValues[i] += appendValues.get(resultBunch * aggvIndex + i).doubleValue();
				}
				break;
			case HSearchPluginPoints.OUTPUT_AVG:
				for ( int i=0; i<resultBunch; i++) {
					finalOutputValues[i] += appendValues.get(resultBunch * aggvIndex + i).doubleValue();
					finalOutputValues[i] = finalOutputValues[i] / 2;
				}
				break;
			case HSearchPluginPoints.OUTPUT_MAX:
				for ( int i=0; i< resultBunch; i++) {
					index = resultBunch * aggvIndex + i;
					double d = appendValues.get(index).doubleValue();
					if ( finalOutputValues[index] < d )  finalOutputValues[index] = d;
				}
				break;
				
			case HSearchPluginPoints.OUTPUT_MIN:
				for ( int i=0; i<resultBunch; i++) {
					index = resultBunch * aggvIndex + i;
					double d = appendValues.get(index).doubleValue();
					if ( finalOutputValues[index] > d )  finalOutputValues[index] = d;
				}
				break;
			case HSearchPluginPoints.OUTPUT_SUM:
				for ( int i=0; i<resultBunch; i++) {
					finalOutputValues[i]  += appendValues.get(resultBunch * aggvIndex + i).doubleValue(); 
				}
				break;
				
			default:
				throw new IOException("Not able to process the aggv type - Generic Coprocessor" +  output);
		}
	}
		
}

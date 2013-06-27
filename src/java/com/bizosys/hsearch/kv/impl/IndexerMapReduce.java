package com.bizosys.hsearch.kv.impl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import com.bizosys.hsearch.byteutils.SortedBytesBoolean;
import com.bizosys.hsearch.byteutils.SortedBytesChar;
import com.bizosys.hsearch.byteutils.SortedBytesDouble;
import com.bizosys.hsearch.byteutils.SortedBytesFloat;
import com.bizosys.hsearch.byteutils.SortedBytesInteger;
import com.bizosys.hsearch.byteutils.SortedBytesLong;
import com.bizosys.hsearch.byteutils.SortedBytesString;
import com.bizosys.hsearch.treetable.Cell2;
import com.bizosys.hsearch.treetable.CellComparator;
import com.bizosys.hsearch.util.LineReaderUtil;

public class IndexerMapReduce{


	private static final String XML_FILE_PATH = "CONFIG_XMLFILE_LOCATION";
	public static final char FIELD_SEPARATOR = '|';
	public static final char RECORD_SEPARATOR = '\n';
	public static final byte[] COL_NAME = new byte[]{0};
	public static final byte[] FAM_NAME = "1".getBytes();

    public static class KVMapper extends Mapper<LongWritable, Text, Text, Text> {
  	  
		LineReaderUtil util = new LineReaderUtil();
    	
    	Text keVal = new Text();
    	String[] result = null;
    	Set<Integer> neededPositions = null; 
    	FieldMapping fm = null;

		Map<Integer,String> rowIdMap = new HashMap<Integer, String>();
		StringBuilder rowkKeybuilder = new StringBuilder();

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {

			Configuration conf = context.getConfiguration();
			String path = conf.get(XML_FILE_PATH);
			StringBuilder sb = new StringBuilder();
			
			try {
				Path hadoopPath = new Path(path);
				System.out.println("path " + hadoopPath);
				FileSystem fs = FileSystem.get(new Configuration());
				BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(hadoopPath)));
				String line = null;
				while((line = br.readLine())!=null) {
					sb.append(line);
				}
			} catch (Exception e) {
				System.err.println("Cannot read from path " + path);
			}
			
			fm = FieldMapping.getXMLStringFieldMappings(sb.toString());
			neededPositions = fm.fieldSeqs.keySet();
		}
    	
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        	if ( null == result) {
        		result = value.toString().split("|");
        	}
        	Arrays.fill(result, null);
        	
        	LineReaderUtil.fastSplit(result, value.toString(), FIELD_SEPARATOR);
        	int recordKeyPos = -1;
        	String fldValue = "";
        	String rowId = "";
        	rowIdMap.clear();
	    	
			for (int needIndex : neededPositions) {
	    		fldValue = result[needIndex];
	    		FieldMapping.Field fld = fm.fieldSeqs.get(needIndex);
	    		if ( fld.isMergedKey) {
	    			rowIdMap.put(fld.mergePosition, fldValue);
	    		}if ( fld.isJoinKey ){
	    			recordKeyPos = needIndex;
	    		} 
	    	}
			
			String[] megedKeyArr = new String[rowIdMap.size()];

			for (Integer mergePosition : rowIdMap.keySet()) {
				megedKeyArr[mergePosition] = rowIdMap.get(mergePosition);
			}
			for (int j = 0; j < megedKeyArr.length; j++) {
				if(megedKeyArr[j] == null || megedKeyArr[j].length() == 0)continue;
				rowId += megedKeyArr[j] + "_";
			}

			for ( int needIndex : neededPositions) {
        		
        		fldValue = result[needIndex];
        		FieldMapping.Field fld = fm.fieldSeqs.get(needIndex);
        		
        		if ( fld.isMergedKey) continue;
        		if ( fld.isJoinKey ) continue;
        		if ( !fld.isIndexable) continue;
        		
        		rowkKeybuilder.delete(0, rowkKeybuilder.capacity());
        		
        		boolean isEmpty = ( null == rowId) ? true : (rowId.length() == 0);
        		String rowKey = ( isEmpty) ? fld.name : rowId + fld.name;
        		rowKey = rowkKeybuilder.append(rowKey).append( FIELD_SEPARATOR ).append( fld.fieldType )
        							   .append( FIELD_SEPARATOR ).append( fld.skipNull ).append( FIELD_SEPARATOR)
        							   .append(fld.defaultValue).toString();
        		
          		context.write(new Text(rowKey), new Text(result[recordKeyPos] + FIELD_SEPARATOR + fldValue) );
        	}
        }
    }

    public static class KVReducer extends TableReducer<Text, Text, ImmutableBytesWritable> {
    	
    	String line = "";
    	String[] resultKey = new String[4];
    	String[] resultValue = new String[2];
    	
    	static Map<String, Character> dataTypesPrimitives = new HashMap<String, Character>();
    	
    	static {
    		dataTypesPrimitives.put("string", 't');
    		dataTypesPrimitives.put("int", 'i');
    		dataTypesPrimitives.put("float", 'f');
    		dataTypesPrimitives.put("double", 'd');
    		dataTypesPrimitives.put("long", 'l');
    		dataTypesPrimitives.put("short", 's');
    		dataTypesPrimitives.put("boolean", 'b');
    		dataTypesPrimitives.put("byte", 'c');
    	}
		
    	@Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
	    	String keyData = key.toString();
			
	    	LineReaderUtil.fastSplit(resultKey, keyData, FIELD_SEPARATOR);
			String rowKey = resultKey[0];
			String dataType = resultKey[1].toLowerCase();
			boolean skipNull = resultKey[2].equalsIgnoreCase("true") ? true : false;
			String skipDefaultvalue = resultKey[3];
			
			char dataTypeChar = dataTypesPrimitives.get(dataType);
			byte[] finalData = null;
			boolean hasValue = false;

			switch (dataTypeChar) {
				case 't':
				{
					Cell2<Integer,String> nonrepeatableCell = new Cell2<Integer, String>
						(SortedBytesInteger.getInstance(), SortedBytesString.getInstance());
					for (Text text : values) {
						
						if ( null == text) continue;
						Arrays.fill(resultValue, null);

						line = text.toString();
						
						LineReaderUtil.fastSplit(resultValue, line, FIELD_SEPARATOR);
						int containerKey = Integer.parseInt(resultValue[0]);
						String containervalue = resultValue[1];
						if ( null == containervalue){
							if(skipNull) continue;
							else containervalue = skipDefaultvalue;
						} 
						
						hasValue = true;
						nonrepeatableCell.add(containerKey, containervalue);
					}
					
					if ( hasValue ) {
						nonrepeatableCell.sort(new CellComparator.StringComparator<Integer>());
						finalData = nonrepeatableCell.toBytesOnSortedData();
					}
				}
				break;

				case 'i':
				{
					Cell2<Integer,Integer> nonrepeatableCell = new Cell2<Integer, Integer>
							(SortedBytesInteger.getInstance(), SortedBytesInteger.getInstance());
					
					for (Text text : values) {
						
						if ( null == text) continue;
						Arrays.fill(resultValue, null);

						line = text.toString();
						
						LineReaderUtil.fastSplit(resultValue, line, FIELD_SEPARATOR);
						int containerKey = Integer.parseInt(resultValue[0]);
						int containervalue = 0;
						if ( null == resultValue[1]){
							if(skipNull) continue;
							else containervalue = Integer.parseInt(skipDefaultvalue);
						} else{
							containervalue = Integer.parseInt(resultValue[1]);	
						}

						hasValue = true;
						nonrepeatableCell.add(containerKey, containervalue);
					}
					
					if ( hasValue ) {
						nonrepeatableCell.sort(new CellComparator.IntegerComparator<Integer>());
						finalData = nonrepeatableCell.toBytesOnSortedData();
					}
				}
				break;

				case 'f':
				{
						
					Cell2<Integer,Float> nonrepeatableCell = new Cell2<Integer, Float>
						(SortedBytesInteger.getInstance(), SortedBytesFloat.getInstance());
					
					for (Text text : values) {
						if ( null == text) continue;
						Arrays.fill(resultValue, null);

						line = text.toString();
						
						LineReaderUtil.fastSplit(resultValue, line, FIELD_SEPARATOR);
						int containerKey = Integer.parseInt(resultValue[0]);
						float containervalue = 0.0f;
						if ( null == resultValue[1]){
							if(skipNull) continue;
							else containervalue = Float.parseFloat(skipDefaultvalue);
						} else{
							containervalue = Float.parseFloat(resultValue[1]);	
						}
						
						hasValue = true;
						nonrepeatableCell.add(containerKey, containervalue);
					}
					
					if ( hasValue ) {
						nonrepeatableCell.sort(new CellComparator.FloatComparator<Integer>());
						finalData = nonrepeatableCell.toBytesOnSortedData();
					}
					break;
				}

				case 'd':
				{
						
					Cell2<Integer,Double> nonrepeatableCell = new Cell2<Integer, Double>
						(SortedBytesInteger.getInstance(),SortedBytesDouble.getInstance());
					
					for (Text text : values) {
						if ( null == text) continue;
						Arrays.fill(resultValue, null);

						line = text.toString();
						
						LineReaderUtil.fastSplit(resultValue, line, FIELD_SEPARATOR);
						int containerKey = Integer.parseInt(resultValue[0]);
						double containervalue = 0.0;
						if ( null == resultValue[1]){
							if(skipNull) continue;
							else containervalue = Double.parseDouble(skipDefaultvalue);
						} else{
							containervalue = Double.parseDouble(resultValue[1]);	
						}
						
						hasValue = true;
						nonrepeatableCell.add(containerKey, containervalue);
					}
					
					if ( hasValue ) {
						nonrepeatableCell.sort(new CellComparator.DoubleComparator<Integer>());
						finalData = nonrepeatableCell.toBytesOnSortedData();
					}
					
				break;
				}

				case 'l':
				{
					Cell2<Integer,Long> nonrepeatableCell = new Cell2<Integer, Long>
							(SortedBytesInteger.getInstance(),SortedBytesLong.getInstance());
					
					for (Text text : values) {
						if ( null == text) continue;
						Arrays.fill(resultValue, null);

						line = text.toString();
						
						LineReaderUtil.fastSplit(resultValue, line, FIELD_SEPARATOR);
						int containerKey = Integer.parseInt(resultValue[0]);
						long containervalue = 0;
						if ( null == resultValue[1]){
							if(skipNull) continue;
							else containervalue = Long.parseLong(skipDefaultvalue);
						} else{
							containervalue = Long.parseLong(resultValue[1]);	
						}
						
						hasValue = true;
						nonrepeatableCell.add(containerKey, containervalue);
					}
					
					if ( hasValue ) {
						nonrepeatableCell.sort(new CellComparator.LongComparator<Integer>());
						finalData = nonrepeatableCell.toBytesOnSortedData();
					}
				}
				break;
				case 'b':
				{
					Cell2<Integer,Boolean> nonrepeatableCell = new Cell2<Integer, Boolean>
							(SortedBytesInteger.getInstance(),SortedBytesBoolean.getInstance());
					
					for (Text text : values) {
						if ( null == text) continue;
						Arrays.fill(resultValue, null);

						line = text.toString();
						
						LineReaderUtil.fastSplit(resultValue, line, FIELD_SEPARATOR);
						int containerKey = Integer.parseInt(resultValue[0]);
						boolean containervalue = false;
						if ( null == resultValue[1]){
							if(skipNull) continue;
							else containervalue = skipDefaultvalue.equalsIgnoreCase("true") ? true : false;
						} else{
							containervalue = resultValue[1].equalsIgnoreCase("true") ? true : false;	
						}
						
						hasValue = true;
						nonrepeatableCell.add(containerKey, containervalue);
					}
					
					if ( hasValue ) {
						nonrepeatableCell.sort(new CellComparator.BooleanComparator<Integer>());
						finalData = nonrepeatableCell.toBytesOnSortedData();
					}
				}
				break;
				case 'c':
				{
					Cell2<Integer,Byte> nonrepeatableCell = new Cell2<Integer, Byte>
							(SortedBytesInteger.getInstance(),SortedBytesChar.getInstance());
					
					for (Text text : values) {
						if ( null == text) continue;
						Arrays.fill(resultValue, null);

						line = text.toString();
						
						LineReaderUtil.fastSplit(resultValue, line, FIELD_SEPARATOR);
						int containerKey = Integer.parseInt(resultValue[0]);
						byte containervalue;
						if ( null == resultValue[1]){
							if(skipNull) continue;
							else containervalue = skipDefaultvalue.getBytes()[0];
						} else{
							containervalue = resultValue[1].getBytes()[0];	
						}
						
						hasValue = true;
						nonrepeatableCell.add(containerKey, containervalue);
					}
					
					if ( hasValue ) {
						nonrepeatableCell.sort(new CellComparator.ByteComparator<Integer>());
						finalData = nonrepeatableCell.toBytesOnSortedData();
					}
				}
				break;
				default:
					break;
			}
				
            Put put = new Put(rowKey.getBytes());
            put.add(FAM_NAME,COL_NAME, finalData);
            context.write(null, put);
		}
    }
    
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
 
    	if(args.length < 3){
            System.out.println("Please enter valid number of arguments.");
            System.out.println("Usage : KVIndexer <<Input File Path>> <<XML File Configuration>> <<Destination Table>>");
            System.exit(1);
        }
    	
    	String inputFile = args[0];

    	if (null == inputFile || inputFile.trim().isEmpty()) {
            System.out.println("Please enter proper path");
            System.exit(1);
        }
 
		Configuration conf = HBaseConfiguration.create();
		conf.set(XML_FILE_PATH, args[1]);
		
        Job job = new Job(conf, "HSearch Key Value indexer");
        job.setJarByClass(IndexerMapReduce.class);
        job.setMapperClass(KVMapper.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(KVReducer.class);
 
        FileInputFormat.addInputPath(job, new Path(inputFile.trim()));
        TableMapReduceUtil.initTableReducerJob(args[2], KVReducer.class, job);
        job.setNumReduceTasks(10);
        job.waitForCompletion(true);
    }
}	

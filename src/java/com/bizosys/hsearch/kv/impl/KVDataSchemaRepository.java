package com.bizosys.hsearch.kv.impl;

import java.util.HashMap;
import java.util.Map;

import com.bizosys.hsearch.functions.GroupSortedObject;
import com.bizosys.hsearch.functions.GroupSortedObject.FieldType;
import com.bizosys.hsearch.kv.impl.FieldMapping.Field;

public class KVDataSchemaRepository {
	
	
	private static KVDataSchemaRepository colMapper = null; 
	public static KVDataSchemaRepository getInstance() {
		if ( null != colMapper) return colMapper;
		colMapper = new KVDataSchemaRepository();
		return colMapper;
	}
	
	public static class KVDataSchema {
		
		public Map<String, Integer> nameToSeqMapping = new HashMap<String, Integer>(); 
		public Map<Integer, String> seqToNameMapping = new HashMap<Integer, String>(); 
		public Map<String, FieldType> dataTypeMapping = new HashMap<String, FieldType>(); 
		
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

		public KVDataSchema(final FieldMapping fm) {
			Map<Integer,Field> seqFields = fm.fieldSeqs;
			String dataType = ""; 
			for (Integer seq : seqFields.keySet()) {
				Field fld = seqFields.get(seq);
				nameToSeqMapping.put(fld.name, fld.sourceSeq);
				seqToNameMapping.put(fld.sourceSeq, fld.name);
				
				dataType = fld.fieldType.toLowerCase();
				char dataTypeChar = dataTypesPrimitives.get(dataType);
				FieldType dataTypeField = GroupSortedObject.FieldType.STRING;
				switch (dataTypeChar) {
				case 't':
					dataTypeField = GroupSortedObject.FieldType.STRING;
					break;
				case 'i':
					dataTypeField = GroupSortedObject.FieldType.INTEGER;
					break;
				case 'f':
					dataTypeField = GroupSortedObject.FieldType.FLOAT;
					break;
				case 'd':
					dataTypeField = GroupSortedObject.FieldType.DOUBLE;
					break;
				case 'l':
					dataTypeField = GroupSortedObject.FieldType.LONG;
					break;
				case 's':
					dataTypeField = GroupSortedObject.FieldType.SHORT;
					break;
				case 'b':
					dataTypeField = GroupSortedObject.FieldType.BOOLEAN;
					break;
				case 'c':
					dataTypeField = GroupSortedObject.FieldType.BYTE;
					break;
				default:
					break;
				}
				
				dataTypeMapping.put(fld.name, dataTypeField);
			}
		}
	}
	
	Map<String, KVDataSchema> repositoryMap = new HashMap<String, KVDataSchemaRepository.KVDataSchema>();
	
	public final void add(final String repositoryName, final FieldMapping fm) {
		if(repositoryMap.containsKey(repositoryName))
			return;
		repositoryMap.put(repositoryName, new KVDataSchema(fm));
	}
	
	public final KVDataSchema get(final String repositoryName) {
		return repositoryMap.get(repositoryName);
	}
}

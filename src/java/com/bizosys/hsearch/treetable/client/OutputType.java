package com.bizosys.hsearch.treetable.client;

import java.io.IOException;

public class OutputType {
	public static final int OUTPUT_ID = 0;
	public static final int OUTPUT_IDVAL = 1;
	public static final int OUTPUT_VAL = 2;
	public static final int OUTPUT_COLS = 3;
	
	public static final int OUTPUT_COUNT = 4;
	public static final int OUTPUT_MIN = 5;
	public static final int OUTPUT_MAX = 6;
	public static final int OUTPUT_AVG = 7;

	public int typeCode = OUTPUT_ID;
	
	public OutputType() {
		
	}
	
	public OutputType(int typeCode) {
		this.typeCode = typeCode;
	}

	public OutputType(String code) throws IOException {
		char firstChar = code.charAt(0);
		switch (firstChar) {
			case '0':
				this.typeCode = OUTPUT_ID; break;
			case '1':
				this.typeCode = OUTPUT_IDVAL; break;
			case '2':
				this.typeCode = OUTPUT_VAL; break;
			case '3':
				this.typeCode = OUTPUT_COLS; break;
			case '4':
				this.typeCode = OUTPUT_COUNT; break;
			case '5':
				this.typeCode = OUTPUT_MIN; break;
			case '6':
				this.typeCode = OUTPUT_MAX; break;
			case '7':
				this.typeCode = OUTPUT_AVG; break;
			default:
				throw new IOException("Unknown result output type[" + firstChar + ']');
		}
		
	}

}

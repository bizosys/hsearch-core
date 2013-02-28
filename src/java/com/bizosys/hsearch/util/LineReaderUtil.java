package com.bizosys.hsearch.util;

public class LineReaderUtil {
	  
	public static final void fastSplit( final String[] result, final int[] positions, 
		final String text, final char separator) {
		  
		if (text == null) return;
		if (text.length() == 0) return;

		int index1 = 0;
		int index2 = text.indexOf(separator);

		int pos = -1;
		int resultSeq = 0;
		if ( index2 >= 0 ) {
			String token = null;
			while (index2 >= 0) {
				pos++;
				for ( int aPos: positions ) {
					if ( pos != aPos) continue;
					token = text.substring(index1, index2);
					result[resultSeq++] = token;
					break;
				}
				index1 = index2 + 1;
				index2 = text.indexOf(separator, index1);
				if ( index2 < 0 ) index1--;
			}
		            
			if (index1 < text.length() - 1) {
				pos++;
				for ( int aPos: positions ) {
					if ( pos != aPos) continue;
					result[resultSeq++] = text.substring(index1+1);
					break;
				}
			}
			  
		  } else {
			  pos++;
			  for ( int aPos: positions ) {
				  if ( pos != aPos) continue;
					result[resultSeq++] = text;
				  break;
			  }
		  }
	  }	  
	
	private StringBuilder appender = new StringBuilder(65536);
	
	public String append(final String[] cols, final char separator) {
		boolean isFirst = true;
		for (String col : cols) {
			if ( isFirst ) isFirst = false;
			else appender.append(separator);
			
			appender.append(col);
		}
		String value = appender.toString();
		appender.delete(0, 65535);
		return value;
		
	}
	  

}

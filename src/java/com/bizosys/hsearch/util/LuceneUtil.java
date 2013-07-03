package com.bizosys.hsearch.util;

public class LuceneUtil {

	  public static String escapeLuceneSpecialCharacters(String text)
	  {
		  if (null == text) return null;
		  if (text.trim().length() == 0 ) return "";
		  
		  StringBuilder sb = new StringBuilder(text.length());
		  for (char ch : text.toCharArray())
		  {
			  switch(ch)
			  {
			  	case '+': sb.append('\\').append('+'); break;
			  	case '-': sb.append('\\').append('-'); break;
			  	case '&': sb.append('\\').append('&');break;
			  	case '|': sb.append('\\').append('|');break;
			  	case '!': sb.append('\\').append('!');break;
			  	case '{': sb.append('\\').append('{');break;
			  	case '}': sb.append('\\').append('}');break;
			  	case '(': sb.append('\\').append('(');break;
			  	case ')': sb.append('\\').append(')');break;
			  	case '[': sb.append('\\').append('[');break;
			  	case ']': sb.append('\\').append(']');break;
			  	case '^': sb.append('\\').append('^');break;
			  	case '"': sb.append('\\').append('"');break;
			  	case '~': sb.append('\\').append('~');break;
			  	case '*': sb.append('\\').append('*');break;
			  	case '?': sb.append('\\').append('?');break;
			  	case ':': sb.append('\\').append(':');break;
			  	case '\\': sb.append('\\').append("\\"); break;
			  	default:
			  		if ((int)ch != 160) sb.append(ch);
			  }
		  }
		  return sb.toString();
	  }	
	  
	  public static String removeLuceneSpecialCharacters(String text)
	  {
		  if (null == text) return null;
		  if (text.trim().length() == 0 ) return "";
		  
		  StringBuilder sb = new StringBuilder(text.length());
		  for (char ch : text.toCharArray())
		  {
			  switch(ch)
			  {
			  	case '+': break;
			  	case '-': break;
			  	case '&': break;
			  	case '|': break;
			  	case '!': break;
			  	case '{': break;
			  	case '}': break;
			  	case '(': break;
			  	case ')': break;
			  	case '[': break;
			  	case ']': break;
			  	case '^': break;
			  	case '"': break;
			  	case '~': break;
			  	case '*': break;
			  	case '?': break;
			  	case ':': break;
			  	case '\\': break;
			  	default:
			  		if ((int)ch != 160) sb.append(ch);
			  }
		  }
		  return sb.toString();
	  }		  
}

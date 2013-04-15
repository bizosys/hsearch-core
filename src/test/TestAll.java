import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;
import junit.framework.TestFerrari;

import com.bizosys.hsearch.byteutils.ByteStringTest;
import com.bizosys.hsearch.byteutils.SortedBytesArrayTest;
import com.bizosys.hsearch.byteutils.SortedBytesBooleanTest;
import com.bizosys.hsearch.byteutils.SortedBytesCharTest;
import com.bizosys.hsearch.byteutils.SortedBytesDoubleTest;
import com.bizosys.hsearch.byteutils.SortedBytesFloatTest;
import com.bizosys.hsearch.byteutils.SortedBytesIntegerTest;
import com.bizosys.hsearch.byteutils.SortedBytesLongCompressedTest;
import com.bizosys.hsearch.byteutils.SortedBytesShortTest;
import com.bizosys.hsearch.byteutils.SortedBytesStringTest;
import com.bizosys.hsearch.byteutils.SortedBytesUnsignedShortTest;
import com.bizosys.hsearch.treetable.Cell2Test;
import com.bizosys.hsearch.treetable.Cell3Test;
import com.bizosys.hsearch.treetable.Cell4Test;
import com.bizosys.hsearch.treetable.Cell6Test;
import com.bizosys.hsearch.treetable.Cell7Test;
import com.bizosys.hsearch.treetable.SearchIndexTest;


public class TestAll {
	public static void main(String[] args) throws Exception {
		List<TestCase>  testcases = new ArrayList<TestCase>();
		testcases.add(new ByteStringTest());
		testcases.add( new SortedBytesBooleanTest() );
		testcases.add( new SortedBytesCharTest() );
		testcases.add( new SortedBytesShortTest() );
		testcases.add( new SortedBytesUnsignedShortTest() );
		testcases.add( new SortedBytesFloatTest() );
		testcases.add( new SortedBytesIntegerTest() );
		testcases.add( new SortedBytesLongCompressedTest() );
		testcases.add( new SortedBytesDoubleTest() );
		testcases.add( new SortedBytesArrayTest() );
		testcases.add( new SortedBytesStringTest() );
		
		testcases.add( new Cell2Test() );
		testcases.add( new Cell3Test() );
		testcases.add( new Cell4Test() );
		testcases.add( new Cell6Test() );
		testcases.add( new Cell7Test() );
		testcases.add( new SearchIndexTest() );
		
		int failures = 0;
		for (TestCase t : testcases) {
			failures += TestFerrari.testRandom(t).getFailures();
			//TestFerrari.testMemory(t);
			//TestFerrari.testMaxMin(t);
			//TestFerrari.testResponse(t);
		}
		System.out.println("Total Failures = " + failures);
	}
}

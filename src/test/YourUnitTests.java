package test;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.junit.Before;
import org.junit.Test;

import hw1.AggregateOperator;
import hw1.Catalog;
import hw1.Database;
import hw1.HeapFile;
import hw1.HeapPage;
import hw1.IntField;
import hw1.Query;
import hw1.Relation;
import hw1.StringField;
import hw1.Tuple;
import hw1.TupleDesc;
import hw1.Type;

public class YourUnitTests {
	
	private HeapFile hf;
	private TupleDesc td;
	private Catalog c;
	private HeapPage hp;
	private HeapFile ahf;
	private TupleDesc atd;

	@Before
	public void setup() {
		try {
			Files.copy(new File("testfiles/test.dat.bak").toPath(), new File("testfiles/test.dat").toPath(), StandardCopyOption.REPLACE_EXISTING);
			Files.copy(new File("testfiles/A.dat.bak").toPath(), new File("testfiles/A.dat").toPath(), StandardCopyOption.REPLACE_EXISTING);
		} catch (IOException e) {
			System.out.println("unable to copy files");
			e.printStackTrace();
		}
		
		c = Database.getCatalog();
		c.loadSchema("testfiles/test.txt");
		
		c = Database.getCatalog();
		c.loadSchema("testfiles/A.txt");

		int tableId = c.getTableId("A");
		atd = c.getTupleDesc(tableId);
		ahf = c.getDbFile(tableId);
	}
	
	@Test
	public void testQuerySelectAllWithWhere() {
		Query q = new Query("SELECT * FROM test WHERE c1 = 530");
		Relation r = q.execute();
		
		assertTrue(r.getTuples().size() == 1);
		assertTrue(r.getDesc().getSize() == 133);
	}
	
//	@Test
//	public void testDifferentAggregateFunctions() {
//		Query q = new Query("SELECT MAX(c1), SUM(c2) FROM test");
//		Relation r = q.execute();
//		
//		assertTrue(r.getTuples().size() == 1);
//		IntField agg = (IntField) (r.getTuples().get(0).getField(0));
//		assertTrue(agg.getValue() == 36);
//	}
	
	@Test
	public void testRelationDifferentAggregateFunction() {
		Relation ar = new Relation(ahf.getAllTuples(), atd);
		ArrayList<Integer> c = new ArrayList<Integer>();
		c.add(1);
		ar = ar.project(c);
		ar = ar.aggregate(AggregateOperator.MAX, false);
		
		
		assertTrue(ar.getTuples().size() == 1);
		IntField agg = (IntField)(ar.getTuples().get(0).getField(0));
		assertTrue(agg.getValue() == 8);
	}

}

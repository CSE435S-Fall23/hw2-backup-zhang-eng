package hw1;

import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * This class represents a tuple that will contain a single row's worth of information
 * from a table. It also includes information about where it is stored
 * @author Sam Madden modified by Doug Shook
 *
 */
public class Tuple {
	private TupleDesc t;
	private Field[] values;
	private int pid;
	private int id;
	
	
	/**
	 * Creates a new tuple with the given description
	 * @param t the schema for this tuple
	 */
	public Tuple(TupleDesc t) {
		this.t = t;
		values = new Field[t.numFields()];
		pid = 0;
		id = 0;
	}

	public TupleDesc getDesc() {
		return t;
	}
	
	/**
	 * retrieves the page id where this tuple is stored
	 * @return the page id of this tuple
	 */
	public int getPid() {
		return this.pid;
	}

	public void setPid(int pid) {
		this.pid = pid;
	}

	/**
	 * retrieves the tuple (slot) id of this tuple
	 * @return the slot where this tuple is stored
	 */
	public int getId() {
		return this.id;
	}

	public void setId(int id) {
		this.id = id;
	}
	
	public void setDesc(TupleDesc td) {
		t = td;
	}
	
	/**
	 * Stores the given data at the i-th field
	 * @param i the field number to store the data
	 * @param v the data
	 */
	public void setField(int i, Field v) {
		System.out.print(this);
		values[i] = v;
	}
	
	public Field getField(int i) {
		return values[i];
	}
	
	/**
	 * Creates a string representation of this tuple that displays its contents.
	 * You should convert the binary data into a readable format (i.e. display the ints in base-10 and convert
	 * the String columns to readable text).
	 */
	public String toString() {
		StringBuilder sb = new StringBuilder();
		for(int i = 0; i < values.length; i++) {
			sb.append(t.getFieldName(i)).append(": ").append(values[i]);
		}
		return sb.toString();
	}
}
	
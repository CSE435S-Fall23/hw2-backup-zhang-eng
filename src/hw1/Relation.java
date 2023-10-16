package hw1;

import java.util.ArrayList;

/**
 * This class provides methods to perform relational algebra operations. It will be used
 * to implement SQL queries.
 * @author Doug Shook
 *
 */
public class Relation {

	private ArrayList<Tuple> tuples;
	private TupleDesc td;
	
	public Relation(ArrayList<Tuple> l, TupleDesc td) {
		this.tuples = l;
		this.td = td;
	}
	
	/**
	 * This method performs a select operation on a relation
	 * @param field number (refer to TupleDesc) of the field to be compared, left side of comparison
	 * @param op the comparison operator
	 * @param operand a constant to be compared against the given column
	 * @return
	 */
	public Relation select(int field, RelationalOperator op, Field operand) {
		ArrayList<Tuple> selectedValues = new ArrayList<Tuple>();
		for(int i = 0; i < this.tuples.size(); i++) {
			Tuple curValue = this.tuples.get(i);
			if(curValue.getField(field).compare(op, operand)) {
				selectedValues.add(curValue);
			}
		}
		
//		this.tuples = selectedValues;
		return new Relation(selectedValues, this.td);
	}
	
	/**
	 * This method performs a rename operation on a relation
	 * @param fields the field numbers (refer to TupleDesc) of the fields to be renamed
	 * @param names a list of new names. The order of these names is the same as the order of field numbers in the field list
	 * @return
	 */
	public Relation rename(ArrayList<Integer> fields, ArrayList<String> names) {
		int size = this.td.numFields();
		Type[] copyTypes = new Type[size];
		String[] copyFields = new String[size];
		
		for(int i = 0; i < size; i++) {
			copyTypes[i] = this.td.getType(i);
			copyFields[i] = this.td.getFieldName(i);
		}
		
		for(int i = 0; i < fields.size(); i++) {
			int curField = fields.get(i);
			String curName = names.get(i);
			copyFields[curField] = curName;
		}
		
//		this.td =  new TupleDesc(copyTypes, copyFields);
		return new Relation(this.tuples, new TupleDesc(copyTypes, copyFields));
		
	}
	
	/**
	 * This method performs a project operation on a relation
	 * @param fields a list of field numbers (refer to TupleDesc) that should be in the result
	 * @return
	 */
	public Relation project(ArrayList<Integer> fields) {
		int numFields = fields.size();
		
		Type[] keptTypes = new Type[numFields];
		String[] keptFields = new String[numFields];
		
		for(int i = 0; i < fields.size(); i++) {
			int keptIdx = fields.get(i);
			
			keptTypes[i] = this.td.getType(keptIdx);
			keptFields[i] = this.td.getFieldName(keptIdx);
		}
		
		TupleDesc keptDesc = new TupleDesc(keptTypes, keptFields);
		ArrayList<Tuple> keptTuples = new ArrayList<Tuple>();
		
		for(int j = 0; j < this.tuples.size(); j++) {
			Tuple cur = this.tuples.get(j);
			Tuple keptTuple = new Tuple(keptDesc);
			for(int k = 0; k < fields.size(); k++) {
				int keptIdx = fields.get(k);
				keptTuple.setField(k, cur.getField(keptIdx));
			}
			keptTuples.add(keptTuple);
			
		}
		
//		this.td = keptDesc;
//		this.tuples = keptTuples;
		
		return new Relation(keptTuples, keptDesc);
	}
	
	/**
	 * This method performs a join between this relation and a second relation.
	 * The resulting relation will contain all of the columns from both of the given relations,
	 * joined using the equality operator (=)
	 * @param other the relation to be joined
	 * @param field1 the field number (refer to TupleDesc) from this relation to be used in the join condition
	 * @param field2 the field number (refer to TupleDesc) from other to be used in the join condition
	 * @return
	 */
	public Relation join(Relation other, int field1, int field2) {
		//Figure out which values in field1 and field2 are the same, keep these tuples
		//Create new TupleDesc with combined types + fields
		//Create new tuple list where null value for other relation's fields
		
		int i = 0;
		ArrayList<Tuple> relation1 = new ArrayList<Tuple>();
		ArrayList<Tuple> relation2 = new ArrayList<Tuple>();
//		while(i < this.tuples.size() && i < other.getTuples().size()) {
//			if(this.tuples.get(i).get)
//			
//			i++;
//		}
		
		
	}
	
	/**
	 * Performs an aggregation operation on a relation. See the lab write up for details.
	 * @param op the aggregation operation to be performed
	 * @param groupBy whether or not a grouping should be performed
	 * @return
	 */
	public Relation aggregate(AggregateOperator op, boolean groupBy) {
		//your code here
		return null;
	}
	
	public TupleDesc getDesc() {
		return this.td;
	}
	
	public void setDesc(TupleDesc td) {
		this.td = td;
	}
	
	public ArrayList<Tuple> getTuples() {
		return this.tuples;
	}
	
	public void setTuples(ArrayList<Tuple> tuples) {
		this.tuples = tuples;
	}
	
	/**
	 * Returns a string representation of this relation. The string representation should
	 * first contain the TupleDesc, followed by each of the tuples in this relation
	 */
	public String toString() {
		//your code here
		return null;
	}
}


package hw1;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

/**
 * A class to perform various aggregations, by accepting one tuple at a time
 * @author Doug Shook
 *
 */
public class Aggregator {
	final int FIRST_VAL = 0;
	final int SECOND_VAL = 1;
	/*
	 * with GROUP BY: assume that the relation being aggregated has a single column
	 * without GROUP BY: assume that the relation will have exactly two columns
	 * 		1. Column containing the groups
	 * 		2. Column containing the data to be aggregated
	 */
	
	private AggregateOperator o;
	private boolean groupBy;
	private TupleDesc td;
	
	private ArrayList<Tuple> tuples;
	private HashMap<Field, ArrayList<Tuple>> groupedTuples;
	
	public Aggregator(AggregateOperator o, boolean groupBy, TupleDesc td) {
		this.o = o;
		this.groupBy = groupBy;
		this.td = td;
		
		this.groupedTuples = new HashMap<Field, ArrayList<Tuple>>();
		this.tuples = new ArrayList<Tuple>();
	}

	/**
	 * Merges the given tuple into the current aggregation
	 * @param t the tuple to be aggregated
	 */
	public void merge(Tuple t) {
		Field key = t.getField(FIRST_VAL);
		if(groupBy) {
			if(!this.groupedTuples.containsKey(key)) {
				this.groupedTuples.put(key, new ArrayList<Tuple>());
			}
			this.groupedTuples.get(key).add(t);

		}
		else {
			tuples.add(t);
		}
		
	}
	
	/**
	 * Returns the result of the aggregation
	 * @return a list containing the tuples after aggregation
	 */
	public ArrayList<Tuple> getResults() {
		ArrayList<Tuple> res = new ArrayList<Tuple>();
		if(groupBy) {
			if(this.groupedTuples.size() < 1) {
				return null;
			}
			switch(this.o) {
			case MIN:
				for(HashMap.Entry<Field, ArrayList<Tuple>> entry : groupedTuples.entrySet()) {
					Tuple minTuple = new Tuple(this.td);
					minTuple.setField(SECOND_VAL, this.minTuple(entry.getValue(), SECOND_VAL));
					res.add(minTuple);
				}
				return res;
				
			case MAX:
				for(HashMap.Entry<Field, ArrayList<Tuple>> entry : groupedTuples.entrySet()) {
					Tuple maxTuple = new Tuple(this.td);
					maxTuple.setField(SECOND_VAL, this.maxTuple(entry.getValue(), SECOND_VAL));
					res.add(maxTuple);
				}
				return res;
				
			case COUNT:
				for(HashMap.Entry<Field, ArrayList<Tuple>> entry : groupedTuples.entrySet()) {
					Tuple countTuple = new Tuple(this.td);
					countTuple.setField(SECOND_VAL, new IntField(this.countTuples(entry.getValue())));
					res.add(countTuple);
				}
				return res;
				
			case SUM:
				for(HashMap.Entry<Field, ArrayList<Tuple>> entry : groupedTuples.entrySet()) {
					Tuple sumTuple = new Tuple(this.td);
					sumTuple.setField(SECOND_VAL, new IntField(this.sumTuples(entry.getValue(), SECOND_VAL)));
					res.add(sumTuple);
				}
				return res;
				
			case AVG:
				for(HashMap.Entry<Field, ArrayList<Tuple>> entry : groupedTuples.entrySet()) {
					Tuple avgTuple = new Tuple(this.td);
					avgTuple.setField(SECOND_VAL, new IntField(this.avgTuples(entry.getValue(), SECOND_VAL)));
					res.add(avgTuple);
				}
				return res;
				
			default:
				throw new UnsupportedOperationException("Aggregate Functions only");
			}
		}
		else {
			if(this.tuples.size() < 1) {
				return null;
			}
			
			Tuple resVal = new Tuple(this.td);
			switch(this.o) {
			case MIN:
				resVal.setField(FIRST_VAL, this.minTuple(this.tuples, FIRST_VAL));
				break;
				
			case MAX:
				resVal.setField(FIRST_VAL, this.maxTuple(this.tuples, FIRST_VAL));
				break;
				
			case COUNT:
				resVal.setField(FIRST_VAL, new IntField(this.countTuples(this.tuples)));
				break;
				
			case SUM:
				resVal.setField(FIRST_VAL, new IntField(this.sumTuples(this.tuples, FIRST_VAL)));
				break;
				
			case AVG:
				resVal.setField(FIRST_VAL, new IntField(this.avgTuples(this.tuples, FIRST_VAL)));
				break;
			
			default:
				throw new UnsupportedOperationException("Aggregate Functions only");
			}
			res.add(resVal);
		}
		return res;
	}
	
	// Custom helper methods
	
	/**
	 * Finds the minimum value
	 * @param list of values
	 * @return minimum value
	 */
	private Field minTuple(ArrayList<Tuple> values, int fieldNum) {
		Field min = this.tuples.get(fieldNum).getField(fieldNum);
		for(int i = 1; i < this.tuples.size(); i++) {
			Field cur = this.tuples.get(i).getField(fieldNum);
			if(cur.compare(RelationalOperator.LT, min)) {
				min = cur;
			}
		}
		return min;
	}
	
	/**
	 * Finds the maximum value
	 * @param list of values
	 * @return maximum value
	 */
	private Field maxTuple(ArrayList<Tuple> values, int fieldNum) {
		Field max = this.tuples.get(FIRST_VAL).getField(fieldNum);
		for(int i = 1; i < this.tuples.size(); i++) {
			Field cur = this.tuples.get(i).getField(fieldNum);
			if(cur.compare(RelationalOperator.GT, max)) {
				max = cur;
			}
		}
		
		return max;
	}
	
	
	/**
	 * Sums up the given tuples
	 * @param list of tuples
	 * @return sum
	 */
	private Integer sumTuples(ArrayList<Tuple> values, int fieldNum) {
		if(this.td.getType(FIRST_VAL) == Type.STRING) {
			return null;
		}
		
		int sum = 0;
		for(int i = 0; i < values.size(); i++) {
			sum += values.get(i).getField(fieldNum).hashCode();
			
		}
		
		return sum;
	}
	
	/**
	 * Counts the total number of tuples
	 * @param list of tuples
	 * @return total
	 */
	private Integer countTuples(ArrayList<Tuple> values) {
		return values.size();
	}
	
	/**
	 * Averages given tuples
	 * @param list of tuples
	 * @return average
	 */
	private Integer avgTuples(ArrayList<Tuple> values, int fieldNum) {
		if(sumTuples(values, fieldNum) == null) {
			return null;
		}
		return sumTuples(values, fieldNum) / countTuples(values);
	}
}

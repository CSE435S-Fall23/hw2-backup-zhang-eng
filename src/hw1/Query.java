package hw1;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.parser.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.*;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectItem;

public class Query {

	private String q;
	
	public Query(String q) {
		this.q = q;
	}
	
	public Relation execute()  {
		Statement statement = null;
		try {
			statement = CCJSqlParserUtil.parse(q);
		} catch (JSQLParserException e) {
			System.out.println("Unable to parse query");
			e.printStackTrace();
		}
		Select selectStatement = (Select) statement;
		PlainSelect sb = (PlainSelect)selectStatement.getSelectBody();
		
		
		//your code here
		
		System.out.println("\n\nQUERY: " + sb);
		
		// parse all fields
		List<SelectItem> selects = sb.getSelectItems();
		FromItem froms = sb.getFromItem();
		List<Join> joins = sb.getJoins();
		Expression wheres = sb.getWhere();
		List<Expression> groups = sb.getGroupByColumnReferences();
		
		boolean noJoins, noWheres, noGroups;
		if (joins == null) {
			noJoins = true;
		}
		else {
			noJoins = false;
		}
		if (wheres == null) {
			noWheres = true;
		}
		else {
			noWheres = false;
		}
		if (groups == null) {
			noGroups = true;
		}
		else {
			noGroups = false;
		}
		
		// initialize relation
		Catalog c = Database.getCatalog();
		int table = c.getTableId(froms.toString());
		ArrayList<Tuple> tuples = c.getDbFile(table).getAllTuples();
		TupleDesc td = tuples.get(0).getDesc();
		Relation rel = new Relation(tuples, td);
		ArrayList<Integer> results = new ArrayList<>();
		
		// deal with select * query
		if (selects.get(0).toString().equals("*")) {
			// deal with where
			if (! noWheres) {
				WhereExpressionVisitor whereExp = new WhereExpressionVisitor();
				wheres.accept(whereExp);
				rel = rel.select(td.nameToId(whereExp.getLeft()), whereExp.getOp(), whereExp.getRight());
			}
			return rel;
		}
		
		// deal with joins
		if (! noJoins) {
			// execute join
			for (Join j : joins) {
				
				System.out.println("\nJOIN: "+j);
				
				// get fields from 2nd
				FromItem from2 = j.getRightItem();
				Catalog c2 = Database.getCatalog();
				int table2 = c2.getTableId(from2.toString());
				ArrayList<Tuple> tuples2 = c2.getDbFile(table2).getAllTuples();
				TupleDesc td2 = tuples2.get(0).getDesc();
				Relation r2 = new Relation(tuples2, td2);
				int field1 = 0;
				int field2 = 0;
				
				String myS[] = j.getOnExpression().toString().split(" = ");
				String myS2[] = myS[0].split("\\.");
				String myS3[] = myS[1].split("\\.");
				
				if (c.getTableId(myS2[0].trim()) != table) {
					for (int i = 0; i < td2.numFields(); i++) {
						String name = td2.getFieldName(i);
						if(name.equals(myS2[1].trim())) {
							field2 = i;
						}
					}
					
					for (int i = 0; i < td.numFields(); i++) {
						String name = td.getFieldName(i);
						if(name.equals(myS3[1].trim())) {
							field1 = i;
						}
					}
				}
				else {
					for (int i = 0; i < td.numFields(); i++) {
						String name = td.getFieldName(i);
						if (name.equalsIgnoreCase(myS2[1].trim())) {
							field1 = i;
						}
					}
					
					for (int i = 0; i < td2.numFields(); i++) {
						String name = td2.getFieldName(i);
						if(name.equalsIgnoreCase(myS3[1].trim())) {
							field2 = i;
						}
					}
				}
				rel = rel.join(r2, field1, field2);
				
			}
		}
//		System.out.println("\n!!RELATION: "+rel);
//		System.out.println("\n!!PROJECTS: "+results);

		// go through select items
		for (SelectItem s : selects) {
			ColumnVisitor col = new ColumnVisitor();
			s.accept(col);
			
			// deal with aggregate
			if (col.isAggregate()) {
				AggregateOperator operator = col.getOp();
				
				if (noGroups) {
					rel = rel.aggregate(operator, false);
					results = new ArrayList<>();
				}
				else { // deal with groups
					rel = rel.aggregate(operator, true);
				}
			}
			else {
				results.add(td.nameToId(col.getColumn()));
			}
		}
		
		System.out.println("\n!!RELATION: "+rel);
		System.out.println("\n!!PROJECTS: "+results);

		rel = rel.project(results);
		
		// deal with wheres
		if (! noWheres) {
			WhereExpressionVisitor whereExp = new WhereExpressionVisitor();
			wheres.accept(whereExp);
			rel = rel.select(td.nameToId(whereExp.getLeft()), whereExp.getOp(), whereExp.getRight());
		}
		
		return rel;
	}
}

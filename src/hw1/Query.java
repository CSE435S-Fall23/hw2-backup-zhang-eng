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
		
		List<SelectItem> selects = sb.getSelectItems();
		FromItem froms = sb.getFromItem();
		List<Join> joins = sb.getJoins();
		Expression wheres = sb.getWhere();
		
		Catalog c = Database.getCatalog();
		int table = c.getTableId(froms.toString());
		ArrayList<Tuple> tuples = c.getDbFile(table).getAllTuples();
		
		TupleDesc td = tuples.get(0).getDesc();
		Relation rel = new Relation(tuples, td);
		
		System.out.println("\nselect: "+selects);
		System.out.println("\nfrom: "+froms);
		System.out.println("\njoin: "+joins);
		System.out.println("\nwhere: "+wheres);
		
		// select * query
		if (selects.get(0).toString().equals("*")) {
			if (wheres == null) {
				return rel;
			}
			else {
				WhereExpressionVisitor whereExp = new WhereExpressionVisitor();
				rel = rel.select(td.nameToId(whereExp.getLeft()), whereExp.getOp(), whereExp.getRight());
				return rel;
			}
		}
		
		ArrayList<Integer> projects = new ArrayList<>();
		
		for(SelectItem s : selects) {
			
			
			ColumnVisitor col = new ColumnVisitor();
			s.accept(col);
			
			if (col.isAggregate()) {
				// fix!
				System.out.println("\n!!RELATION: "+s);

				if(sb.getGroupByColumnReferences() != null) {
					rel = rel.aggregate(col.getOp(), true);
				}
				else {
					rel = rel.aggregate(col.getOp(), false);
					projects = new ArrayList<>();
				}
			}
			else {
				projects.add(td.nameToId(col.getColumn()));
			}
		}
		
		rel = rel.project(projects);
		
		
		if(wheres != null) {
			WhereExpressionVisitor whereExp = new WhereExpressionVisitor();
			wheres.accept(whereExp);
			rel = rel.select(td.nameToId(whereExp.getLeft()), whereExp.getOp(), whereExp.getRight());
		}

		return rel;
		
	}
}

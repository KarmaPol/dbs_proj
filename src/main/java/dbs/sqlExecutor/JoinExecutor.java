package dbs.sqlExecutor;

import java.util.LinkedList;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;

public class JoinExecutor {
	public void joinRecords(Statement input) {
		if (input instanceof Select) {
			Select selectStatement = (Select)input;
			PlainSelect plainSelect = (PlainSelect)selectStatement.getSelectBody();

			System.out.println("FROM Table: " + plainSelect.getFromItem().toString());

			Join join = plainSelect.getJoins().get(0);
			Expression onExpression = join.getOnExpression();
			System.out.println("JOIN Table: " + join.getFromItem().toString());
			LinkedList<?> onExpressions = (LinkedList<?>)join.getOnExpressions();
			EqualsTo equalsTo = (EqualsTo)onExpressions.get(0);
			String left = equalsTo.getLeftExpression().toString();
			String right = equalsTo.getRightExpression().toString();

		}
	}
}

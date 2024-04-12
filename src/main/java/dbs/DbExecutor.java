package dbs;

import java.util.Scanner;

import net.sf.jsqlparser.JSQLParserException;

import dbs.sqlExecutor.DeleteExecutor;
import dbs.sqlExecutor.InsertExecutor;
import dbs.sqlExecutor.SelectExecutor;
import dbs.sqlExecutor.TableCreator;
import dbs.sqlParser.SqlParser;

public class DbExecutor {

	public static void main(String[] args) throws JSQLParserException {
		TableCreator tableCreator = new TableCreator();
		InsertExecutor insertExecutor = new InsertExecutor();
		SelectExecutor selectExecutor = new SelectExecutor();
		DeleteExecutor deleteExecutor = new DeleteExecutor();
		SqlParser sqlParser = new SqlParser(tableCreator, insertExecutor, selectExecutor, deleteExecutor);

		Scanner sc = new Scanner(System.in);
		while(true) {
			String input = sc.nextLine();
			if(!sqlParser.parse(input)) break;
		}
	}
}

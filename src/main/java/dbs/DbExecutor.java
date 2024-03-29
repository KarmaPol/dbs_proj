package dbs;

import java.util.Scanner;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.PlainSelect;

import dbs.queryExecutor.InsertExecutor;
import dbs.queryExecutor.SelectExecutor;
import dbs.queryExecutor.TableCreator;

public class DbExecutor {
	private static final String CREATE_PATTERN = "^CREATE TABLE [a-zA-Z0-9_]+ \\((.+\s+.+,*\s*)+(PRIMARY KEY\\s*\\([a-zA-Z0-9_]+\\))\\);$"; // CREATE TABLE 테이블명 (컬럼명1 자료형1, 컬럼명2 자료형2, ... PRIMARY KEY (컬럼명))
	private static final String SELECT_PATTERN = "^SELECT\\s+.+\\s+FROM\\s+[a-zA-Z0-9_]+(\\s+WHERE\\s+.+)?;$"; // SELECT 컬럼명1, 컬럼명2, ... FROM 테이블명 WHERE 조건
	private static final String INSERT_PATTERN = "^INSERT INTO [a-zA-Z0-9_]+ \\(([a-zA-Z0-9_]+, )*([a-zA-Z0-9_]+)\\) VALUES \\((.+)\\);$"; // INSERT INTO 테이블명 (컬럼명1, 컬럼명2, ...) VALUES (값1, 값2, ...)
	private static final String EXIT_PATTERN = "EXIT;";

	public static void main(String[] args) throws JSQLParserException {
		TableCreator tableCreator = new TableCreator();
		InsertExecutor insertExecutor = new InsertExecutor();
		SelectExecutor selectExecutor = new SelectExecutor();

		Scanner sc = new Scanner(System.in);
		while(true) {
			String input = sc.nextLine();

			if(input.equals(EXIT_PATTERN)) {
				System.out.println("Exiting...");
				break;
			}

			if(input.matches(CREATE_PATTERN)) {
				CreateTable sql = (CreateTable)CCJSqlParserUtil.parse(input);
				tableCreator.createFile(sql);
			}
			else if(input.matches(SELECT_PATTERN)) {
				PlainSelect select = (PlainSelect)CCJSqlParserUtil.parse(input);
				selectExecutor.selectRecords(select);
			}
			else if(input.matches(INSERT_PATTERN)) {
				Insert sql = (Insert)CCJSqlParserUtil.parse(input);
				insertExecutor.insertRecord(sql);
			}
			else {
				System.out.println("Invalid query");
			}
		}
	}
}

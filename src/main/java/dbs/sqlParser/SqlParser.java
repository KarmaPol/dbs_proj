package dbs.sqlParser;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.PlainSelect;

import dbs.sqlExecutor.DeleteExecutor;
import dbs.sqlExecutor.InsertExecutor;
import dbs.sqlExecutor.SelectExecutor;
import dbs.sqlExecutor.TableCreator;

public class SqlParser {
	private static final String CREATE_PATTERN = "^CREATE TABLE [a-zA-Z0-9_]+ \\((.+\s+.+,*\s*)+(PRIMARY KEY\\s*\\([a-zA-Z0-9_]+\\))\\);$"; // CREATE TABLE 테이블명 (컬럼명1 자료형1, 컬럼명2 자료형2, ... PRIMARY KEY (컬럼명))
	private static final String SELECT_PATTERN = "^SELECT\\s+.+\\s+FROM\\s+[a-zA-Z0-9_]+(\\s+WHERE\\s+.+)?;$"; // SELECT 컬럼명1, 컬럼명2, ... FROM 테이블명 WHERE 조건
	private static final String INSERT_PATTERN = "^INSERT INTO [a-zA-Z0-9_]+ \\(([a-zA-Z0-9_]+, )*([a-zA-Z0-9_]+)\\) VALUES \\((.+)\\);$"; // INSERT INTO 테이블명 (컬럼명1, 컬럼명2, ...) VALUES (값1, 값2, ...)
	private static final String DELETE_PATTERN = "^DELETE FROM [a-zA-Z0-9_]+ WHERE [a-zA-Z0-9_]+ = [^;]+;$"; // DELETE FROM 테이블명 WHERE 컬럼명 = 값;
	private static final String EXIT_PATTERN = "EXIT;";

	private final TableCreator tableCreator;
	private final InsertExecutor insertExecutor;
	private final SelectExecutor selectExecutor;
	private final DeleteExecutor deleteExecutor;

	public SqlParser(TableCreator tableCreator, InsertExecutor insertExecutor, SelectExecutor selectExecutor, DeleteExecutor deleteExecutor) {
		this.tableCreator = tableCreator;
		this.insertExecutor = insertExecutor;
		this.selectExecutor = selectExecutor;
		this.deleteExecutor = deleteExecutor;
	}

	public boolean parse(String input) throws JSQLParserException {
		if(input.equals(EXIT_PATTERN)) {
			System.out.println("Exiting...");
			return false;
		}

		if(input.matches(CREATE_PATTERN)) {
			CreateTable sql = (CreateTable)CCJSqlParserUtil.parse(input);
			tableCreator.createFile(sql);
		}
		else if(input.matches(SELECT_PATTERN)) {
			PlainSelect sql = (PlainSelect)CCJSqlParserUtil.parse(input);
			selectExecutor.selectRecords(sql);
		}
		else if(input.matches(INSERT_PATTERN)) {
			Insert sql = (Insert)CCJSqlParserUtil.parse(input);
			insertExecutor.insertRecord(sql);
		}
		else if(input.matches(DELETE_PATTERN)) {
			Delete sql = (Delete)CCJSqlParserUtil.parse(input);
			deleteExecutor.deleteRecord(sql);
		}
		else {
			System.out.println("Invalid query");
		}

		return true;
	}
}

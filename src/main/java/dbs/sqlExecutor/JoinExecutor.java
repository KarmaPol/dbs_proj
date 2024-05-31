package dbs.sqlExecutor;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;

import dbs.fileManager.RecordReader;
import dbs.fileManager.RecordWriter;
import dbs.metadataManager.MetadataHandler;
import dbs.metadataManager.vo.AttributeMetadataVO;
import dbs.metadataManager.vo.TableMetadataVO;

public class JoinExecutor {
	private static final int MODULUS = 3;

	public void joinRecords(Statement input) {
		Select selectStatement = (Select)input;
		PlainSelect plainSelect = (PlainSelect)selectStatement.getSelectBody();
		String fromTable = plainSelect.getFromItem().toString();
		Join join = plainSelect.getJoins().get(0);
		Expression onExpression = join.getOnExpression();
		String joinTable = join.getFromItem().toString();
		LinkedList<?> onExpressions = (LinkedList<?>)join.getOnExpressions();
		EqualsTo equalsTo = (EqualsTo)onExpressions.get(0);
		String left = equalsTo.getLeftExpression().toString();
		String right = equalsTo.getRightExpression().toString();

		List<Map<String, String>> fromRecords = readTables(fromTable);
		List<Map<String, String>> joinRecords = readTables(joinTable);

		createPartitionFile(fromRecords, left, fromTable);
	}

	private void createPartitionFile(List<Map<String, String>> records, String key, String tableName) {
		Map<Integer, BufferedWriter> writers = new HashMap<>();
		File file;
		AtomicInteger columnIdx = new AtomicInteger();

		try {
			for (int i = 0; i < MODULUS; i++) {
				file = new File(tableName + "_partition_" + i + ".txt");
				isFileExists(file);
				writers.put(i, new BufferedWriter(new FileWriter(file, true)));
			}

			for (Map<String, String> record : records) {
				String keyValue = record.get(key);
				int modValue = getModValue(keyValue, MODULUS);

				BufferedWriter writer = writers.get(modValue);

				List<String> columnNames = new ArrayList<>(record.keySet());
				List<String> columnValues = new ArrayList<>(record.values());

				TableMetadataVO tableMetaData = MetadataHandler.getTableMetaData(tableName);
				Map<String, AttributeMetadataVO> attributeMetadataVOMap = MetadataHandler.getAttributeMetadata(tableName);

				List<String> recordList = new ArrayList<>(Collections.nCopies(columnNames.size(), null));
				StringBuilder recordMaker = new StringBuilder();

				for (int i = 0; i < columnNames.size(); i++) {
					String columnName = columnNames.get(i);
					String columnValue = columnValues.get(i);

					AttributeMetadataVO currentAttributeMetadata = attributeMetadataVOMap.get(columnName);

					int columnSize = currentAttributeMetadata.size();
					String columnType = currentAttributeMetadata.type();
					int columnIndex = currentAttributeMetadata.columnIdx();

					String emptyRecord = new String(new char[columnSize]);
					String currentValue = (columnValue + emptyRecord).substring(0, columnSize);
					recordList.set(columnIndex, currentValue);
					recordMaker.append(currentValue);
				}

				String currentRecord = recordMaker.toString();
				RecordWriter.writeRecord(currentRecord, tableMetaData);

				writer.write(currentRecord);
				writer.newLine();
			}

			for (BufferedWriter writer : writers.values()) {
				writer.close();
			}
		} catch (IOException e) {
			System.out.println("Error creating file: " + e.getMessage());
		}
	}

	private static void isFileExists(File file) throws IOException {
		if (!file.exists()) {
			file.createNewFile();
		}
	}

	private static int getModValue(String key, int modulus) {
		int intValue = Integer.parseInt(key);
		return intValue % modulus;
	}

	private List<Map<String, String>> readTables(String tableName) {
		TableMetadataVO tableMetaData = MetadataHandler.getTableMetaData(tableName);
		Map<String, AttributeMetadataVO> attributeMetadataVOMap = MetadataHandler.getAttributeMetadata(tableName);
		List<AttributeMetadataVO> attributeMetadatas = attributeMetadataVOMap.values().stream().toList();

		List<AttributeMetadataVO> sortedAttributeMetadatas = attributeMetadatas.stream()
			.sorted(Comparator.comparingInt(AttributeMetadataVO::columnIdx))
			.collect(Collectors.toList());

		List<String> records = RecordReader.readFile(tableMetaData);
		List<Map<String, String>> answer = new LinkedList<>();

		for(String record : records) {
			Map<String, String> map = new HashMap<>();
			if(isDeleted(record)) {
				continue;
			}

			int idx = 0;
			for (AttributeMetadataVO attributeMetadata : sortedAttributeMetadatas) {
				int size = attributeMetadata.size();
				String substring = record.substring(idx, idx + size);
				substring = removeNullChars(substring);
				idx += size;
				map.put(attributeMetadata.name(), substring);
			}
			answer.add(map);
		}
		return answer;
	}
	private boolean isDeleted(String record) {
		return record.startsWith(":free");
	}
	private String removeNullChars(String str) {
		return str.replace("\u0000", "");
	}
}

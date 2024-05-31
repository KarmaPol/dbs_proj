package dbs.sqlExecutor;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.zip.CRC32;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;

import dbs.fileManager.RecordReader;
import dbs.metadataManager.MetadataHandler;
import dbs.metadataManager.vo.AttributeMetadataVO;
import dbs.metadataManager.vo.TableMetadataVO;

public class JoinExecutor {
	private static final Integer MODULUS = 3;
	private static final Integer JOIN_MODULUS = 4;
	private static final int BLOCK_FACTOR = 3;

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
		createPartitionFile(joinRecords, right, joinTable);

		joinPartitionFiles(fromTable, joinTable, left, right);

	}

	private static List<String> splitStringBySize(String str, int size) {
		List<String> result = new ArrayList<>();
		for (int start = 0; start < str.length(); start += size) {
			int end = Math.min(start + size, str.length());
			result.add(str.substring(start, end));
		}
		return result;
	}

	public static List<String> readPartitionFile(TableMetadataVO tableMetadataVO, int partitionNumber) {
		String fileName = tableMetadataVO.name();
		int recordSize = tableMetadataVO.recordSize();
		String filePath = fileName + "_partition_" + partitionNumber + ".txt";

		List<String> records = new ArrayList<>();
		try (FileReader fileReader = new FileReader(filePath)) {
			char[] characters = new char[recordSize* BLOCK_FACTOR];
			int numCharsRead;

			while ((numCharsRead = fileReader.read(characters)) != -1) {
				String record = new String(characters, 0, numCharsRead);
				List<String> splitedRecords = splitStringBySize(record, recordSize);
				records.addAll(splitedRecords);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		return records;
	}

	private void joinPartitionFiles(String fromTable, String joinTable, String left, String right) {
		TableMetadataVO fromTableMetaData = MetadataHandler.getTableMetaData(fromTable);
		TableMetadataVO joinTableMetaData = MetadataHandler.getTableMetaData(joinTable);

		Map<String, AttributeMetadataVO> fromAttributeMetadataVOMap = MetadataHandler.getAttributeMetadata(fromTableMetaData.name());
		List<AttributeMetadataVO> fromAttributeMetadatas = fromAttributeMetadataVOMap.values().stream().toList();

		Map<String, AttributeMetadataVO> joinAttributeMetadataVOMap = MetadataHandler.getAttributeMetadata(joinTableMetaData.name());
		List<AttributeMetadataVO> joinAttributeMetadatas = joinAttributeMetadataVOMap.values().stream().toList();

		List<AttributeMetadataVO> fromSortedAttributeMetadatas = fromAttributeMetadatas.stream()
			.sorted(Comparator.comparingInt(AttributeMetadataVO::columnIdx))
			.collect(Collectors.toList());
		List<AttributeMetadataVO> joinSortedAttributeMetadatas = joinAttributeMetadatas.stream()
			.sorted(Comparator.comparingInt(AttributeMetadataVO::columnIdx))
			.collect(Collectors.toList());

		for(int i = 0; i < MODULUS; i++) {
			List<String> fromPartitionRecords = readPartitionFile(fromTableMetaData, i);
			List<String> joinPartitionRecords = readPartitionFile(joinTableMetaData, i);

			HashMap<Integer, HashMap<String, String>> fromHashIndex = new HashMap<>();
			HashMap<Integer, HashMap<String, String>> joinHashIndex = new HashMap<>();

			for(String record : fromPartitionRecords) {
				if(isDeleted(record)) {
					continue;
				}

				int idx = 0;
				HashMap<String, String> temp = new HashMap<>();
				for (AttributeMetadataVO attributeMetadata : fromSortedAttributeMetadatas) {
					int size = attributeMetadata.size();
					String substring = record.substring(idx, idx + size);
					substring = removeNullChars(substring);
					idx += size;
					temp.put(attributeMetadata.name(), substring);
				}
				Integer modValue = Math.abs(getModValue(temp.get(left), JOIN_MODULUS));
				fromHashIndex.put(modValue, temp);
			}

			for(String record : joinPartitionRecords) {
				if(isDeleted(record)) {
					continue;
				}

				int idx = 0;
				HashMap<String, String> temp = new HashMap<>();
				for (AttributeMetadataVO attributeMetadata : joinSortedAttributeMetadatas) {
					int size = attributeMetadata.size();
					String substring = record.substring(idx, idx + size);
					substring = removeNullChars(substring);
					idx += size;
					temp.put(attributeMetadata.name(), substring);
				}
				Integer modValue = Math.abs(getModValue(temp.get(right), MODULUS));
				joinHashIndex.put(modValue, temp);
			}

			List<String> joinedRecords = new ArrayList<>();

			for (Map.Entry<Integer, HashMap<String, String>> fromEntry : fromHashIndex.entrySet()) {
				Integer modValue = fromEntry.getKey();
				HashMap<String, String> fromRecord = fromEntry.getValue();

				if (joinHashIndex.containsKey(modValue)) {
					HashMap<String, String> joinRecord = joinHashIndex.get(modValue);
					if (fromRecord.get(left).equals(joinRecord.get(left))) {
						StringBuilder joinedRecord = new StringBuilder();
						for (String value : fromRecord.values()) {
							joinedRecord.append(value).append(",");
						}
						for (String value : joinRecord.values()) {
							joinedRecord.append(value).append(",");
						}
						// Remove the last comma
						joinedRecord.setLength(joinedRecord.length() - 1);
						joinedRecords.add(joinedRecord.toString());
					}
				}
			}

			// Output the joined records
			for (String record : joinedRecords) {
				System.out.println(record);
			}
		}
	}


	private void createPartitionFile(List<Map<String, String>> records, String key, String tableName) {
		Map<Integer, BufferedWriter> writers = new HashMap<>();
		File file;

		try {
			for (int i = 0; i < MODULUS; i++) {
				file = new File(tableName + "_partition_" + i + ".txt");
				file.createNewFile();
				writers.put(i, new BufferedWriter(new FileWriter(file, true)));
			}

			for (Map<String, String> record : records) {
				String keyValue = record.get(key);
				Integer modValue = Math.abs(getModValue(keyValue, MODULUS));

				BufferedWriter writer = writers.get(modValue);

				List<String> columnNames = new ArrayList<>(record.keySet());
				List<String> columnValues = new ArrayList<>(record.values());

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

				writer.write(currentRecord);
			}

			for (BufferedWriter writer : writers.values()) {
				writer.close();
			}
		} catch (IOException e) {
			System.out.println("Error creating file: " + e.getMessage());
		}
	}

	private static int getModValue(String key, int modulus) {
		CRC32 crc32 = new CRC32();
		crc32.update(key.getBytes());
		int mod = (int) crc32.getValue() % modulus;
		return mod;
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

package dbs.sqlExecutor;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.delete.Delete;

import dbs.fileManager.RecordReader;
import dbs.fileManager.RecordWriter;
import dbs.metadataManager.MetadataHandler;
import dbs.metadataManager.vo.AttributeMetadataVO;
import dbs.metadataManager.vo.TableMetadataVO;

public class DeleteExecutor {
	public void deleteRecord(Delete sql) {
		String tableName = sql.getTable().toString();
		Expression where = sql.getWhere();

		String[] parts = where.toString().split("=");
		String columnName = parts[0].trim();
		String columnValue = parts[1].trim();

		TableMetadataVO tableMetaData = MetadataHandler.getTableMetaData(tableName);
		Map<String, AttributeMetadataVO> attributeMetadataVOMap = MetadataHandler.getAttributeMetadata(tableName);
		List<AttributeMetadataVO> attributeMetadatas = attributeMetadataVOMap.values().stream().toList();

		List<AttributeMetadataVO> sortedAttributeMetadatas = attributeMetadatas.stream()
			.sorted(Comparator.comparingInt(AttributeMetadataVO::columnIdx))
			.collect(Collectors.toList());

		List<String> records = RecordReader.readFile(tableMetaData);

		int recordIdx = 0; boolean hasDelete = false;
		for(String record : records) {
			int idx = 0;
			for (AttributeMetadataVO attributeMetadata : sortedAttributeMetadatas) {
				int size = attributeMetadata.size();
				String substring = record.substring(idx, idx + size);
				substring = removeNullChars(substring);
				idx += size;

				if(attributeMetadata.name().equals(columnName) && substring.equals(columnValue)) {
					hasDelete = true;
					break;
				}
			}
			if(hasDelete) break;
			recordIdx++;
		}

		if(hasDelete) {
			int recordSize = tableMetaData.recordSize();
			int offset = tableMetaData.recordSize()* recordIdx;
			RecordWriter.writeRecordAtOffset(":free ", offset, tableMetaData);
			System.out.println(columnValue + " " + columnName + " deleted successfully");
		}
	}

	private String removeNullChars(String str) {
		return str.replace("\u0000", "");
	}
}

package dbs.sqlExecutor;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import net.sf.jsqlparser.statement.select.PlainSelect;

import dbs.metadataHandler.MetadataHandler;
import dbs.metadataHandler.vo.AttributeMetadataVO;
import dbs.metadataHandler.vo.TableMetadataVO;
import dbs.sqlExecutor.recordUtil.RecordReader;

public class SelectExecutor {
	public void selectRecords(PlainSelect sql) {
		String tableName = sql.getFromItem().toString();

		TableMetadataVO tableMetaData = MetadataHandler.getTableMetaData(tableName);
		Map<String, AttributeMetadataVO> attributeMetadataVOMap = MetadataHandler.getAttributeMetadata(tableName);
		List<AttributeMetadataVO> attributeMetadatas = attributeMetadataVOMap.values().stream().toList();

		List<AttributeMetadataVO> sortedAttributeMetadatas = attributeMetadatas.stream()
			.sorted(Comparator.comparingInt(AttributeMetadataVO::columnIdx))
			.collect(Collectors.toList());

		StringBuilder answer = new StringBuilder();
		List<String> records = RecordReader.readFile(tableMetaData);

		for(String record : records) {
			int idx = 0;
			for (AttributeMetadataVO attributeMetadata : sortedAttributeMetadatas) {
				int size = attributeMetadata.size();
				String substring = record.substring(idx, idx + size);
				substring = removeNullChars(substring);
				idx += size;
				answer.append(attributeMetadata.name()).append(":").append(substring).append("\n");
			}
			answer.append("\n");
		}
		System.out.println(answer.toString());
	}

	private String removeNullChars(String str) {
		return str.replace("\u0000", "");
	}
}

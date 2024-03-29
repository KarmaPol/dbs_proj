package dbs.queryExecutor;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import net.sf.jsqlparser.statement.select.PlainSelect;

import dbs.metadataHandler.MetadataHandler;
import dbs.metadataHandler.vo.AttributeMetadataVO;
import dbs.metadataHandler.vo.TableMetadataVO;
import dbs.recordUtil.RecordReader;

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
		int idx = 0;
		for(String record : records) {
			for (AttributeMetadataVO attributeMetadata : sortedAttributeMetadatas) {
				int size = attributeMetadata.size();
				String substring = record.substring(idx, idx + size);
				idx += size;
				answer.append(attributeMetadata.name()).append(":").append(substring).append(" ");
			}
			answer.append("\n");
		}
		System.out.println(answer.toString());
	}
}

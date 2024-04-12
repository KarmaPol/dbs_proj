package dbs.sqlExecutor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import net.sf.jsqlparser.statement.insert.Insert;

import dbs.metadataHandler.MetadataHandler;
import dbs.metadataHandler.vo.AttributeMetadataVO;
import dbs.metadataHandler.vo.TableMetadataVO;
import dbs.sqlExecutor.recordUtil.RecordWriter;

public class InsertExecutor {
	public void insertRecord(Insert sql) {
		String tableName = sql.getTable().toString();
		List<String> columnNames = sql.getColumns().stream()
			.map(col -> {
				return col.getColumnName();
			}).collect(Collectors.toList());
		List<String> columnValues = sql.getSelect().getValues().getExpressions().stream()
			.map(value -> value.toString())
			.collect(Collectors.toList());

		TableMetadataVO tableMetaData = MetadataHandler.getTableMetaData(tableName);
		Map<String, AttributeMetadataVO> attributeMetadataVOMap = MetadataHandler.getAttributeMetadata(tableName);

		List<String> recordList = new ArrayList<>(Collections.nCopies(columnNames.size(), null));
		StringBuilder recordMaker = new StringBuilder();

		for(int i = 0; i < columnNames.size(); i++) {
			String columnName = columnNames.get(i);
			String columnValue = columnValues.get(i);

			AttributeMetadataVO currentAttributeMetadata = attributeMetadataVOMap.get(columnName);

			int columnSize = currentAttributeMetadata.size();
			String columnType = currentAttributeMetadata.type();
			int columnIdx = currentAttributeMetadata.columnIdx();

			String emptyRecord = new String(new char[columnSize]);
			String currentValue = (columnValue + emptyRecord).substring(0, columnSize);
			recordList.set(columnIdx, currentValue);
			recordMaker.append(currentValue);
		}

		String currentRecord = recordMaker.toString();
		RecordWriter.writeRecord(currentRecord, tableMetaData);
	}
}

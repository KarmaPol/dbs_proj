package dbs.queryExecutor;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;

import dbs.metadataHandler.MetadataHandler;
import dbs.metadataHandler.vo.AttributeMetadataVO;
import dbs.metadataHandler.vo.TableMetadataVO;
import dbs.recordUtil.RecordWriter;

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
		List<AttributeMetadataVO> attributeMetadatas = MetadataHandler.getAttributeMetadata(tableName);

		StringBuilder recordMaker = new StringBuilder();
		for(int i = 0; i < columnNames.size(); i++) {
			String columnName = columnNames.get(i);
			String columnValue = columnValues.get(i);

			AttributeMetadataVO currentAttributeMetadata = null;
			for(int j = 0; j < attributeMetadatas.size(); j++) {
				if(attributeMetadatas.get(i).name().equals(columnName)) {
					currentAttributeMetadata = attributeMetadatas.get(i);
					break;
				}
			}

			int columnSize = currentAttributeMetadata.size();
			String columnType = currentAttributeMetadata.type();

			String emptyRecord = new String(new char[columnSize]);
			String currentValue = (columnValue + emptyRecord).substring(0, columnSize);
			recordMaker.append(currentValue);
		}
		String currentRecord = recordMaker.toString();
		RecordWriter.writeRecord(currentRecord, tableMetaData);
	}
}

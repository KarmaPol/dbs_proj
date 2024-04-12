package dbs.sqlExecutor;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import net.sf.jsqlparser.statement.create.table.CreateTable;

import dbs.metadataManager.MetadataHandler;
import dbs.metadataManager.vo.AttributeMetadataVO;

public class TableCreator {
	private File file;
	private int recordSize = 0;

	public void createFile(CreateTable sql) {
		String tableName = sql.getTable().getName();
		AtomicInteger columnIdx = new AtomicInteger();
		try {
			file = new File(tableName + ".txt");
			isFileExists();

			String primaryKey = sql.getIndexes().get(0).getColumns().get(0).toString();
			List<AttributeMetadataVO> attributeMetadatas = new ArrayList<>();


			sql.getColumnDefinitions().forEach(column -> {
				String columnName = column.getColumnName();
				String dataType = column.getColDataType().getDataType();
				int currentColumnSize = 0;

				if(dataType.equals("varchar")) {
					currentColumnSize += Integer.parseInt(column.getColDataType().getArgumentsStringList().get(0));
					recordSize += currentColumnSize;
				}
				else if(dataType.equals("int")) {
					currentColumnSize = 10;
					recordSize += currentColumnSize;
				} else {
					throw new RuntimeException("Invalid data type");
				}

				AttributeMetadataVO attributeMetadataVO =
					new AttributeMetadataVO(columnName, dataType, currentColumnSize, columnIdx.get(), tableName);
				attributeMetadatas.add(attributeMetadataVO);
				columnIdx.getAndIncrement();
			});

			MetadataHandler.createTableMetadata(tableName, recordSize, primaryKey);
			MetadataHandler.createAttributeMetadata();
			MetadataHandler.insertColumnMetadata(attributeMetadatas);
		} catch (Exception e) {
			System.out.println("Error creating file: " + e.getMessage());
		}
	}

	private void isFileExists() throws IOException {
		if(!file.exists()) {
			file.createNewFile();
		}
	}
}

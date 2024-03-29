package dbs.queryExecutor;

import java.io.File;
import java.io.IOException;

import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.table.Index;

import dbs.metadataHandler.MetadataHandler;
import dbs.metadataHandler.vo.AttributeMetadataVO;

public class TableCreator {
	private final String FILE_PATH = "";
	private File file;
	private int recordSize = 0;

	public TableCreator() {
	}

	public void createFile(CreateTable createSql) {
		String tableName = createSql.getTable().getName();

		try {
			file = new File(FILE_PATH + tableName + ".txt");
			isFileExists();

			String primaryKey = createSql.getIndexes().get(0).getColumns().get(0).toString();

			createSql.getColumnDefinitions().forEach(column -> {
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
					new AttributeMetadataVO(columnName, dataType, currentColumnSize, tableName);
				MetadataHandler.createAttributeMetadata(attributeMetadataVO);
			});

			MetadataHandler.createTableMetadata(tableName, recordSize, primaryKey);

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

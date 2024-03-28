package dbs.queryExecutor;

import java.io.File;

public class TableCreator {
	private final String FILE_PATH = "src/main/resources/";
	private File file;

	public TableCreator() {
	}

	private void createFile(String fileName) {
		try {
			file = new File(FILE_PATH + fileName + ".csv");
		} catch (Exception e) {
			System.out.println("Error creating file: " + e.getMessage());
		}
	}
}

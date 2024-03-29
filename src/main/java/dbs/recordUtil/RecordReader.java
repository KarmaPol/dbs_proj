package dbs.recordUtil;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RecordReader {
	private static final int BLOCK_FACTOR = 3;

	private static List<String> readFile(String fileName, int recordSize) {
		String filePath = fileName + ".txt";
		List<String> records = new ArrayList<>();
		try (FileReader fileReader = new FileReader(filePath)) {
			char[] characters = new char[recordSize* BLOCK_FACTOR];
			int numCharsRead;

			while ((numCharsRead = fileReader.read(characters)) != -1) {
				String record = new String(characters, 0, numCharsRead);
				records.add(record);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return records;
	}
}

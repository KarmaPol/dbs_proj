package dbs.sqlExecutor.recordUtil;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import dbs.metadataHandler.vo.TableMetadataVO;

public class RecordReader {
	private static final int BLOCK_FACTOR = 3;

	public static List<String> readFile(TableMetadataVO tableMetadataVO) {
		String fileName = tableMetadataVO.name();
		int recordSize = tableMetadataVO.recordSize();
		String filePath = fileName + ".txt";

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

	private static List<String> splitStringBySize(String str, int size) {
		List<String> result = new ArrayList<>();
		for (int start = 0; start < str.length(); start += size) {
			int end = Math.min(start + size, str.length());
			result.add(str.substring(start, end));
		}
		return result;
	}
}

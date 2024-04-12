package dbs.sqlExecutor.recordUtil;

import java.io.FileWriter;
import java.io.IOException;

import dbs.metadataHandler.vo.TableMetadataVO;

public class RecordWriter {
	public static void writeRecord(String record, TableMetadataVO tableMetadataVO) {
		String filePath = tableMetadataVO.name() + ".txt";

		try (FileWriter writer = new FileWriter(filePath, true)) {
			writer.write(record);
			System.out.println("레코드가 파일의 마지막에 성공적으로 추가되었습니다.");
		} catch (IOException e) {
			System.err.println("파일 쓰기 중 오류가 발생했습니다: " + e.getMessage());
		}
	}
}

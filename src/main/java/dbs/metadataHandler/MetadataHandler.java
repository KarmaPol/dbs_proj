package dbs.metadataHandler;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import dbs.metadataHandler.vo.AttributeMetadataVO;
import dbs.metadataHandler.vo.TableMetadataVO;

public class MetadataHandler {
	private static final String url = "jdbc:mysql://localhost:3307/metadata_db"; // yourDatabaseName을 데이터베이스 이름으로 변경하세요.
	private static final String user = "root"; // 데이터베이스 사용자 이름
	private static final String password = "root1234!"; // 데이터베이스 비밀번호
	public static void createTableMetadata(String tableName, int recordSize, String primaryKey) {
		try (Connection conn = DriverManager.getConnection(url, user, password)) {
			// Statement 객체 생성
			try (Statement stmt = conn.createStatement()) {

				String dropTableSql = "DROP TABLE IF EXISTS table_metadata;";
				stmt.executeUpdate(dropTableSql);

				// 테이블 생성 SQL 쿼리
				String createTableSql = "CREATE TABLE IF NOT EXISTS table_metadata (" +
					"id INT AUTO_INCREMENT PRIMARY KEY, " +
					"name VARCHAR(255) NOT NULL, " +
					"record_size INT NOT NULL, " +
					"pk VARCHAR(255) NOT NULL)";
				stmt.executeUpdate(createTableSql);
				System.out.println("테이블이 성공적으로 생성되었습니다.");

				// 데이터 삽입 SQL 쿼리
				String insertSql = "INSERT INTO table_metadata (name, record_size, pk) VALUES ('" + tableName + "', " + recordSize + ", '" + primaryKey + "'"  + ")";
				stmt.executeUpdate(insertSql);
				System.out.println("데이터가 성공적으로 삽입되었습니다.");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void createAttributeMetadata() {
		String dropTableSql = "DROP TABLE IF EXISTS column_metadata;";

		String createTableSql = "CREATE TABLE IF NOT EXISTS column_metadata (" +
			"id INT AUTO_INCREMENT PRIMARY KEY, " +
			"column_name VARCHAR(255) NOT NULL, " +
			"data_type VARCHAR(255) NOT NULL, " +
			"column_size INT, " +
			"column_idx INT, " +
			"table_name VARCHAR(255) NOT NULL" +
			")";

		try (Connection conn = DriverManager.getConnection(url, user, password);
			 Statement stmt = conn.createStatement()) {
			stmt.executeUpdate(dropTableSql);

			stmt.executeUpdate(createTableSql);
			System.out.println("column_metadata 테이블이 성공적으로 생성되었습니다.");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public static void insertColumnMetadata(List<AttributeMetadataVO> vos) throws SQLException {
		String insertSql = "INSERT INTO column_metadata (column_name, data_type, column_size, table_name, column_idx) VALUES (?, ?, ?, ?, ?)";

		try (Connection conn = DriverManager.getConnection(url, user, password)) {
			try (PreparedStatement pstmt = conn.prepareStatement(insertSql)) {
				// VO(Value Object) 리스트를 반복 처리
				AtomicInteger idx = new AtomicInteger(0);
				vos.forEach(vo -> {
					try {
						pstmt.setString(1, vo.name());
						pstmt.setString(2, vo.type());
						pstmt.setInt(3, vo.size());
						pstmt.setString(4, vo.tableName());
						pstmt.setInt(5, idx.get());
						idx.getAndIncrement();

						// 배치에 현재 설정된 파라미터를 추가
						pstmt.addBatch();
					} catch (SQLException e) {
						e.printStackTrace();
					}
				});

				// 모든 VO에 대해 설정된 배치를 한 번에 실행
				pstmt.executeBatch();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public static TableMetadataVO getTableMetaData(String tableName) {
		try (Connection conn = DriverManager.getConnection(url, user, password)) {
			// PreparedStatement 객체 생성
			String querySql = "SELECT name, record_size, pk FROM table_metadata WHERE name = ?";
			try (PreparedStatement pstmt = conn.prepareStatement(querySql)) {
				pstmt.setString(1, tableName); // 첫 번째 파라미터에 검색할 name 값 설정

				// 쿼리 실행 및 결과 처리
				try (ResultSet rs = pstmt.executeQuery()) {
					while (rs.next()) {
						return new TableMetadataVO(rs.getString("name"), rs.getInt("record_size"), rs.getString("pk"));
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return new TableMetadataVO(tableName, 0, "");
	}

	public static Map<String, AttributeMetadataVO> getAttributeMetadata(String tableName) {
		String query = "SELECT column_name, data_type, column_size, column_idx FROM column_metadata WHERE TABLE_NAME = ?";

		List<AttributeMetadataVO> attributeMetadatas = new ArrayList<>();
		Map<String, AttributeMetadataVO> attributeMetadataVOMap = new HashMap<>();

		try (Connection conn = DriverManager.getConnection(url, user, password);
			 PreparedStatement pstmt = conn.prepareStatement(query)) {

			pstmt.setString(1, tableName);

			try (ResultSet rs = pstmt.executeQuery()) {
				while (rs.next()) {
					String columnName = rs.getString("column_name");
					String dataType = rs.getString("data_type");
					int columnSize = rs.getInt("column_size");
					int columnIdx = rs.getInt("column_idx");

					AttributeMetadataVO vo = new AttributeMetadataVO(columnName, dataType, columnSize, columnIdx, tableName);
					attributeMetadataVOMap.put(columnName, vo);
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return attributeMetadataVOMap;
	}
}

import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class DiskManager {
	public static String CSV_SEPARATOR = ",";
	public static String NEW_LINE_SEPARATOR = "\n";
	
	public static void createDirectory(String dir) {
		try {
			Path path = Paths.get(dir);
			Files.deleteIfExists(path);
			Files.createDirectory(path);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void writeRecordsToDisk(String dir, String filename, List<Record> records) {
		FileWriter fileWriter = null;
		try {
			fileWriter = new FileWriter(dir + "/" + filename);
			for (Record r : records) {
				String line = String.join(CSV_SEPARATOR, r.getValues());
				fileWriter.append(line);
				fileWriter.append(NEW_LINE_SEPARATOR);
			}
			fileWriter.flush();
			fileWriter.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void writeRecords(String dir, String filename, List<String[]> records) {
		FileWriter fileWriter = null;
		try {
			fileWriter = new FileWriter(dir + "/" + filename);
			for (String[] r : records) {
				String line = String.join(CSV_SEPARATOR, r);
				fileWriter.append(line);
				fileWriter.append(NEW_LINE_SEPARATOR);
			}
			fileWriter.flush();
			fileWriter.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
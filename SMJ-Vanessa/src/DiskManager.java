import java.io.FileWriter;
import java.util.List;

public class DiskManager {
	public static String CSV_SEPARATOR = ",";
	public static String NEW_LINE_SEPARATOR = "\n";
	
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
}
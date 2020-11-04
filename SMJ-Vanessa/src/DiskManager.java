import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
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
			deleteDirectory(dir);
			Files.createDirectory(path);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void deleteDirectory(String dir) {
		File directoryToBeDeleted = new File(dir);
	    File[] allContents = directoryToBeDeleted.listFiles();
	    if (allContents != null) {
	        for (File file : allContents) {
	            deleteDirectory(file.getPath());
	        }
	    }
	    System.gc();
	    directoryToBeDeleted.delete();
	}
	
	public static void writeRecordsToDisk(String filename, List<Record> records) {
		FileWriter fileWriter = null;
		try {
			fileWriter = new FileWriter(filename);
			for (Record r : records) {
				String line = String.join(CSV_SEPARATOR, r.getValues());
				fileWriter.write(line);
				fileWriter.write(NEW_LINE_SEPARATOR);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
				try {
					if (fileWriter != null) {
					fileWriter.flush();
					fileWriter.close();
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
		}
	}
	
	public static void appendRecordsToDisk(String filename, List<Record> records) {
		FileWriter fileWriter = null;
		try {
			fileWriter = new FileWriter(filename, true);
			for (Record r : records) {
				String line = String.join(CSV_SEPARATOR, r.getValues());
				fileWriter.append(line);
				fileWriter.append(NEW_LINE_SEPARATOR);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (fileWriter != null) {
				fileWriter.flush();
				fileWriter.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
}
	}
}

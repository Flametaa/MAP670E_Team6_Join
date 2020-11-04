import java.io.File;
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
			deleteDirectory(dir);
			Files.createDirectory(path);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	static public boolean deleteDirectory(String dir) {
		File path = new File(dir);
	    if (path.exists()) {
	        File[] files = path.listFiles();
	        for (int i = 0; i < files.length; i++) {
	            if (files[i].isDirectory()) {
	                deleteDirectory(files[i].getPath());
	            } else {
	                files[i].delete();
	            }
	        }
	    }
	    System.gc();
	    return (path.delete());
	}
	
	public static void writeRecordsToDisk(String filename, List<Record> records) {
		FileWriter fileWriter = null;
		try {
			fileWriter = new FileWriter(filename);
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
	
	public static void appendRecordsToDisk(String filename, List<Record> records) {
		FileWriter fileWriter = null;
		try {
			fileWriter = new FileWriter(filename, true);
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
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.RandomAccessFile;

import java.util.ArrayList;
import java.util.List;

public class Table {	
	public static String CSV_SPLIT_BY = ",";
	
	private String tablename;
	private String filename;
	private int numFields;
	private int numRecords;
	private List<Integer> recordsOffset;
	List<Integer> recordsLength;
	
	public Table(String tablename, String filename) {
		this.tablename = tablename;
		this.filename = filename;
		this.recordsOffset = new ArrayList<Integer>();
		this.recordsLength = new ArrayList<Integer>();
		this.numFields = countFields();
		this.numRecords = countRecords();
	}

	private int countRecords() {
		int n = 0;
		BufferedReader br = null;
		String line = "";
		try {
			br = new BufferedReader(new FileReader(filename));
			recordsOffset.add(0);
			while ((line=br.readLine()) != null) {
				recordsLength.add(line.getBytes().length);
				recordsOffset.add(line.getBytes().length + recordsOffset.get(recordsOffset.size()-1)+1);
				n++;
			}
			br.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return n;
	}
	
	private int countFields() {
		int n = 0;
		BufferedReader br = null;
		String line = "";
		try {
			br = new BufferedReader(new FileReader(filename));
			if ((line=br.readLine()) != null) {
				String[] l = line.split(CSV_SPLIT_BY);
				n = l.length;
			}
			br.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return n;
	}
	
	public int getNumRecords() {
		return numRecords;
	}
	
	public String getRecord(int i) {
		byte[] bytes = new byte[(int) recordsLength.get(i)];
		RandomAccessFile raf;
		try {
			raf = new RandomAccessFile(filename, "r");
			raf.seek(recordsOffset.get(i));
			raf.read(bytes);
			raf.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return new String(bytes);
	}
	
	public String getTablename() {
		return tablename;
	}

	public String getFilename() {
		return filename;
	}

	public List<Integer> getRecordsLength() {
		return recordsLength;
	}
	
	public int getNumFields() {
		return numFields;
	}

	public List<Integer> getRecordsOffset() {
		return recordsOffset;
	}
}
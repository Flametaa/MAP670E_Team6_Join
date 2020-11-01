import java.io.BufferedReader;
import java.io.FileReader;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

public class Table {
	private static int numRecordsPerMemoryPage = 1000;
	
	private String tablename;
	private String filename;
	private int numFields;
	private int numRecords;
	private List<Long> recordsPos;
	List<Integer> recordsSize;
	private List<Long> pagesPos;
	private int numMemoryPages;
	
	public Table(String filename) {
		this.filename = filename;
		this.recordsPos = new ArrayList<Long>();
		this.recordsSize = new ArrayList<Integer>();
		this.numFields = countFields();
		this.numRecords = countRecords();
		this.pagesPos = pagesPosition();
		this.numMemoryPages = (int) Math.ceil(numRecords/numRecordsPerMemoryPage);
	}
	
	public int getNumRecords() {
		return numRecords;
	}

	public int getNumPages() {
		return numMemoryPages;
	}

	private List<Long> pagesPosition() {
		List<Long> pos = new ArrayList<Long>();
		for (int i=0; i < numRecords-1; i+=numRecordsPerMemoryPage) {
			pos.add(recordsPos.get(i));
		}
		pos.add(recordsPos.get(numRecords-1));
		return pos;
	}
	
	public List<Long> getPagesPos() {
		return pagesPos;
	}

	private int countRecords() {
		int n = 0;
		BufferedReader br = null;
		String line = "";
		try {
			br = new BufferedReader(new FileReader(filename));
			recordsPos.add((long) 0);
			while ((line=br.readLine()) != null) {
				recordsSize.add(line.getBytes().length-1);
				recordsPos.add(line.getBytes().length + recordsPos.get(recordsPos.size()-1)+1);
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
		String csvSplitBy = ",";
		try {
			br = new BufferedReader(new FileReader(filename));
			if ((line=br.readLine()) != null) {
				String[] l = line.split(csvSplitBy);
				n = l.length;
			}
			br.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return n;
	}
	
	public String getRecord(int i) {
		byte[] bytes = new byte[recordsSize.get(i-1)];
		RandomAccessFile raf;
		try {
			raf = new RandomAccessFile(filename, "r");
			raf.seek(recordsPos.get(i-1));
			raf.read(bytes);
			raf.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return new String(bytes);
	}
	
	public String getRecord(int i, int j) {
		byte[] bytes = new byte[recordsSize.get(i-1)];
		RandomAccessFile raf;
		String[] l = {};
		try {
			raf = new RandomAccessFile(filename, "r");
			raf.seek(recordsPos.get(i-1));
			raf.read(bytes);
			raf.close();
			String line = new String(bytes);
			String csvSplitBy = ",";
			l = line.split(csvSplitBy);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return l[j];
	}
	
	public List<String[]> PagetoList(int i) {
		long start = pagesPos.get(i-1);
		String csvSplitBy = ",";
		String l = "";
		List<String[]> page = new ArrayList<String[]>();
		 try {
			for (int j=0; j < numRecordsPerMemoryPage; ++j) {
				 RandomAccessFile raf = new RandomAccessFile(filename, "r");
				 raf.seek(start);
				 byte[] bytes = new byte[recordsSize.get(j)];
				 raf.read(bytes);
				 l = new String(bytes);
				 page.add(l.split(csvSplitBy));
				 start += recordsSize.get(j);
				 raf.close();
			}
		 } catch (Exception e) {
		  throw new RuntimeException(e);
		 }
		 return page;
	}
	
	
	public MappedByteBuffer PagestoMemory(int i) {
		 try {
			 RandomAccessFile raf = new RandomAccessFile(filename, "r");
			 FileChannel fc = raf.getChannel();
			 MappedByteBuffer buffer = fc.map(FileChannel.MapMode.READ_ONLY, pagesPos.get(i-1), pagesPos.get(i)-1);
			 for(int j = 0; j < pagesPos.get(i) - pagesPos.get(i-1) - 1; j++)
			        System.out.print((char)buffer.get(j));
			 raf.close();
			  return buffer;
		 } catch (Exception e) {
		  throw new RuntimeException(e);
		 }
	}
	
	public String getTablename() {
		return tablename;
	}

	public String getFilename() {
		return filename;
	}

	public int getNumFields() {
		return numFields;
	}

	public List<Long> getRecordsPosition() {
		return recordsPos;
	}
	
	public static void main(String[] args) {
		Table t = new Table("src/authors.csv");
		List<String[]> l = t.PagetoList(1);
		System.out.println(l.size());
		for (int j=0; j<1000;++j ) {
			for (int i=0; i < 6;++i) {
				System.out.println(l.get(j)[i]);
			}
		}
	}
	
}
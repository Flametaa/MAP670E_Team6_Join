import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

public class PageManager {
	public static int RECORDS_PER_PAGE = 1000;
	
	private Table table;
	private int totalRecords;
	private int numPages;
	
	public PageManager(Table table) {
		this.table = table;
		this.totalRecords = table.getNumRecords();
		this.numPages = (totalRecords-1)/RECORDS_PER_PAGE + 1;
	}
	
	public List<Record> loadPageToMemory(int p) {
		List<Record> page = new ArrayList<Record>();
		List<Integer> recordsOffset = table.getRecordsOffset();
		List<Integer> recordsLength = table.getRecordsLength();
		RandomAccessFile raf = null;
		 try {
			 raf = new RandomAccessFile(table.getFilename(), "r");
			 int start = recordsOffset.get(p*RECORDS_PER_PAGE);
			 int end;
			 if (p==numPages-1) {
				 end = recordsOffset.get(totalRecords-1) + recordsLength.get(totalRecords-1);
			 } else {
				 end = recordsOffset.get((p+1)*RECORDS_PER_PAGE)-1;
			 }
			 MappedByteBuffer buffer = raf.getChannel().map(FileChannel.MapMode.READ_ONLY, start, end-start);
			 String line = "";
			 for(int j = 0; j < end-start; j++) {
				 if ((char)buffer.get(j) != '\n') {
					 line += (char)buffer.get(j);
				 } else {
					 Record r = new Record(line.split(Table.CSV_SPLIT_BY));
					 page.add(r);
					 line = "";
				 }
			 }
			 Record r = new Record(line.split(Table.CSV_SPLIT_BY));
			 page.add(r);
			 buffer.force();
			 buffer.clear();
		 } catch (Exception e) {
			 e.printStackTrace();
		 } finally {
			 try {
				 if (raf != null) {
					 raf.close();
				 }
			 } catch (Exception e) {
				 e.printStackTrace();
			 }
		 }
		 return page;
	}
	
	public int getNumPages() {
		return numPages;
	}
}
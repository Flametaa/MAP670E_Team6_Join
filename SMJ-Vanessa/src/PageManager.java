import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

public class PageManager {
	public static int RECORDS_PER_PAGE = 5000;
	
	private Table table;
	private int totalRecords;
	private int numPages;
	private List<Integer> pagesOffset;
	private List<List<Integer>> pageRecordsLength;
	
	public PageManager(Table table) {
		this.table = table;
		this.totalRecords = table.getNumRecords();
		this.numPages = (totalRecords-1)/RECORDS_PER_PAGE + 1;
		this.pagesOffset = new ArrayList<Integer>();
		this.pageRecordsLength = new ArrayList<List<Integer>>();
		computePageLengthOffset();
	}
	
	private void computePageLengthOffset() {
		List<Integer> totalRecordsOffset = table.getRecordsOffset();
		List<Integer> totalRecordsLength = table.getRecordsLength();
		for (int i=0; i < totalRecords; i+=RECORDS_PER_PAGE) {
			if (i+RECORDS_PER_PAGE > totalRecords-1) {
				pageRecordsLength.add(totalRecordsLength.subList(i, totalRecords));
			} else {
				pageRecordsLength.add(totalRecordsLength.subList(i, i+RECORDS_PER_PAGE+1));	
			}
			pagesOffset.add(totalRecordsOffset.get(i));
		}
	}
	
	public List<String[]> loadPageToMemory(int p) {
		long start = pagesOffset.get(p);
		List<Integer> recordsLength = pageRecordsLength.get(p);
		List<String[]> page = new ArrayList<String[]>();
		String record = "";
		 try {
			for (int j=0; j <= RECORDS_PER_PAGE; ++j) {
				 RandomAccessFile raf = new RandomAccessFile(table.getFilename(), "r");
				 raf.seek(start);
				 byte[] bytes = new byte[recordsLength.get(j)];
				 raf.read(bytes);
				 record = new String(bytes).replace("\"","");
				 page.add(record.split(Table.CSV_SPLIT_BY));
				 start += recordsLength.get(j)+1;
				 raf.close();
			}
		 } catch (Exception e) {
			 throw new RuntimeException(e);
		 }
		 return page;
	}
	
	public int getNumPages() {
		return numPages;
	}

	public List<Integer> getPagesOffset() {
		return pagesOffset;
	}

	public List<List<Integer>> getPageRecordsLength() {
		return pageRecordsLength;
	}
}
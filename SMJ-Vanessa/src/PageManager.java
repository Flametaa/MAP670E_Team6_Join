import java.util.ArrayList;
import java.util.List;

public class PageManager {
	public static int RECORDS_PER_PAGE = 100;
	
	private Table table;
	private int totalRecords;
	private int numPages;
	
	public PageManager(Table table) {
		this.table = table;
		this.totalRecords = table.getNumRecords();
		this.numPages = (totalRecords-1)/RECORDS_PER_PAGE + 1;
	}
	
	public List<TableRecord> loadPageToMemory(int p) {
		List<TableRecord> page = new ArrayList<TableRecord>();
		for (int j=0; j < Math.min(RECORDS_PER_PAGE, totalRecords); ++j) {
			TableRecord r = new TableRecord(table, p*RECORDS_PER_PAGE + j);
			page.add(r);
		}
		return page;
	}
	
	public int getNumPages() {
		return numPages;
	}
}
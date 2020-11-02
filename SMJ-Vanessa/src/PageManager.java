import java.util.ArrayList;
import java.util.List;

public class PageManager {
	public static int RECORDS_PER_PAGE = 2000;
	
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
		for (int j=0; j < RECORDS_PER_PAGE; ++j) {
			Record r = new Record(table, p*RECORDS_PER_PAGE + j);
			System.out.println(r.getValue(0));
			page.add(r);
		}
		return page;
	}
	
	public int getNumPages() {
		return numPages;
	}
}
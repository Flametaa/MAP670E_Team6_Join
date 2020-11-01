import java.util.ArrayList;
import java.util.List;

public class PageManager {
	private static int RECORDS_PER_PAGE = 1000;
	
	private Table table;
	private int numPages;
	private List<Long> pagesPos;
	
	public PageManager(Table table) {
		this.table = table;
		this.numPages = (int) Math.ceil(table.getNumRecords()/RECORDS_PER_PAGE);
		this.pagesPos = pagesPosition();
	}
	
	private List<Long> pagesPosition() {
		List<Long> pos = new ArrayList<Long>();
		for (int i=0; i < table.getNumRecords()-1; i+=RECORDS_PER_PAGE) {
			pos.add(table.getRecordsPosition().get(i));
		}
		pos.add(table.getRecordsPosition().get(table.getNumRecords()-1));
		return pos;
	}
	
}
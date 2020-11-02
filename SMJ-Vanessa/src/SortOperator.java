import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class SortOperator {
	public static int NUM_BUFFERS = 5;
	
	private Table table;
	private PageManager pageManager;
	private Comparator<Record> comparator;
	
	public SortOperator(Table table) {
		this.table = table;
		this.pageManager = new PageManager(this.table);
		this.comparator = (r1, r2) -> (r1.getValue(0)).compareTo(r2.getValue(0));
	}
	
	public void sort(String runsDir) {
		DiskManager.createDirectory(runsDir);
		int runsNum = (pageManager.getNumPages()-1)/NUM_BUFFERS + 1;
		for (int r=0; r<runsNum; ++r) {
			List<Record> buffersRecords = new ArrayList<Record>();
			for (int p=0; p< Math.min(NUM_BUFFERS, pageManager.getNumPages()); ++p) {
				buffersRecords.addAll(pageManager.loadPageToMemory(r*NUM_BUFFERS + p));
			}
			buffersRecords.sort(comparator);
			String filename = "run_" + r + ".csv";
			DiskManager.writeRecordsToDisk(runsDir + "/" + filename, buffersRecords);
		}
	}
}
public class SortOperator {
	public static int NUM_BUFFERS = 3;
	private Table table;
	private PageManager pageManager;
	
	public SortOperator(Table table) {
		this.table = table;
		this.pageManager = new PageManager(table);
	}
}
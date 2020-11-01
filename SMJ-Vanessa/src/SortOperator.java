public class SortOperator {
	private Table table;
	private PageManager pageManager;
	private int numBuffers; //We can fix this parameter instead of RECORDS_PER_PAGE
	
	public SortOperator(Table table) {
		this.table = table;
		this.pageManager = new PageManager(table);
		this.numBuffers = pageManager.getNumPages();
	}
}
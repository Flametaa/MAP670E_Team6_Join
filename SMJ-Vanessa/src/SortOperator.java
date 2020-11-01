public class SortOperator {
	private Table table;
	private int numBuffers;
	
	public SortOperator(Table table) {
		this.table = table;
		this.numBuffers = table.getNumPages();
	}
	
//	public sort
}
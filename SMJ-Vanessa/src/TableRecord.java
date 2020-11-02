public class TableRecord extends Record {
	Table table;
	int row;
	private int pageNum;
	private int pageEntryNum;
	
	public TableRecord(Table table, int row) {
		super(table.getRecordValues(row));
		this.table = table;
		this.row = row;
		this.pageNum = Math.floorDiv(row, PageManager.RECORDS_PER_PAGE);
		this.pageEntryNum = row % PageManager.RECORDS_PER_PAGE;
	}

	public int getRow() {
		return row;
	}


	public int getPageNum() {
		return pageNum;
	}

	public int getPageEntryNum() {
		return pageEntryNum;
	}
}
public class Record {
	Table table;
	int row;
	int length;
	private int pageNum;
	private int pageEntryNum;
	private String[] values;
	
	public Record(Table table, int row) {
		this.table = table;
		this.row = row;
		this.length = table.getRecordsLength().get(row);
		this.pageNum = Math.floorDiv(row, PageManager.RECORDS_PER_PAGE);
		this.pageEntryNum = row % PageManager.RECORDS_PER_PAGE;
		String record = table.getRecord(row).replace("\"","");
		this.values = record.split(Table.CSV_SPLIT_BY);
	}
	
	public String getValue(int column) {
		return values[column];
	}

	public int getRow() {
		return row;
	}

	public int getLength() {
		return length;
	}

	public int getPageNum() {
		return pageNum;
	}

	public int getPageEntryNum() {
		return pageEntryNum;
	}

	public String[] getValues() {
		return values;
	}
}
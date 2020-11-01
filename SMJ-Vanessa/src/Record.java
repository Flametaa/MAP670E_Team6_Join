public class Record {
	int tablePos;
	int size;
	private int pageNum;
	private int entryNum;
	private String[] values;
	
	public Record(int tablePos) {
		this.tablePos = tablePos;
	}
}
public class SortMergeJoinThread extends SortMergeJoin implements Runnable {
	String output_path;

	public SortMergeJoinThread(Table t1, Table t2, String filename) {
		super(t1, t2);
		this.output_path = filename;
	}

	@Override
	public void run() {
		this.join(output_path);
	}

}

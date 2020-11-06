import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Database {
	String fileDir;
	Map<String, Table> tables;

	public Database(String fileDir) {
		this.fileDir = fileDir;
		tables = new HashMap<String, Table>();
	}

	public Table addTable(String tablename, String filename) {
		Table t = new Table(tablename, fileDir + "/" + filename);
		tables.put(tablename, t);
		return t;
	}

	public void deleteTable(String tablename) {
		tables.remove(tablename);
	}

	public static void main(String[] args) {
		Database d = new Database("database");
		Table t1 = d.addTable("clients_heavy", "clients_heavy.csv");
		Table t2 = d.addTable("purchases_heavy", "purchases_heavy.csv");
		long startTime = System.currentTimeMillis();
		SortMergeJoin j = new SortMergeJoin(t1, t2);
		j.join("database/joined.csv");
		long endTime = System.currentTimeMillis();
		long duration = endTime - startTime;
		System.out.println("Single-threaded duration: " + duration + " ms");

		long startTime0 = System.currentTimeMillis();
		SortMergeJoinThreadMain j0 = new SortMergeJoinThreadMain(t1, t2, 2, "database/joined_thread.csv");
		j0.start();
		try {
			j0.join();
		} catch (InterruptedException ex) {
			Logger.getLogger(Database.class.getName()).log(Level.SEVERE, null, ex);
		}
		long endTime0 = System.currentTimeMillis();
		long duration0 = endTime0 - startTime0;
		System.out.println("Total duration: " + duration0 + " ms");
		System.out.println("Partitioning duration: " + j0.duration_partition + " ms");
		System.out.println("Threads duration: " + j0.duration_threads + " ms");
		System.out.println("Combine duration: " + j0.duration_combine + " ms");
	}
}

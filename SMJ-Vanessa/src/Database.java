import java.util.HashMap;
import java.util.Map;

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
		long duration = (endTime - startTime);
		System.out.println("Duration: " + (endTime-startTime) + " ms");
	}
}
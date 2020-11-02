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
		Table t1 = d.addTable("R1", "mini_purchases_sorted.csv");
		Table t2 = d.addTable("R2", "mini_clients_sorted.csv");
		SortMergeJoin j = new SortMergeJoin(t1, t2);
		Table joined = j.join("joinedClientsPurchases", "database/joined_clients_purchases.csv");
		System.out.println("Done Join");
//		PageManager pm = new PageManager(t2);
//		SortOperator so1 = new SortOperator(t1);
//		so1.sort("database/run1");
//		SortOperator so2 = new SortOperator(t2);
//		so2.sort("database/run2");
	}
}
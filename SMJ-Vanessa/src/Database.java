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
		Table t = new Table(filename);
		tables.put(tablename, t);
		return t;
	}
	
	public void deleteTable(String tablename) {
		tables.remove(tablename);
	}
	
	public static void main(String[] args) {
		Table authors = new Table("src/authors.csv");
	}
}
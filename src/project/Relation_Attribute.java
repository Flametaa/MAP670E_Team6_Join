package project;
/**
 * 
 * final
 *
 */
public class Relation_Attribute {
	private int value;
	private String name;
	
	public Relation_Attribute() {
		this.value = 0;
		this.name = "";
	}
	public Relation_Attribute(int value, String name) {
		this.value = value;
		this.name = name;
	}
	public int getValue() {
		return value;
	}
	public void setValue(int value) {
		this.value = value;
	}
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
}

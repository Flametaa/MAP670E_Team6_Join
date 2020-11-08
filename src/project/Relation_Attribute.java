package project;
/**
 * 
 * This class allows to obtain the attributes such that :
 * Name of columns of relations and the values(records) in  the column specified in each relation 
 *
 */
public class Relation_Attribute {
	private String value;
	private String name;
	
	public Relation_Attribute() {
		this.value = "";
		this.name = "";
	}
	public Relation_Attribute(String value, String name) {
		this.value = value;
		this.name = name;
	}
	public String  getValue() {
		return value;
	}
	public void setValue(String value) {
		this.value = value;
	}
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
}

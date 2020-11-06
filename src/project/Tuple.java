package project;

/**
 * final
 */

import java.util.ArrayList;
import java.util.List;

public class Tuple {
	
	private int number_of_attributes;
	private String relationName;
	
	List<Relation_Attribute> attributes;
	 
	public Tuple() {
		setNumberOfAttributes(4);
		
		attributes = new ArrayList<Relation_Attribute>();
	}
	
	public Tuple(int number_of_attributes, String relationName) {
		this.number_of_attributes = number_of_attributes;
		this.relationName = relationName;
		attributes = new ArrayList<Relation_Attribute>();
	}


	public int getNumberOfAttributes() {
		return number_of_attributes;
	}

	public void setNumberOfAttributes(int number_of_attributes) {
		this.number_of_attributes = number_of_attributes;
	}
	
	public String getRelationName() {
		return relationName;
	}

	public void setRelationName(String relationName) {
		this.relationName = relationName;
	}
	
	public int getAttributeValue(int index) {
		return attributes.get(index).getValue();
	}
	
	public String getAttributeName(int index) {
		return attributes.get(index).getName();
	}
	
	public void setAttributeValue(int index, int value) {
		attributes.get(index).setValue(value);
	}
	
	public void setAttributeName(int index, String value) {
		attributes.get(index).setName(value);
	}
	
	public Relation_Attribute getAttribute(int index) {
		return attributes.get(index);
	}
	
	public String getHeader() {
		String header = "";
		for (Relation_Attribute attribute: attributes) {
			if (attributes.indexOf(attribute) == attributes.size()-1) {
				header = header + attribute.getName();
			} else {
				header = header + attribute.getName() + ",";
			}
		}
		return header;
	}
	
	public String getCSVFile() {
		String tuple = "";
		for (Relation_Attribute attribute: attributes) {
			if (attributes.indexOf(attribute) != attributes.size()-1) {
				tuple = tuple + attribute.getValue() + ",";
			}
			else if (attributes.indexOf(attribute) == attributes.size()-1) {
				tuple = tuple + attribute.getValue();
			}
		}
		return tuple;
	}	

	public static Tuple joinTuples(Tuple t1, Tuple t2, int a1, int a2) {
			Tuple joinTuple = new Tuple();
			int numberOfAttributesOfT1 = t1.getNumberOfAttributes();
			int numberOfAttributesOfT2 = t2.getNumberOfAttributes();
			joinTuple.setNumberOfAttributes(numberOfAttributesOfT1 + numberOfAttributesOfT2 - 1);
			
			int joinColumn = numberOfAttributesOfT1 - 1;
		
			int counter = 0;
			for (int i=0; i<numberOfAttributesOfT1; i++) {
				if (i != joinColumn) {
					if (i != a1) {
						joinTuple.attributes.add(t1.getAttribute(counter));
						counter++;
					}
					if (i == a1) {
						counter++;
						joinTuple.attributes.add(t1.getAttribute(counter));
						counter++;
					}
				}
				else if (i == joinColumn) {
					joinTuple.attributes.add(t1.getAttribute(a1));
					joinTuple.setAttributeName(joinColumn, t1.getRelationName() + a1
							+ "=" + t2.getRelationName() + a2);
				}
			}
			
			counter = numberOfAttributesOfT1;
			for (int i=numberOfAttributesOfT1; i<numberOfAttributesOfT1 + numberOfAttributesOfT2-1; i++) {
				if (i != joinColumn) {
					if (i-numberOfAttributesOfT1 != a2) {
						joinTuple.attributes.add(t2.getAttribute(counter-numberOfAttributesOfT1));
						counter++;
					}
					if (i-numberOfAttributesOfT1 == a2) {
						counter++;
						joinTuple.attributes.add(t2.getAttribute(counter-numberOfAttributesOfT1));
						counter++;
					}
				}
			}
			
			return joinTuple;
		}
}
		
				


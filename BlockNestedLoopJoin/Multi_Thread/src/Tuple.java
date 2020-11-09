import java.util.ArrayList;

public class Tuple {
    private String fieldsAsString       = null;
    private ArrayList<String> fields    = new ArrayList<String>();

    public  Tuple(String fields){
        if(fields != null){
            this.fieldsAsString = fields;
            for (String field :fields.split(",")){
                this.fields.add(field);
            }
        }
    }

    public String getField(int column){
        return fields.get(column);
    }

    public int getNumFields() {
        return fields.size();
    }

    public String getTupleAsString(){
        return fieldsAsString;
    }
}

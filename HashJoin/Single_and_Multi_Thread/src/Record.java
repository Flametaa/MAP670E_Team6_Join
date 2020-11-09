import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Record {

    List<String> columns;
    Map<String,Integer> keyToId;
    String name;

    public Record(String keys,String name){
        List<String> list = Arrays.asList(keys.split(","));
        Integer count = 0;
        keyToId = new HashMap<>();

        for (String key:list){
            keyToId.put(key,count);
            count++;
        }
        this.columns = list;
        this.name = name;

    }

}

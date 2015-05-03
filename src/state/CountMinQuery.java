package storm.starter.web.crawler.src.state;

import backtype.storm.tuple.Fields;
import org.json.simple.JSONArray;
import org.json.simple.JSONValue;
import org.json.simple.parser.ContainerFactory;
import org.json.simple.parser.JSONParser;
import storm.trident.state.BaseQueryFunction;
import storm.trident.tuple.TridentTuple;
import storm.trident.operation.TridentCollector;

import org.json.simple.parser.ParseException;

import java.io.Serializable;
import java.util.*;

import backtype.storm.tuple.Values;
import org.json.simple.JSONObject;


/**
 *@author: Abhishek Ravi (aravi5)
 */


public class CountMinQuery extends BaseQueryFunction<CountMinSketchState, String> {
    public List<String> batchRetrieve(CountMinSketchState state, List<TridentTuple> inputs) {
        List<String> ret = new ArrayList();
        PriorityQueue<String> myHeap = new PriorityQueue<String>(10, new SketchComparator<String>(state));

        for(TridentTuple input: inputs) {
            JSONParser parser = new JSONParser();
/*            String jsonString = input.getString(0);
            System.out.print(jsonString);*/

            List<Object> jsonObjectList = input.select(new Fields("Urls"));
            //System.out.println("DEBUG: jsonObjectList = "+jsonObjectList);

            for(Object jsonObject : jsonObjectList) {

                //System.out.println("DEBUG CLASS: "+jsonObject.getClass());

                if(jsonObject instanceof JSONObject) {
                   // System.out.println("DEBUG: It is a JSONObject");
                    JSONObject parsedJSON = (JSONObject)jsonObject;
                    String url = (String) parsedJSON.get("url");

                    myHeap.add(url);
                    //System.out.println("DEBUG: URL TO QUERY - " + url);
                    //long count = state.estimateCount(url);
                    //sb.append(count);
                }
            }
        }


        long count = 0;
        System.out.println("DEBUG - QUEUE " + myHeap.toString());
        while(!myHeap.isEmpty()) {
            String rankUrl = myHeap.poll();
            count = state.estimateCount(rankUrl);
            ret.add(rankUrl+","+count);
        }

        return ret;
    }

    public void execute(TridentTuple tuple, String count, TridentCollector collector) {
        collector.emit(new Values(count));
    }
}

class SketchComparator<T> implements Serializable, Comparator<T> {
    CountMinSketchState sketch;

    public SketchComparator(CountMinSketchState sketch) {
        this.sketch = sketch;
    }

    public int compare(T s1, T s2) {
        return (int)(sketch.estimateCount((String)s2) - sketch.estimateCount((String)s1));
    }
}
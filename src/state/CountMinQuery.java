package storm.starter.web.crawler.src.state;

import backtype.storm.tuple.Fields;

import org.json.simple.parser.JSONParser;
import storm.trident.state.BaseQueryFunction;
import storm.trident.tuple.TridentTuple;
import storm.trident.operation.TridentCollector;


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

        //Priority Queue to sort the URLs based on Page Rank - MAX HEAP
        PriorityQueue<String> myHeap = new PriorityQueue<String>(10, new SketchComparator<String>(state));

        for(TridentTuple input: inputs) {

            //Extract URLs in the request, input is a
            List<Object> jsonObjectList = input.select(new Fields("Urls"));

            for(Object jsonObject : jsonObjectList) {

                if(jsonObject instanceof JSONObject) {

                    //Parse URL from the JSON data
                    JSONObject parsedJSON = (JSONObject)jsonObject;
                    String url = (String) parsedJSON.get("url");

                    //Add URLs to heap to sort them in rank order
                    myHeap.add(url);
                }
            }
        }


        long count = 0;
        //Extract URLs, highest rank first
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

//Custom Comparator to sort elements in the order of their count in Count-min Sketch
class SketchComparator<T> implements Serializable, Comparator<T> {
    CountMinSketchState sketch;

    public SketchComparator(CountMinSketchState sketch) {
        this.sketch = sketch;
    }

    public int compare(T s1, T s2) {
        return (int)(sketch.estimateCount((String)s2) - sketch.estimateCount((String)s1));
    }
}
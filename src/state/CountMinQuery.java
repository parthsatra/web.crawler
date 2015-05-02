package storm.starter.web.crawler.src.state;

import org.json.simple.JSONArray;
import org.json.simple.JSONValue;
import org.json.simple.parser.ContainerFactory;
import org.json.simple.parser.JSONParser;
import storm.trident.state.BaseQueryFunction;
import storm.trident.tuple.TridentTuple;
import storm.trident.operation.TridentCollector;

import org.json.simple.parser.ParseException;
import java.util.*;

import backtype.storm.tuple.Values;
import org.json.simple.JSONObject;


/**
 *@author: Abhishek Ravi (aravi5)
 */


public class CountMinQuery extends BaseQueryFunction<CountMinSketchState, String> {
    public List<String> batchRetrieve(CountMinSketchState state, List<TridentTuple> inputs) {
        List<String> ret = new ArrayList();

        /* for(TridentTuple input: inputs) {
            ret.add(String.valueOf(state.estimateCount(input.getString(0))));
        }
        return ret;*/

        for(TridentTuple input: inputs) {
            JSONParser parser = new JSONParser();
            String jsonString = input.getString(0);


            try{
                /*JSONArray jsonArray = (JSONArray)parser.parse(jsonString);*/
                JSONObject jsonObject = (JSONObject)parser.parse(jsonString);
                String url = (String) jsonObject.get("_id");
                System.out.println("DEBUG: URL TO QUERY - "+ url);
                ret.add(String.valueOf(state.estimateCount(url)));


            } catch(ParseException pe) {
                System.out.println(pe);
            }

        }

        return ret;


    }

    public void execute(TridentTuple tuple, String count, TridentCollector collector) {
        collector.emit(new Values(count));
    }
}
package storm.starter.web.crawler.src.function;

import backtype.storm.tuple.Values;
import org.json.simple.JSONObject;
import org.mortbay.util.ajax.JSON;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/**
 * Created by Parth Satra on 5/2/15.
 */
public class PrepareESDocument extends BaseFunction {

    @Override
    public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {

        //Define the document that will be stored in ElasticSearch
        String id = JSONObject.escape(tridentTuple.getString(0));
        String categories = JSONObject.escape(tridentTuple.getString(1));

        String source = "{\"url\":\""+id+"\",\"categories\":\""+categories+"\"}";

        tridentCollector.emit(new Values(id, source));
    }
}

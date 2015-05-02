package storm.starter.web.crawler.src.function;

import backtype.storm.tuple.Values;
import org.elasticsearch.index.query.QueryBuilders;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/**
 * Created by Parth Satra on 5/2/15.
 */
public class GenerateQuery extends BaseFunction {

    @Override
    public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {
        String category = tridentTuple.getString(0);
        String query = QueryBuilders.termQuery("text", category).buildAsBytes().toUtf8();
        tridentCollector.emit(new Values(query, Constants.INDEX, Constants.TYPE));
    }
}

package storm.starter.web.crawler.src.function;

import backtype.storm.tuple.Values;
import org.elasticsearch.common.collect.Lists;
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

        //Create a QueryBuilder to query elasticSearch
        String query = QueryBuilders.termQuery("categories", category).buildAsBytes().toUtf8();
        tridentCollector.emit(new Values(query, Lists.newArrayList(Constants.INDEX), Lists.newArrayList(Constants.TYPE)));
    }
}

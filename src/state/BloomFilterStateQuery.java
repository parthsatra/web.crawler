package storm.starter.web.crawler.src.state;

import backtype.storm.tuple.Values;
import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseQueryFunction;
import storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by parthsatra on 5/1/15.
 */
public class BloomFilterStateQuery extends BaseQueryFunction<BloomFilterState, String> {
    @java.lang.Override
    public List<String> batchRetrieve(BloomFilterState bloomFilterState, java.util.List<TridentTuple> list) {
        StringBuilder sb = new StringBuilder();
        sb.append(",");
        List<String> ret = new ArrayList<String>();
        for(TridentTuple tuple : list) {
            String url = tuple.getString(0);
            sb.append(bloomFilterState.isPresent(url));
        }
        ret.add(sb.toString());
        return ret;
    }

    @java.lang.Override
    public void execute(TridentTuple tridentTuple, String string, TridentCollector tridentCollector) {
        tridentCollector.emit(new Values(string));
    }
}

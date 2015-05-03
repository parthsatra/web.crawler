package storm.starter.web.crawler.src.state;

import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseStateUpdater;
import storm.trident.tuple.TridentTuple;

import java.util.List;

/**
 * Created by Parth Satra on 5/1/15.
 */
public class BloomFilterStateUpdater extends BaseStateUpdater<BloomFilterState> {


    @Override
    public void updateState(BloomFilterState bloomFilterState, List<TridentTuple> list, TridentCollector tridentCollector) {
        //add new URLs to BloomFilter to prevent loop
        for(TridentTuple tuple : list) {
            bloomFilterState.add(tuple.getString(0));
        }
    }
}

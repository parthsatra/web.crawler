package storm.starter.web.crawler.src.state;

import backtype.storm.task.IMetricsContext;
import storm.trident.state.State;
import storm.trident.state.StateFactory;

import java.util.Map;

/**
 * Created by Parth Satra on 5/1/15.
 */
public class BloomFilterStateFactory implements StateFactory {

    private BloomFilterState state;

    public BloomFilterStateFactory(int size) {
        state = new BloomFilterState(size);
    }

    @Override
    public State makeState(Map map, IMetricsContext iMetricsContext, int i, int i1) {
        return state;
    }
}

package storm.starter.web.crawler.src.state;

import storm.trident.state.BaseStateUpdater;
import storm.trident.tuple.TridentTuple;
import storm.trident.operation.TridentCollector;
import java.util.List;


/**
 *@author: aravi5
 */

public class CountMinSketchUpdater extends BaseStateUpdater<CountMinSketchState> {
    public void updateState(CountMinSketchState state, List<TridentTuple> tuples, TridentCollector collector) {

        for(TridentTuple t: tuples) {

            //Every tuple is a list of URLs
            List<Object> urlList = t.getValues();
            for(Object url : urlList) {
                //Add the count for each URL
                state.add((String)url,1);
            }

        }

    }
}
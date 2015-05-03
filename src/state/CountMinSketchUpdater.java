package storm.starter.web.crawler.src.state;

import storm.trident.state.BaseStateUpdater;
import storm.trident.tuple.TridentTuple;
import storm.trident.operation.TridentCollector;
import java.util.List;


/**
 *@author: Abhishek Ravi (aravi5)
 */

public class CountMinSketchUpdater extends BaseStateUpdater<CountMinSketchState> {
    public void updateState(CountMinSketchState state, List<TridentTuple> tuples, TridentCollector collector) {

        for(TridentTuple t: tuples) {

            String[] stringArray = t.getString(0).split(" ");
            //For each link present in current URL, increment in-degree
            for(String s : stringArray) {
                if(!(s.trim().isEmpty())) {
                    state.add(s,1);
                }
            }

        }

    }
}
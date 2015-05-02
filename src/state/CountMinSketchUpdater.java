package storm.starter.web.crawler.src.state;

import backtype.storm.tuple.Fields;
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

//            System.out.println("DEBUG: TUPLE - " + t.toString());
            //
            ////Every tuple is a list of URLs
            //List<Object> urlList = t.select(new Fields("outgoingUrls"));
            //
            //for(Object url : urlList) {
            ////Add the count for each URL
            //System.out.println("DEBUG: URL - " + url.toString());
            //if(url instanceof String) {
            //System.out.println("DEBUG: Is an instance of String");
            //state.add((String)url,1);
            //}

            String[] stringArray = t.getString(0).split(" ");

            for(String s : stringArray) {
                if(!(s.trim().isEmpty())) {
                    state.add(s,1);
                }
        }

        }

    }
}
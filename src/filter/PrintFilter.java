package storm.starter.web.crawler.src.filter;

import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;

/**
 * Created by parthsatra on 5/2/15.
 */
public class PrintFilter extends BaseFilter {
    @Override
    public boolean isKeep(TridentTuple tridentTuple) {
        System.out.println("DEBUG Print : " + tridentTuple);
        return true;
    }
}

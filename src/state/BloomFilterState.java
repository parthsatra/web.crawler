package storm.starter.web.crawler.src.state;

import com.google.common.base.Charsets;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import storm.trident.state.State;

import java.io.Serializable;

/**
 * Created by Parth Satra on 5/1/15.
 */
public class BloomFilterState implements State, Serializable{

    private static final long serialVersionUID = 1L;
    private BloomFilter filter = BloomFilter.create(Funnels.byteArrayFunnel(), 1000, 0.01);

    public BloomFilterState(int size) {
        filter = BloomFilter.create(Funnels.byteArrayFunnel(), size, 0.01);
    }

    public void add(String url) {
        filter.put(url.getBytes(Charsets.UTF_8));
    }

    public boolean isPresent(String url) {
        return filter.mightContain(url.getBytes(Charsets.UTF_8));
    }

    @Override
    public void beginCommit(Long aLong) {

    }

    @Override
    public void commit(Long aLong) {

    }
}

package storm.starter.web.crawler.src.dataclasses;

/**
 * Created by Abidaan on 5/2/15.
 */
public class PageData {
    private String baseurl;
    private String[] urls;
    private String[] categories;

    public PageData() {

    }

    public PageData(String baseurl, String[] urls, String[] categories) {
        this.baseurl = baseurl;
        this.urls = urls;
        this.categories = categories;
    }

    public String getBaseurl() {
        return baseurl;
    }

    public void setBaseurl(String baseurl) {
        this.baseurl = baseurl;
    }

    public String[] getUrls() {
        return urls;
    }

    public void setUrls(String[] urls) {
        this.urls = urls;
    }

    public String[] getCategories() {
        return categories;
    }

    public void setCategories(String[] categories) {
        this.categories = categories;
    }
}

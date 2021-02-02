package com.athome.config;

import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @Author zhangxw03
 * @Dat 2021-02-02 12:57
 * @Describe
 */
public class EsClient2 {

    private static final HttpHost[] HTTP_HOSTS = {
            new HttpHost("39.102.61.252", 9200, "http")
    };

    private RestHighLevelClient restHighLevelClient = null;

    @Before
    public void init() {
        restHighLevelClient = new RestHighLevelClient(RestClient.builder(HTTP_HOSTS));
    }

    @After
    public void close() {
        if (restHighLevelClient != null) {
            try {
                restHighLevelClient.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 批量增加
     */
    @Test
    public void test() throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(new IndexRequest("ik").id("10").source(XContentType.JSON, "name", "须知但", "age", "2"));
        bulkRequest.add(new IndexRequest("ik").id("11").source(XContentType.JSON, "name", "莫须有", "age", "3"));
        bulkRequest.add(new IndexRequest("ik").id("12").source(XContentType.JSON, "name", "需只有", "age", "2"));
        bulkRequest.add(new IndexRequest("ik").id("13").source(XContentType.JSON, "name", "许多事", "age", "5"));
        BulkResponse bulk = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        System.out.println("******" + bulk);
    }

    /**
     * 查询所有
     */
    @Test
    public void test1() throws IOException {

        SearchRequest searchRequest = new SearchRequest("ik");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchRequest.source(searchSourceBuilder);
        SearchResponse search = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        long value = search.getHits().getTotalHits().value;
        System.out.println("******总的条数" + value);

        //查出来总的数据
        SearchHit[] hits = search.getHits().getHits();
        //遍历查出来的数据
        for (SearchHit hit : hits) {
            System.out.println(hit.getIndex());
            System.out.println(hit.getId());
            System.out.println(hit.getScore());
            System.out.println("------------------------------------");
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();

            sourceAsMap.forEach((k, v) -> {
                System.out.println("k：" + k);
                System.out.println("v:" + v);
            });
        }
    }

    /**
     * 匹配查询
     */
    @Test
    public void test2() throws IOException {

        SearchRequest searchRequest = new SearchRequest("ik");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.multiMatchQuery("莫须有", "name"));
        searchRequest.source(searchSourceBuilder);
        SearchResponse search = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHit[] hits = search.getHits().getHits();
        System.out.println(search.getHits().getTotalHits().value);

        for (SearchHit hit : hits) {
            System.out.println(hit.getIndex());
            System.out.println(hit.getId());
            System.out.println(hit.getScore());

            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            sourceAsMap.forEach((k, v) -> {
                System.out.println(k + "-------" + v);
            });
        }

    }

    /**
     * 分页查询
     */
    @Test
    public void test3() throws IOException {
        SearchRequest searchRequest = new SearchRequest("ik");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.from(0).size(2).sort("age", SortOrder.ASC);
        searchSourceBuilder.query(QueryBuilders.multiMatchQuery("莫须有", "name"));
        searchRequest.source(searchSourceBuilder);

        SearchResponse search = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println(search.getHits().getTotalHits().value);

        for (SearchHit documentFields : search.getHits().getHits()) {
            System.out.println(documentFields.getScore());
            Map<String, Object> sourceAsMap = documentFields.getSourceAsMap();

            sourceAsMap.forEach((k, v) -> {
                System.out.println(k + "---------" + v);
            });
        }

    }

    /**
     * 分页查询，高亮
     */
    @Test
    public void test4() throws IOException {
        SearchRequest searchRequest = new SearchRequest("ik");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.from(0).size(2);
        searchSourceBuilder.query(QueryBuilders.multiMatchQuery("莫须有", "name"));

        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.field("name").preTags("\"<span style='color:red;'>\"").postTags("\"</span>\"");
        searchSourceBuilder.highlighter(highlightBuilder);

        searchRequest.source(searchSourceBuilder);

        SearchResponse search = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println(search.getHits().getTotalHits().value);

        for (SearchHit documentFields : search.getHits().getHits()) {
            System.out.println(documentFields.getScore());
            Map<String, Object> sourceAsMap = documentFields.getSourceAsMap();

            sourceAsMap.forEach((k, v) -> {
                System.out.println(k + "---------" + v);
            });
        }

    }
}

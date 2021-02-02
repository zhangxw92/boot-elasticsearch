package com.athome.config;

import org.apache.http.HttpHost;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
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
public class EsClient {

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
     * 添加数据
     */
    @Test
    public void test() {
        Map<String, Object> map = new HashMap<>();
        map.put("name", "zhangsan");
        map.put("age", 12);
        IndexRequest indexRequest = new IndexRequest().index("ik").id("5").source(map);

        try {
            IndexResponse index = restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
            System.out.println(index);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 查询数据
     */
    @Test
    public void test02() throws IOException {
        GetRequest getRequest = new GetRequest("ik", "5");
        GetResponse documentFields = restHighLevelClient.get(getRequest, RequestOptions.DEFAULT);
        System.out.println("&&&&&&&&&&" + documentFields);
    }

    /**
     * 修改数据
     *
     * @throws IOException
     */
    @Test
    public void test03() throws IOException {
        Map<String, Object> map = new HashMap<>();
        map.put("name", "wangwu");
        map.put("age", 22);
        UpdateRequest updateRequest = new UpdateRequest("ik", "5").doc(map);

        UpdateResponse update = restHighLevelClient.update(updateRequest, RequestOptions.DEFAULT);
        System.out.println("&&&&&&&&&&" + update);
    }

    /**
     * 删除数据
     *
     * @throws IOException
     */
    @Test
    public void test04() throws IOException {

        DeleteRequest deleteRequest = new DeleteRequest("ik", "5");

        DeleteResponse delete = restHighLevelClient.delete(deleteRequest, RequestOptions.DEFAULT);
        System.out.println("&&&&&&&&&&" + delete);
    }
}

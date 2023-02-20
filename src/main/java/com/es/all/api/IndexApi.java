package com.es.all.api;


import com.es.all.EsStudyApplication;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;

/**
 * @Author qcl
 * @Description
 * @Date 10:48 AM 2/20/2023
 */
@Slf4j
@RunWith(SpringRunner.class)
@SpringBootTest(classes = { EsStudyApplication.class })
public class IndexApi {

    /**
     * es 索引
     */
    public static final String index = "study";

    @Autowired
    private RestHighLevelClient client;

    @Test
    public void ping() throws IOException {
        if(client.ping(RequestOptions.DEFAULT)) {
            log.info("链接成功");
        }else {
            log.info("链接失败 !");
        }
    }

    /**
     * 创建索引
     */
    //@Test
    public void createIndex() throws IOException {

        // log.info("创建索引 ===> "+ JSONObject.toJSONString(response));
    }

    // @Test
    public void searchIndex() throws IOException {

        //  log.info("查询索引 ===> {}", indexResponse);
    }
}
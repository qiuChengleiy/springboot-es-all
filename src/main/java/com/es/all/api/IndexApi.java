package com.es.all.api;


import com.alibaba.fastjson.JSONObject;
import com.es.all.EsStudyApplication;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
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
    @Test
    public void createIndex() throws IOException {
        CreateIndexRequest request = new CreateIndexRequest(index);

        // index settings
        request.settings(
                Settings.builder()
                        .put("index.number_of_shards", 3)
                        .put("index.number_of_replicas", 2)
        );

        // alias
        request.alias(new Alias("study_alias"));

        // index mappings
//        {
//            "mapping": {
//            "_doc": {
//                "properties": {
//                    "name": {
//                        "type": "text"
//                    }
//                }
//            }
//        }
//        }
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.startObject("properties");
            {
                builder.startObject("name");
                {
                    builder.field("type", "text");
                }
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();
        request.mapping(builder);

        // 请求设置
        request.setTimeout(TimeValue.timeValueMinutes(1));

        CreateIndexResponse createIndexResponse = client.indices().create(request, RequestOptions.DEFAULT);
        log.info("创建索引 ===> "+ JSONObject.toJSONString(createIndexResponse)); // 创建索引 ===> {"acknowledged":true,"fragment":false,"shardsAcknowledged":true}
    }

    /**
     * 判断索引是否存在
     * @throws IOException
     */
    @Test
    public void existIndex() throws IOException {
        GetIndexRequest request = new GetIndexRequest(index);
        boolean exists = client.indices().exists(request, RequestOptions.DEFAULT);
        log.info("索引{}存在 ===> {}", index, exists);
    }

    /**
     * 删除索引
     * @throws IOException
     */
    @Test
    public void delIndex() throws IOException {
        DeleteIndexRequest request = new DeleteIndexRequest(index);
        AcknowledgedResponse delete = client.indices().delete(request, RequestOptions.DEFAULT);
        log.info("删除索引 ===> {}", JSONObject.toJSONString(delete)); // 删除索引 ===> {"acknowledged":true,"fragment":false}
    }
}
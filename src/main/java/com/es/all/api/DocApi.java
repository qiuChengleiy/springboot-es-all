package com.es.all.api;

import com.es.all.EsStudyApplication;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @Author qcl
 * @Description
 * @Date 10:17 AM 2/28/2023
 */
@Slf4j
@RunWith(SpringRunner.class)
@SpringBootTest(classes = { EsStudyApplication.class })
public class DocApi {
    /**
     * es 索引
     */
    public static final String index = "req_log";

    @Autowired
    private RestHighLevelClient client;

    /**
     * 新增 json方式
     * @throws IOException
     */
    @Test
    public void add() throws IOException {
        IndexRequest request = new IndexRequest(index);

        // 指定id
        request.id("1");
        String jsonString = "{" +
                "\"method\":\"POST\"," +
                "\"times\":\"60\"," +
                "\"path\":\"/api/post/1/update\"," +
                "\"created\":\"2023-02-28\"" +
                "}";
        request.source(jsonString, XContentType.JSON);

        request.timeout(TimeValue.timeValueSeconds(1));
        request.timeout("1s");

        client.index(request, RequestOptions.DEFAULT);
    }

    /**
     * 新增 map方式
     * @throws IOException
     */
    @Test
    public void add1() throws IOException {
        IndexRequest request = new IndexRequest(index);
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("method", "POST");
        jsonMap.put("times", "40");
        jsonMap.put("path", "/api/post/2/update");
        jsonMap.put("created", "2023-02-28");
        request.id("2").source(jsonMap);


        request.timeout(TimeValue.timeValueSeconds(1));
        request.timeout("1s");

        client.index(request, RequestOptions.DEFAULT);
    }

    /**
     * 新增 builder
     * @throws IOException
     */
    @Test
    public void add2() throws IOException {
        IndexRequest request = new IndexRequest(index);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.field("method", "POST");
            builder.field("times", "90");
            builder.field("path", "/api/post/3/update");
            builder.timeField("created", "2023-02-28");
        }
        builder.endObject();
        request.id("3").source(builder);

        request.timeout(TimeValue.timeValueSeconds(1));
        request.timeout("1s");

        client.index(request, RequestOptions.DEFAULT);
    }

    /**
     * 更新 json方式
     * 其它方式与新增类似 只不过request不一样
     * @throws IOException
     */
    @Test
    public void update() throws IOException {
        UpdateRequest request = new UpdateRequest(index, "1");
        String jsonString = "{" +
                "\"method\":\"POST\"," +
                "\"times\":\"60\"," +
                "\"path\":\"/api/post/1/update1\"," +
                "\"created\":\"2023-02-28\"" +
                "}";
        request.doc(jsonString, XContentType.JSON);

        request.timeout(TimeValue.timeValueSeconds(1));
        request.timeout("1s");

        client.update(request, RequestOptions.DEFAULT);
    }

    /**
     * 删除操作
     * @throws IOException
     */
    @Test
    public void delete() throws IOException {
        DeleteRequest request = new DeleteRequest(index, "1");
        request.timeout(TimeValue.timeValueSeconds(1));
        request.timeout("1s");
        client.delete(request, RequestOptions.DEFAULT);
    }

    /**
     * 查询
     * @throws IOException
     */
    @Test
    public void fetch() throws IOException {
        GetRequest request = new GetRequest(index, "1");

        // request的一些可选参数:
        // 禁用源检索，默认启用
        request.fetchSourceContext(FetchSourceContext.DO_NOT_FETCH_SOURCE);


        // 配置特定字段的源包含
        String[] includes = new String[]{"method", "path"};
        String[] excludes = Strings.EMPTY_ARRAY;
        FetchSourceContext fetchSourceContext =
                new FetchSourceContext(true, includes, excludes);
        request.fetchSourceContext(fetchSourceContext);

        // 配置特定字段的源排除 调换一下
//        String[] includes = Strings.EMPTY_ARRAY;
//        String[] excludes = new String[]{"method", "path"};


        // 在检索文档之前执行刷新 默认 false
        request.refresh(true);

        // 指定版本
        //request.version(2);

        try {
            // 默认下 get 是同步执行
            GetResponse getResponse = client.get(request, RequestOptions.DEFAULT);

            // 异步执行
            client.getAsync(request, RequestOptions.DEFAULT, listener);

            // 获取索引名称
            String index = getResponse.getIndex();
            log.info("index >>>> {}", index); // index >>>> req_log

            if(getResponse.isExists()) {
                // 获取version
                long version = getResponse.getVersion();
                log.info("version >>>> {}", version); // version >>>> 4

                // 获取文档数据
                String sourceAsString = getResponse.getSourceAsString();
                log.info("sourceAsString >>>> {}", sourceAsString); // sourceAsString >>>> {"path":"/api/post/1/update1","times":"60","method":"POST","created":"2023-02-28"}

                // map结构的文档
                Map<String, Object> sourceAsMap = getResponse.getSourceAsMap();
                log.info("sourceAsMap >>>> {}", sourceAsMap); // {path=/api/post/1/update1, times=60, method=POST, created=2023-02-28}

                // byte[]格式
                byte[] sourceAsBytes = getResponse.getSourceAsBytes();
            }else {
                log.error("response is not exists");
            }
        } catch (ElasticsearchException e) {
            // 404 状态码处理
            if (e.status() == RestStatus.NOT_FOUND) {
                log.error(e.getMessage());
            }
        }
        for(;;){}
    }

    /**
     * 监听异步响应
     */
    ActionListener<GetResponse> listener = new ActionListener<GetResponse>() {
        @Override
        public void onResponse(GetResponse getResponse) {
            log.info("异步回调>>>>");
            // 获取索引名称
            String index = getResponse.getIndex();
            log.info("index >>>> {}", index); // index >>>> req_log
        }

        @Override
        public void onFailure(Exception e) {
            log.error(e.getMessage());
        }
    };


    /**
     * 批量请求
     * @throws IOException
     */
    @Test
    public void bulk() throws IOException {
        BulkRequest request = new BulkRequest();

        // 新增
        String jsonString = "{" +
                "\"method\":\"POST\"," +
                "\"times\":\"60\"," +
                "\"path\":\"/api/post/5/update\"," +
                "\"created\":\"2023-02-28\"" +
                "}";
        request.add(new IndexRequest(index).source(jsonString, XContentType.JSON));

        String jsonString1 = "{" +
                "\"method\":\"POST\"," +
                "\"times\":\"60\"," +
                "\"path\":\"/api/post/6/update\"," +
                "\"created\":\"2023-02-28\"" +
                "}";
        request.add(new IndexRequest(index).source(jsonString1, XContentType.JSON));

        // 删除
        request.add(new DeleteRequest(index, "3"));


        // 更新
        String jsonString2 = "{" +
                "\"method\":\"POST\"," +
                "\"times\":\"60\"," +
                "\"path\":\"/api/post/1/update1\"," +
                "\"created\":\"2023-02-28\"" +
                "}";
        request.add(new UpdateRequest(index, "1").doc(jsonString2, XContentType.JSON));
        client.bulk(request, RequestOptions.DEFAULT);
    }

}

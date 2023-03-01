package com.es.all.api;

import com.es.all.EsStudyApplication;
import lombok.extern.slf4j.Slf4j;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @Author qcl
 * @Description 文档高级查询
 * @Date 9:27 AM 3/1/2023
 */
@Slf4j
@RunWith(SpringRunner.class)
@SpringBootTest(classes = { EsStudyApplication.class })
public class DocSearchApi {
    /**
     * es 索引
     */
    public static final String index = "req_log";

    @Autowired
    private RestHighLevelClient client;

    /**
     * 通过查询进行更新
     * @throws IOException
     */
    @Test
    public void updateByQuery() throws IOException {
        // 构建UpdateByQueryRequest。可以使用QueryBuilder来构建查询条件，然后将其添加到UpdateByQueryRequest中
        UpdateByQueryRequest request = new UpdateByQueryRequest(index);
        request.setQuery(QueryBuilders.matchQuery("path", "/api/post/4/update"));

        // 更新times字段 +10 因为之前我建的times是text类型，所以这里加完之后为 '12,010'
        request.setScript(new Script(ScriptType.INLINE, "painless", "ctx._source.times += params.count", Collections.singletonMap("count", 10)));

        BulkByScrollResponse response = client.updateByQuery(request, RequestOptions.DEFAULT);
        if(!response.isTimedOut()) {
            log.info("status >>>> {}", String.valueOf(response.getStatus()));
            // BulkIndexByScrollResponse[sliceId=null,updated=1,created=0,deleted=0,batches=1,versionConflicts=0,noops=0,retries=0,throttledUntil=0s]
        }
    }

    /**
     * 查询全部
     * @throws IOException
     */
    @Test
    public void searchAll() throws IOException {
        SearchRequest searchRequest = new SearchRequest(index);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchRequest.source(searchSourceBuilder);
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        if(!response.isTimedOut()) {
            log.info("status >>>> {}", response.status());


            SearchHits hits = response.getHits();
            TotalHits totalHits = hits.getTotalHits();
            long numHits = totalHits.value;
            log.info("numHits >>>> {}", numHits);

            TotalHits.Relation relation = totalHits.relation;
            float maxScore = hits.getMaxScore();
            log.info("maxScore >>>> {}", maxScore);


            SearchHit[] searchHits = hits.getHits();
            for (SearchHit hit : searchHits) {
                String index = hit.getIndex();
                log.info("index >>>> {}", index);

                String id = hit.getId();
                log.info("id >>>> {}", id);

                float score = hit.getScore();
                log.info("score >>>> {}", score);


                String sourceAsString = hit.getSourceAsString();
                log.info("sourceAsString >>>> {}", sourceAsString);


                Map<String, Object> sourceAsMap = hit.getSourceAsMap();
                String path = (String) sourceAsMap.get("path");
                log.info("path >>>> {}", path);
            }
        }

//        2023-03-01 10:21:04.977  INFO 15872 --- [           main] com.es.all.api.DocSearchApi              : status >>>> OK
//        2023-03-01 10:21:04.978  INFO 15872 --- [           main] com.es.all.api.DocSearchApi              : numHits >>>> 29
//        2023-03-01 10:21:04.978  INFO 15872 --- [           main] com.es.all.api.DocSearchApi              : maxScore >>>> 1.0
//        2023-03-01 10:21:04.978  INFO 15872 --- [           main] com.es.all.api.DocSearchApi              : index >>>> req_log
//        2023-03-01 10:21:04.978  INFO 15872 --- [           main] com.es.all.api.DocSearchApi              : id >>>> GUK3NIYBdXrpvlCF01bz
//        2023-03-01 10:21:04.978  INFO 15872 --- [           main] com.es.all.api.DocSearchApi              : score >>>> 1.0
//        2023-03-01 10:21:04.981  INFO 15872 --- [           main] com.es.all.api.DocSearchApi              : sourceAsString >>>> {"times":80,"method":"GET","path":"/api/post/1","created":"2023-02-09"}
//        2023-03-01 10:21:04.987  INFO 15872 --- [           main] com.es.all.api.DocSearchApi              : path >>>> /api/post/1
//        2023-03-01 10:21:04.987  INFO 15872 --- [           main] com.es.all.api.DocSearchApi              : index >>>> req_log
//        2023-03-01 10:21:04.987  INFO 15872 --- [           main] com.es.all.api.DocSearchApi              : id >>>> GkK3NIYBdXrpvlCF01bz
//        2023-03-01 10:21:04.987  INFO 15872 --- [           main] com.es.all.api.DocSearchApi              : score >>>> 1.0
//        2023-03-01 10:21:04.987  INFO 15872 --- [           main] com.es.all.api.DocSearchApi              : sourceAsString >>>> {"times":30,"method":"GET","path":"/api/post/2","created":"2023-02-07"}
//        2023-03-01 10:21:04.987  INFO 15872 --- [           main] com.es.all.api.DocSearchApi              : path >>>> /api/post/2
//        2023-03-01 10:21:04.987  INFO 15872 --- [           main] com.es.all.api.DocSearchApi              : index >>>> req_log
//        2023-03-01 10:21:04.987  INFO 15872 --- [           main] com.es.all.api.DocSearchApi              : id >>>> G0K3NIYBdXrpvlCF01bz
//        2023-03-01 10:21:04.987  INFO 15872 --- [           main] com.es.all.api.DocSearchApi              : score >>>> 1.0
//        2023-03-01 10:21:04.987  INFO 15872 --- [           main] com.es.all.api.DocSearchApi              : sourceAsString >>>> {"times":20,"method":"GET","path":"/api/post/3","created":"2023-02-08"}
//        2023-03-01 10:21:04.987  INFO 15872 --- [           main] com.es.all.api.DocSearchApi              : path >>>> /api/post/3
//        2023-03-01 10:21:04.987  INFO 15872 --- [           main] com.es.all.api.DocSearchApi              : index >>>> req_log
//        2023-03-01 10:21:04.988  INFO 15872 --- [           main] com.es.all.api.DocSearchApi              : id >>>> HEK3NIYBdXrpvlCF01bz
//        2023-03-01 10:21:04.988  INFO 15872 --- [           main] com.es.all.api.DocSearchApi              : score >>>> 1.0
//        2023-03-01 10:21:04.988  INFO 15872 --- [           main] com.es.all.api.DocSearchApi              : sourceAsString >>>> {"times":120,"method":"GET","path":"/api/post/20","created":"2023-02-06"}
//        2023-03-01 10:21:04.988  INFO 15872 --- [           main] com.es.all.api.DocSearchApi              : path >>>> /api/post/20
//        2023-03-01 10:21:04.988  INFO 15872 --- [           main] com.es.all.api.DocSearchApi              : index >>>> req_log
//        2023-03-01 10:21:04.988  INFO 15872 --- [           main] com.es.all.api.DocSearchApi              : id >>>> HUK3NIYBdXrpvlCF01bz
//        2023-03-01 10:21:04.988  INFO 15872 --- [           main] com.es.all.api.DocSearchApi              : score >>>> 1.0
//        2023-03-01 10:21:04.988  INFO 15872 --- [           main] com.es.all.api.DocSearchApi              : sourceAsString >>>> {"times":150,"method":"GET","path":"/api/post/1","created":"2023-02-05"}
//        2023-03-01 10:21:04.988  INFO 15872 --- [           main] com.es.all.api.DocSearchApi              : path >>>> /api/post/1
//        2023-03-01 10:21:04.988  INFO 15872 --- [           main] com.es.all.api.DocSearchApi              : index >>>> req_log
//        2023-03-01 10:21:04.988  INFO 15872 --- [           main] com.es.all.api.DocSearchApi              : id >>>> HkK3NIYBdXrpvlCF01bz
//        2023-03-01 10:21:04.988  INFO 15872 --- [           main] com.es.all.api.DocSearchApi              : score >>>> 1.0
//        2023-03-01 10:21:04.988  INFO 15872 --- [           main] com.es.all.api.DocSearchApi              : sourceAsString >>>> {"times":80,"method":"GET","path":"/api/post/3","created":"2023-02-04"}
//        2023-03-01 10:21:04.988  INFO 15872 --- [           main] com.es.all.api.DocSearchApi              : path >>>> /api/post/3
//        2023-03-01 10:21:04.989  INFO 15872 --- [           main] com.es.all.api.DocSearchApi              : index >>>> req_log
//        2023-03-01 10:21:04.989  INFO 15872 --- [           main] com.es.all.api.DocSearchApi              : id >>>> H0K3NIYBdXrpvlCF01bz
//        2023-03-01 10:21:04.989  INFO 15872 --- [           main] com.es.all.api.DocSearchApi              : score >>>> 1.0
//        2023-03-01 10:21:04.989  INFO 15872 --- [           main] com.es.all.api.DocSearchApi              : sourceAsString >>>> {"times":960,"method":"GET","path":"/api/post/6","created":"2023-02-03"}
//        2023-03-01 10:21:04.990  INFO 15872 --- [           main] com.es.all.api.DocSearchApi              : path >>>> /api/post/6
//        2023-03-01 10:21:04.990  INFO 15872 --- [           main] com.es.all.api.DocSearchApi              : index >>>> req_log
//        2023-03-01 10:21:04.990  INFO 15872 --- [           main] com.es.all.api.DocSearchApi              : id >>>> IEK3NIYBdXrpvlCF01bz
//        2023-03-01 10:21:04.990  INFO 15872 --- [           main] com.es.all.api.DocSearchApi              : score >>>> 1.0
//        2023-03-01 10:21:04.990  INFO 15872 --- [           main] com.es.all.api.DocSearchApi              : sourceAsString >>>> {"times":9000,"method":"GET","path":"/api/post/8","created":"2023-02-02"}
//        2023-03-01 10:21:04.990  INFO 15872 --- [           main] com.es.all.api.DocSearchApi              : path >>>> /api/post/8
//        2023-03-01 10:21:04.990  INFO 15872 --- [           main] com.es.all.api.DocSearchApi              : index >>>> req_log
//        2023-03-01 10:21:04.990  INFO 15872 --- [           main] com.es.all.api.DocSearchApi              : id >>>> IUK3NIYBdXrpvlCF01bz
//        2023-03-01 10:21:04.990  INFO 15872 --- [           main] com.es.all.api.DocSearchApi              : score >>>> 1.0
//        2023-03-01 10:21:04.990  INFO 15872 --- [           main] com.es.all.api.DocSearchApi              : sourceAsString >>>> {"times":1300,"method":"GET","path":"/api/post/6","created":"2023-02-01"}
//        2023-03-01 10:21:04.990  INFO 15872 --- [           main] com.es.all.api.DocSearchApi              : path >>>> /api/post/6
//        2023-03-01 10:21:04.990  INFO 15872 --- [           main] com.es.all.api.DocSearchApi              : index >>>> req_log
//        2023-03-01 10:21:04.990  INFO 15872 --- [           main] com.es.all.api.DocSearchApi              : id >>>> IkK3NIYBdXrpvlCF01bz
//        2023-03-01 10:21:04.990  INFO 15872 --- [           main] com.es.all.api.DocSearchApi              : score >>>> 1.0
//        2023-03-01 10:21:04.990  INFO 15872 --- [           main] com.es.all.api.DocSearchApi              : sourceAsString >>>> {"times":400,"method":"GET","path":"/api/post/4","created":"2023-02-10"}
//        2023-03-01 10:21:04.990  INFO 15872 --- [           main] com.es.all.api.DocSearchApi              : path >>>> /api/post/4

        // 异步查询
//        client.searchAsync(searchRequest, RequestOptions.DEFAULT, listener);
//        ActionListener<SearchResponse> listener = new ActionListener<SearchResponse>() {
//            @Override
//            public void onResponse(SearchResponse searchResponse) {
//
//            }
//
//            @Override
//            public void onFailure(Exception e) {
//
//            }
//        };
    }


    /**
     * 分页查询
     * @throws IOException
     */
    @Test
    public void searchByPage() throws IOException {
        SearchRequest searchRequest = new SearchRequest(index);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());

        // 分页
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(5);
        searchSourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));

        searchRequest.source(searchSourceBuilder);
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        // ....
    }


    /**
     * 分页查询
     * @throws IOException
     */
    @Test
    public void searchByQuery() throws IOException {
        SearchRequest searchRequest = new SearchRequest(index);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // 精确匹配
        QueryBuilder termQueryBuilder = QueryBuilders.termQuery("method", "POST");

        // 范围查询
        QueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("times").from(0).to(100);
        // 组合条件查询
        QueryBuilder boolQueryBuilder = QueryBuilders.boolQuery()
                .must(termQueryBuilder)
                .filter(rangeQueryBuilder);

        searchSourceBuilder.query(boolQueryBuilder);

        searchRequest.source(searchSourceBuilder);
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        if(!response.isTimedOut()) {
            log.info("status >>>> {}", response.status());


            SearchHits hits = response.getHits();
            TotalHits totalHits = hits.getTotalHits();
            long numHits = totalHits.value;
            log.info("numHits >>>> {}", numHits);

            TotalHits.Relation relation = totalHits.relation;
            float maxScore = hits.getMaxScore();
            log.info("maxScore >>>> {}", maxScore);


            SearchHit[] searchHits = hits.getHits();
            for (SearchHit hit : searchHits) {
                String index = hit.getIndex();
                log.info("index >>>> {}", index);

                String id = hit.getId();
                log.info("id >>>> {}", id);

                float score = hit.getScore();
                log.info("score >>>> {}", score);


                String sourceAsString = hit.getSourceAsString();
                log.info("sourceAsString >>>> {}", sourceAsString);


                Map<String, Object> sourceAsMap = hit.getSourceAsMap();
                String path = (String) sourceAsMap.get("path");
                log.info("path >>>> {}", path);
            }
        }

//        2023-03-01 10:54:02.706  INFO 17484 --- [           main] com.es.all.api.DocSearchApi              : status >>>> OK
//        2023-03-01 10:54:02.707  INFO 17484 --- [           main] com.es.all.api.DocSearchApi              : numHits >>>> 5
//        2023-03-01 10:54:02.708  INFO 17484 --- [           main] com.es.all.api.DocSearchApi              : maxScore >>>> 1.2144442
//        2023-03-01 10:54:02.708  INFO 17484 --- [           main] com.es.all.api.DocSearchApi              : index >>>> req_log
//        2023-03-01 10:54:02.708  INFO 17484 --- [           main] com.es.all.api.DocSearchApi              : id >>>> 2
//        2023-03-01 10:54:02.708  INFO 17484 --- [           main] com.es.all.api.DocSearchApi              : score >>>> 1.2144442
//        2023-03-01 10:54:02.713  INFO 17484 --- [           main] com.es.all.api.DocSearchApi              : sourceAsString >>>> {"path":"/api/post/2/update","times":"40","method":"POST","created":"2023-02-28"}
//        2023-03-01 10:54:02.723  INFO 17484 --- [           main] com.es.all.api.DocSearchApi              : path >>>> /api/post/2/update
//        2023-03-01 10:54:02.723  INFO 17484 --- [           main] com.es.all.api.DocSearchApi              : index >>>> req_log
//        2023-03-01 10:54:02.723  INFO 17484 --- [           main] com.es.all.api.DocSearchApi              : id >>>> 3
//        2023-03-01 10:54:02.723  INFO 17484 --- [           main] com.es.all.api.DocSearchApi              : score >>>> 1.2144442
//        2023-03-01 10:54:02.723  INFO 17484 --- [           main] com.es.all.api.DocSearchApi              : sourceAsString >>>> {"method":"POST","times":"90","path":"/api/post/3/update","created":"2023-02-28"}
//        2023-03-01 10:54:02.723  INFO 17484 --- [           main] com.es.all.api.DocSearchApi              : path >>>> /api/post/3/update
//        2023-03-01 10:54:02.723  INFO 17484 --- [           main] com.es.all.api.DocSearchApi              : index >>>> req_log
//        2023-03-01 10:54:02.723  INFO 17484 --- [           main] com.es.all.api.DocSearchApi              : id >>>> 1
//        2023-03-01 10:54:02.723  INFO 17484 --- [           main] com.es.all.api.DocSearchApi              : score >>>> 1.2144442
//        2023-03-01 10:54:02.723  INFO 17484 --- [           main] com.es.all.api.DocSearchApi              : sourceAsString >>>> {"path":"/api/post/1/update1","times":"60","method":"POST","created":"2023-02-28"}
//        2023-03-01 10:54:02.724  INFO 17484 --- [           main] com.es.all.api.DocSearchApi              : path >>>> /api/post/1/update1
//        2023-03-01 10:54:02.724  INFO 17484 --- [           main] com.es.all.api.DocSearchApi              : index >>>> req_log
//        2023-03-01 10:54:02.724  INFO 17484 --- [           main] com.es.all.api.DocSearchApi              : id >>>> GaXWloYBBkiEpNgm4EUT
//        2023-03-01 10:54:02.724  INFO 17484 --- [           main] com.es.all.api.DocSearchApi              : score >>>> 1.2144442
//        2023-03-01 10:54:02.724  INFO 17484 --- [           main] com.es.all.api.DocSearchApi              : sourceAsString >>>> {"method":"POST","times":"60","path":"/api/post/5/update","created":"2023-02-28"}
//        2023-03-01 10:54:02.725  INFO 17484 --- [           main] com.es.all.api.DocSearchApi              : path >>>> /api/post/5/update
//        2023-03-01 10:54:02.725  INFO 17484 --- [           main] com.es.all.api.DocSearchApi              : index >>>> req_log
//        2023-03-01 10:54:02.725  INFO 17484 --- [           main] com.es.all.api.DocSearchApi              : id >>>> GqXWloYBBkiEpNgm4EUT
//        2023-03-01 10:54:02.725  INFO 17484 --- [           main] com.es.all.api.DocSearchApi              : score >>>> 1.2144442
//        2023-03-01 10:54:02.725  INFO 17484 --- [           main] com.es.all.api.DocSearchApi              : sourceAsString >>>> {"method":"POST","times":"60","path":"/api/post/6/update","created":"2023-02-28"}
//        2023-03-01 10:54:02.725  INFO 17484 --- [           main] com.es.all.api.DocSearchApi              : path >>>> /api/post/6/update
    }

}

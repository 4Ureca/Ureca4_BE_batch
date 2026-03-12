package com.uplus.batch.controller;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import com.uplus.batch.domain.summary.entity.ConsultationSummary;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequestMapping("/batch")
@RequiredArgsConstructor
public class ReindexController {

  private static final String ANALYZER_INDEX = "consult-analyzer-config";
  private static final String SEARCH_INDEX   = "consult-search-index";
  private static final String KEYWORD_INDEX  = "consult-keyword-index";

  // prod 사전 경로 치환 매핑 (local → prod)
  private static final Map<String, String> PROD_PATH_MAP = Map.of(
      "analysis/userdict.txt",            "analyzers/F187334599",
      "analysis/analysis_userdict.txt",   "analyzers/F125541068",
      "analysis/stopwords.txt",           "analyzers/F136386273",
      "analysis/analysis_stopwords.txt",  "analyzers/F99835004",
      "analysis/synonyms.txt",            "analyzers/F134457758",
      "analysis/analysis_synonyms.txt",   "analyzers/F72664227"
  );

  private final MongoTemplate mongoTemplate;
  private final JobLauncher jobLauncher;
  private final ElasticsearchClient elasticsearchClient;

  @Qualifier("esReindexJob")
  private final Job esReindexJob;

  // -----------------------------------------------------------------------
  // local/dev
  // -----------------------------------------------------------------------

  @PostMapping("/reindex")
  public ResponseEntity<String> reindex() {
    try {
      recreateIndex(ANALYZER_INDEX, false);
      recreateIndex(SEARCH_INDEX, false);
      recreateIndex(KEYWORD_INDEX, false);
      resetFlags();
      triggerJob();
      return ResponseEntity.ok("reindex triggered");
    } catch (Exception e) {
      log.error("reindex 실패", e);
      return ResponseEntity.internalServerError().body("reindex failed: " + e.getMessage());
    }
  }

  // -----------------------------------------------------------------------
  // prod - 사전 경로 치환 후 실행
  // -----------------------------------------------------------------------

  @PostMapping("/reindex/prod")
  public ResponseEntity<String> reindexProd() {
    try {
      recreateIndex(ANALYZER_INDEX, true);
      recreateIndex(SEARCH_INDEX, true);
      recreateIndex(KEYWORD_INDEX, true);
      resetFlags();
      triggerJob();
      return ResponseEntity.ok("reindex prod triggered");
    } catch (Exception e) {
      log.error("reindex prod 실패", e);
      return ResponseEntity.internalServerError().body("reindex prod failed: " + e.getMessage());
    }
  }

  // -----------------------------------------------------------------------
  // private
  // -----------------------------------------------------------------------

  private void recreateIndex(String indexName, boolean prod) throws Exception {
    // DELETE
    boolean exists = elasticsearchClient.indices().exists(r -> r.index(indexName)).value();
    if (exists) {
      elasticsearchClient.indices().delete(r -> r.index(indexName));
      log.info("reindex: {} 삭제 완료", indexName);
    }

    // JSON 로드
    String json = new String(
        new ClassPathResource("es-index/" + indexName + ".json")
            .getInputStream().readAllBytes(),
        StandardCharsets.UTF_8
    );

    // prod면 사전 경로 치환
    if (prod) {
      for (Map.Entry<String, String> entry : PROD_PATH_MAP.entrySet()) {
        json = json.replace(entry.getKey(), entry.getValue());
      }
    }

    // PUT
    InputStream is = new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8));
    elasticsearchClient.indices().create(
        CreateIndexRequest.of(r -> r.index(indexName).withJson(is))
    );
    log.info("reindex: {} 생성 완료 (prod={})", indexName, prod);
  }

  private void resetFlags() {
    mongoTemplate.updateMulti(
        new Query(),
        new Update().set("searchIndexed", false),
        ConsultationSummary.class
    );
    log.info("reindex: searchIndexed=false 전체 초기화 완료");

    LocalDateTime from = LocalDateTime.now().minusDays(31);
    mongoTemplate.updateMulti(
        Query.query(Criteria.where("consultedAt").gte(from)),
        new Update().set("keywordIndexed", false),
        ConsultationSummary.class
    );
    log.info("reindex: keywordIndexed=false 31일 이내 초기화 완료");
  }

  private void triggerJob() throws Exception {
    JobParameters params = new JobParametersBuilder()
        .addLong("triggeredAt", System.currentTimeMillis())
        .toJobParameters();
    jobLauncher.run(esReindexJob, params);
    log.info("reindex: esReindexJob 트리거 완료");
  }
}
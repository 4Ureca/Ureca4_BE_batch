package com.uplus.batch.common.elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/**
 * consult-keyword-index에서 응대 품질 토큰을 집계하는 서비스.
 * agent.quality 필드(quality_analyzer 적용)에 대해 terms + filters aggregation을 수행하여
 * 상담사별 품질 메트릭을 한 번의 쿼리로 산출한다.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class QualityAnalysisAggregator {

  private final ElasticsearchClient elasticsearchClient;

  private static final String INDEX = "consult-keyword-index";
  private static final String QUALITY_FIELD = "agent.quality";
  private static final DateTimeFormatter ES_DATE = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");

  // 품질 토큰 (analysis_synonyms.txt 매핑 결과)
  private static final String TOKEN_EMPATHY = "공감응대";
  private static final String TOKEN_APOLOGY = "사과표현";
  private static final String TOKEN_COURTESY = "친절응대";
  private static final String TOKEN_PROMPTNESS = "신속응대";
  private static final String TOKEN_ACCURACY = "정확응대";
  private static final String TOKEN_WAITING = "대기안내";
  private static final String TOKEN_THANKS = "감사인사";
  private static final String TOKEN_CLOSING = "마무리인사";

  private static final List<String> ALL_QUALITY_TOKENS = List.of(
      TOKEN_EMPATHY, TOKEN_APOLOGY, TOKEN_COURTESY, TOKEN_PROMPTNESS,
      TOKEN_ACCURACY, TOKEN_WAITING, TOKEN_THANKS, TOKEN_CLOSING
  );

  /**
   * 특정 상담사의 특정 날짜 품질 집계 결과를 반환한다.
   *
   * @return 집계 결과, 데이터가 없으면 null
   */
  public QualityAggResult aggregate(Long agentId, LocalDate date) throws IOException {
    String startStr = date.atStartOfDay().format(ES_DATE);
    String endStr = date.atTime(23, 59, 59).format(ES_DATE);

    SearchRequest request = SearchRequest.of(s -> s
        .index(INDEX)
        .size(0)
        .query(q -> q
            .bool(b -> b
                .filter(f -> f.term(t -> t.field("agent_id").value(String.valueOf(agentId))))
                .filter(f -> f.range(r -> r
                    .date(d -> d
                        .field("date")
                        .gte(startStr)
                        .lte(endStr)
                    )
                ))
            )
        )
        .aggregations("quality_tokens", a -> a
            .terms(t -> t
                .field(QUALITY_FIELD)
                .include(i -> i.terms(ALL_QUALITY_TOKENS))
                .size(10)
            )
        )
        .aggregations("doc_filters", a -> a
            .filters(f -> f
                .filters(fb -> fb.keyed(Map.of(
                    "empathy", Query.of(q -> q.term(t -> t.field(QUALITY_FIELD).value(TOKEN_EMPATHY))),
                    "apology", Query.of(q -> q.term(t -> t.field(QUALITY_FIELD).value(TOKEN_APOLOGY))),
                    "courtesy", Query.of(q -> q.term(t -> t.field(QUALITY_FIELD).value(TOKEN_COURTESY))),
                    "promptness", Query.of(q -> q.term(t -> t.field(QUALITY_FIELD).value(TOKEN_PROMPTNESS))),
                    "accuracy", Query.of(q -> q.term(t -> t.field(QUALITY_FIELD).value(TOKEN_ACCURACY))),
                    "waiting", Query.of(q -> q.term(t -> t.field(QUALITY_FIELD).value(TOKEN_WAITING))),
                    "closing", Query.of(q -> q.bool(bl -> bl
                        .should(sh -> sh.term(t -> t.field(QUALITY_FIELD).value(TOKEN_THANKS)))
                        .should(sh -> sh.term(t -> t.field(QUALITY_FIELD).value(TOKEN_CLOSING)))
                    ))
                )))
            )
        )
    );

    SearchResponse<Void> response = elasticsearchClient.search(request, Void.class);

    long totalDocs = response.hits().total() != null ? response.hits().total().value() : 0;
    if (totalDocs == 0) {
      return null;
    }

    // terms aggregation에서 공감응대 토큰의 doc_count 추출
    long empathyDocCount = 0;
    var termsAgg = response.aggregations().get("quality_tokens").sterms();
    for (var bucket : termsAgg.buckets().array()) {
      if (TOKEN_EMPATHY.equals(bucket.key().stringValue())) {
        empathyDocCount = bucket.docCount();
        break;
      }
    }

    // filters aggregation에서 각 토큰별 문서 수 추출
    var filtersAgg = response.aggregations().get("doc_filters").filters();
    var buckets = filtersAgg.buckets().keyed();

    return new QualityAggResult(
        totalDocs,
        empathyDocCount,
        buckets.get("apology").docCount(),
        buckets.get("courtesy").docCount(),
        buckets.get("promptness").docCount(),
        buckets.get("accuracy").docCount(),
        buckets.get("waiting").docCount(),
        buckets.get("closing").docCount()
    );
  }

  public record QualityAggResult(
      long totalDocs,
      long empathyDocCount,
      long apologyDocCount,
      long courtesyDocCount,
      long promptnessDocCount,
      long accuracyDocCount,
      long waitingDocCount,
      long closingDocCount
  ) {}
}

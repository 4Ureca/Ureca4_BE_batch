package com.uplus.batch.common.elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.indices.AnalyzeRequest;
import co.elastic.clients.elasticsearch.indices.AnalyzeResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class ElasticsearchAnalyzeService {

  private final ElasticsearchClient elasticsearchClient;

  public List<String> analyze(String text) throws IOException {

    AnalyzeResponse response = elasticsearchClient.indices().analyze(
        AnalyzeRequest.of(a -> a
            .index("consult-index")
            .analyzer("korean_index_analyzer")
            .text(text)
        )
    );

    return response.tokens()
        .stream()
        .map(t -> t.token())
        .filter(token -> token.length() > 1)
        .collect(Collectors.toList());
  }

  /**
   * 응대 품질 분석용 ES _analyze API 호출.
   * korean_analysis_index_analyzer를 사용하여 analysis_synonyms.txt 동의어 사전이 적용된 토큰을 반환한다.
   * 예: "충분히 이해합니다" → ["공감응대"] 토큰 반환
   */
  public List<String> analyzeForQuality(String text) throws IOException {

    AnalyzeResponse response = elasticsearchClient.indices().analyze(
        AnalyzeRequest.of(a -> a
            .index("consult-index")
            .analyzer("korean_analysis_index_analyzer")
            .text(text)
        )
    );

    return response.tokens()
        .stream()
        .map(t -> t.token())
        .filter(token -> token.length() > 1)
        .collect(Collectors.toList());
  }
}
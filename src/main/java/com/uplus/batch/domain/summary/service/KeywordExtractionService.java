package com.uplus.batch.domain.summary.service;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.indices.AnalyzeRequest;
import com.uplus.batch.domain.summary.values.ExtractKeywords;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class KeywordExtractionService {

  private final ElasticsearchClient elasticsearchClient;

  public List<String> extractKeywords(String text) throws Exception {

    var response =
        elasticsearchClient.indices().analyze(
            AnalyzeRequest.of(a -> a
                .index("consult-analyzer-config")
                .analyzer("consult_analyzer")
                .text(text)
            )
        );

    List<String> tokens =
        response.tokens()
            .stream()
            .map(t -> t.token())
            .distinct()
            .collect(Collectors.toList());

    return ExtractKeywords.filter(tokens);
  }
}
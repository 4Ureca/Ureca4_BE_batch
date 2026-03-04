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
            .analyzer("nori")
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
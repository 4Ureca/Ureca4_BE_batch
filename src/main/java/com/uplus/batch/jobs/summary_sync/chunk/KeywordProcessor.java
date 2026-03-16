package com.uplus.batch.jobs.summary_sync.chunk;

import com.uplus.batch.domain.summary.service.KeywordExtractionService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.*;

@Slf4j
@Component
@RequiredArgsConstructor
public class KeywordProcessor {

  private final KeywordExtractionService keywordService;

  public KeywordResult process(String mergedText, String iamText, String rawSummary)
      throws Exception {

    List<String> rawKeywords;
    List<String> iamKeywords;
    try {
      rawKeywords = keywordService.extractKeywords(mergedText);
      iamKeywords = keywordService.extractKeywords(iamText);
    } catch (Exception e) {
      log.warn("[KeywordProcessor] ES 키워드 추출 실패 (fallback: 빈 리스트 사용) - {}", e.getMessage());
      rawKeywords = List.of();
      iamKeywords = List.of();
    }

    Set<String> iamSet = new HashSet<>(iamKeywords);
    Set<String> rawSet = new HashSet<>(rawKeywords);

    Set<String> intersection = new HashSet<>(iamSet);
    intersection.retainAll(rawSet);

    Set<String> union = new HashSet<>(iamSet);
    union.addAll(rawSet);

    List<String> matchKeywords = new ArrayList<>(intersection);

    double matchRate = union.isEmpty()
        ? 0.0
        : (double) intersection.size() / union.size();

    List<String> summaryKeywords = null;
    if (rawSummary != null) {
      try {
        summaryKeywords = keywordService.extractKeywords(rawSummary);
      } catch (Exception e) {
        log.warn("[KeywordProcessor] ES summary 키워드 추출 실패 (fallback: null) - {}", e.getMessage());
      }
    }

    return new KeywordResult(matchKeywords, summaryKeywords, matchRate);
  }

  public record KeywordResult(
      List<String> matchKeywords,
      List<String> summaryKeywords,
      double matchRate
  ) {}
}

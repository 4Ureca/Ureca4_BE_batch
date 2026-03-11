package com.uplus.batch.jobs.summary_sync.chunk;

import com.uplus.batch.domain.summary.service.KeywordExtractionService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.*;

@Component
@RequiredArgsConstructor
public class KeywordProcessor {

  private final KeywordExtractionService keywordService;

  public KeywordResult process(String mergedText, String iamText, String rawSummary)
      throws Exception {

    List<String> rawKeywords = keywordService.extractKeywords(mergedText);
    List<String> iamKeywords = keywordService.extractKeywords(iamText);

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
      summaryKeywords = keywordService.extractKeywords(rawSummary);
    }

    return new KeywordResult(matchKeywords, summaryKeywords, matchRate);
  }

  public record KeywordResult(
      List<String> matchKeywords,
      List<String> summaryKeywords,
      double matchRate
  ) {}
}
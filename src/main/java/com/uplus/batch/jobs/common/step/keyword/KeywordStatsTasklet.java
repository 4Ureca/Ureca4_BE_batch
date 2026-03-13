package com.uplus.batch.jobs.common.step.keyword;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

@Slf4j
@RequiredArgsConstructor
public class KeywordStatsTasklet implements Tasklet {

    private static final int TOP_KEYWORD_SIZE = 20;
    private static final int GRADE_KEYWORD_SIZE = 5;
    private static final int LONG_TERM_THRESHOLD_DAYS = 4;
    private static final int LONG_TERM_LOOKUP_DAYS = 28;
    private static final int DAILY_TOP_N = 10;

    private final MongoTemplate mongoTemplate;
    private final String targetCollectionName;

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {

        // Job 파라미터에서 날짜 범위 추출 (없으면 기본값: 주별=지난주, 월별=지난달)
        var jobParams = chunkContext.getStepContext().getJobParameters();
        String startDateStr = (String) jobParams.get("startDate");
        String endDateStr = (String) jobParams.get("endDate");

        LocalDate startDate;
        LocalDate endDate;

        if (startDateStr != null && endDateStr != null) {
            startDate = LocalDate.parse(startDateStr);
            endDate = LocalDate.parse(endDateStr);
        } else {
            // 기본값: 테스트용 하드코딩 (2025-01-08 ~ 2025-01-15)
            startDate = LocalDate.of(2025, 1, 8);
            endDate = LocalDate.of(2025, 1, 15);
        }

        log.info("[KeywordStats] {} ~ {} 집계 시작 → {}", startDate, endDate, targetCollectionName);

        // 1. 현재 기간 일별 스냅샷 조회
        List<Document> currentSnapshots = queryDailySnapshots(startDate, endDate);

        if (currentSnapshots.isEmpty()) {
            log.warn("기간 내 daily_report_snapshot이 없습니다. 키워드 집계를 건너뜁니다.");
            return RepeatStatus.FINISHED;
        }

        // 2. 이전 동일 기간 일별 스냅샷 조회 (증감율 계산용)
        long periodDays = java.time.temporal.ChronoUnit.DAYS.between(startDate, endDate) + 1;
        LocalDate prevStartDate = startDate.minusDays(periodDays);
        LocalDate prevEndDate = startDate.minusDays(1);
        List<Document> prevSnapshots = queryDailySnapshots(prevStartDate, prevEndDate);

        // 3. topKeywords 합산 + 증감율 계산
        Map<String, Long> currentKeywordCounts = aggregateKeywordCounts(currentSnapshots);
        Map<String, Long> prevKeywordCounts = aggregateKeywordCounts(prevSnapshots);

        List<KeywordStatsResult.TopKeyword> topKeywords = buildTopKeywords(currentKeywordCounts, prevKeywordCounts);

        // 4. 장기 상위 유지 키워드 계산
        LocalDate lookupStart = endDate.minusDays(LONG_TERM_LOOKUP_DAYS - 1);
        List<Document> longTermSnapshots = queryDailySnapshots(lookupStart, endDate);
        List<KeywordStatsResult.LongTermKeyword> longTermKeywords = buildLongTermKeywords(longTermSnapshots, currentKeywordCounts);

        // 5. 고객 유형별 키워드 합산
        List<KeywordStatsResult.CustomerTypeKeyword> byCustomerType = buildByCustomerType(currentSnapshots);

        // 6. 결과 로깅
        logResults(topKeywords, longTermKeywords, byCustomerType);

        // 7. 스냅샷에 keywordSummary upsert
        saveKeywordSummary(startDate, endDate, topKeywords, longTermKeywords, byCustomerType);

        log.info("[KeywordStats] {} ~ {} 집계 완료 → {}", startDate, endDate, targetCollectionName);
        return RepeatStatus.FINISHED;
    }

    /**
     * daily_report_snapshot에서 기간 내 문서 조회
     * KeywordRankTasklet은 'startAt' 필드 기준으로 upsert하므로 startAt 기준으로 조회
     */
    private List<Document> queryDailySnapshots(LocalDate start, LocalDate end) {
        Query query = new Query(Criteria.where("startAt").gte(start).lte(end));
        return mongoTemplate.find(query, Document.class, "daily_report_snapshot");
    }

    /**
     * 일별 topKeywords 카운트 합산
     */
    private Map<String, Long> aggregateKeywordCounts(List<Document> snapshots) {
        Map<String, Long> totalCounts = new HashMap<>();

        for (Document snapshot : snapshots) {
            Document keywordSummary = (Document) snapshot.get("keywordSummary");
            if (keywordSummary == null) continue;
            List<Document> topKeywords = keywordSummary.getList("topKeywords", Document.class);
            if (topKeywords == null) continue;

            for (Document kw : topKeywords) {
                String keyword = kw.getString("keyword");
                Number count = (Number) kw.get("count");
                if (keyword != null && count != null) {
                    totalCounts.merge(keyword, count.longValue(), Long::sum);
                }
            }
        }

        return totalCounts;
    }

    /**
     * TOP N 키워드 + 증감율 계산
     */
    private List<KeywordStatsResult.TopKeyword> buildTopKeywords(
            Map<String, Long> currentCounts, Map<String, Long> prevCounts) {

        List<Map.Entry<String, Long>> sorted = currentCounts.entrySet().stream()
                .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
                .limit(TOP_KEYWORD_SIZE)
                .toList();

        List<KeywordStatsResult.TopKeyword> result = new ArrayList<>();
        int rank = 1;

        for (Map.Entry<String, Long> entry : sorted) {
            String keyword = entry.getKey();
            long currentCount = entry.getValue();
            long prevCount = prevCounts.getOrDefault(keyword, 0L);

            double changeRate = 0;
            if (prevCount > 0) {
                changeRate = Math.round(((currentCount - prevCount) * 100.0 / prevCount) * 10.0) / 10.0;
            }

            result.add(KeywordStatsResult.TopKeyword.builder()
                    .keyword(keyword)
                    .count(currentCount)
                    .rank(rank++)
                    .changeRate(changeRate)
                    .build());
        }

        return result;
    }

    /**
     * 장기 상위 유지 키워드 판정
     * 최근 28일 중 일별 TOP10에 14일 이상 등장한 키워드
     */
    private List<KeywordStatsResult.LongTermKeyword> buildLongTermKeywords(
            List<Document> snapshots, Map<String, Long> currentCounts) {

        Map<String, Integer> appearanceDays = new HashMap<>();
        int totalDays = snapshots.size();

        for (Document snapshot : snapshots) {
            Document keywordSummary = (Document) snapshot.get("keywordSummary");
            if (keywordSummary == null) continue;
            List<Document> topKeywords = keywordSummary.getList("topKeywords", Document.class);
            if (topKeywords == null) continue;

            // 일별 TOP 10 추출
            topKeywords.stream()
                    .sorted((a, b) -> {
                        Number countA = (Number) a.get("count");
                        Number countB = (Number) b.get("count");
                        return Long.compare(
                                countB != null ? countB.longValue() : 0,
                                countA != null ? countA.longValue() : 0);
                    })
                    .limit(DAILY_TOP_N)
                    .forEach(kw -> {
                        String keyword = kw.getString("keyword");
                        if (keyword != null) {
                            appearanceDays.merge(keyword, 1, Integer::sum);
                        }
                    });
        }

        // 14일 이상 등장한 키워드 = 장기 상위
        List<KeywordStatsResult.LongTermKeyword> result = new ArrayList<>();
        int rank = 1;

        List<Map.Entry<String, Integer>> longTermEntries = appearanceDays.entrySet().stream()
                .filter(e -> e.getValue() >= LONG_TERM_THRESHOLD_DAYS)
                .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
                .toList();

        for (Map.Entry<String, Integer> entry : longTermEntries) {
            String keyword = entry.getKey();
            long count = currentCounts.getOrDefault(keyword, 0L);

            result.add(KeywordStatsResult.LongTermKeyword.builder()
                    .keyword(keyword)
                    .count(count)
                    .rank(rank++)
                    .appearanceDays(entry.getValue())
                    .totalDays(totalDays)
                    .build());
        }

        return result;
    }

    /**
     * 고객 유형별(등급별) 키워드 합산
     * daily_report_snapshot의 keywordSummary.byCustomerType 필드를 읽어서 합산
     */
    private List<KeywordStatsResult.CustomerTypeKeyword> buildByCustomerType(List<Document> snapshots) {
        // customerType → keyword → count
        Map<String, Map<String, Long>> customerTypeKeywordCounts = new HashMap<>();

        for (Document snapshot : snapshots) {
            Document keywordSummary = (Document) snapshot.get("keywordSummary");
            if (keywordSummary == null) continue;
            List<Document> byCustomerType = keywordSummary.getList("byCustomerType", Document.class);
            if (byCustomerType == null) continue;

            for (Document ct : byCustomerType) {
                String customerType = ct.getString("customerType");
                if (customerType == null) continue;

                List<Document> keywords = ct.getList("keywords", Document.class);
                if (keywords == null) continue;

                Map<String, Long> keywordMap = customerTypeKeywordCounts
                        .computeIfAbsent(customerType, k -> new HashMap<>());

                for (Document kw : keywords) {
                    String keyword = kw.getString("keyword");
                    Number count = (Number) kw.get("count");
                    if (keyword != null && count != null) {
                        keywordMap.merge(keyword, count.longValue(), Long::sum);
                    }
                }
            }
        }

        // 유형별 TOP 5 키워드+건수 추출
        return customerTypeKeywordCounts.entrySet().stream()
                .map(entry -> {
                    List<KeywordStatsResult.CustomerKeywordCount> topKeywords = entry.getValue().entrySet().stream()
                            .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
                            .limit(GRADE_KEYWORD_SIZE)
                            .map(e -> KeywordStatsResult.CustomerKeywordCount.builder()
                                    .keyword(e.getKey())
                                    .count(e.getValue())
                                    .build())
                            .toList();

                    return KeywordStatsResult.CustomerTypeKeyword.builder()
                            .customerType(entry.getKey())
                            .keywords(topKeywords)
                            .build();
                })
                .toList();
    }

    private void logResults(List<KeywordStatsResult.TopKeyword> topKeywords,
                            List<KeywordStatsResult.LongTermKeyword> longTermKeywords,
                            List<KeywordStatsResult.CustomerTypeKeyword> byCustomerType) {
        // 상세 로그 생략
    }

    /**
     * weekly/monthly_report_snapshot에 keywordSummary upsert
     */
    private void saveKeywordSummary(LocalDate startDate, LocalDate endDate,
                                    List<KeywordStatsResult.TopKeyword> topKeywords,
                                    List<KeywordStatsResult.LongTermKeyword> longTermKeywords,
                                    List<KeywordStatsResult.CustomerTypeKeyword> byCustomerType) {

        LocalDateTime startAt = startDate.atStartOfDay();
        LocalDateTime endAt = endDate.atTime(23, 59, 59);

        // topKeywords를 Document 리스트로 변환
        List<Document> topKeywordDocs = topKeywords.stream()
                .map(k -> new Document()
                        .append("keyword", k.getKeyword())
                        .append("count", k.getCount())
                        .append("rank", k.getRank())
                        .append("changeRate", k.getChangeRate()))
                .toList();

        // longTermTopKeywords를 Document 리스트로 변환
        List<Document> longTermDocs = longTermKeywords.stream()
                .map(k -> new Document()
                        .append("keyword", k.getKeyword())
                        .append("count", k.getCount())
                        .append("rank", k.getRank())
                        .append("appearanceDays", k.getAppearanceDays())
                        .append("totalDays", k.getTotalDays()))
                .toList();

        // byCustomerType를 Document 리스트로 변환
        List<Document> customerTypeDocs = byCustomerType.stream()
                .map(ct -> {
                    List<Document> kwDocs = ct.getKeywords().stream()
                            .map(kw -> new Document("keyword", kw.getKeyword())
                                    .append("count", kw.getCount()))
                            .toList();
                    return new Document()
                            .append("customerType", ct.getCustomerType())
                            .append("keywords", kwDocs);
                })
                .toList();

        Document keywordSummary = new Document()
                .append("topKeywords", topKeywordDocs)
                .append("longTermTopKeywords", longTermDocs)
                .append("byCustomerType", customerTypeDocs);

        // startAt 기준으로 upsert
        Query query = new Query(Criteria.where("startAt").is(startAt));
        Update update = new Update()
                .set("startAt", startAt)
                .set("endAt", endAt)
                .set("keywordSummary", keywordSummary)
                .currentDate("createdAt");

        mongoTemplate.upsert(query, update, targetCollectionName);

    }
}

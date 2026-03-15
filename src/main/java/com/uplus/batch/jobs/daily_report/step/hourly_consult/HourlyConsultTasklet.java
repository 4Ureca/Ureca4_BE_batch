package com.uplus.batch.jobs.daily_report.step.hourly_consult;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.*;
import java.util.stream.Collectors;

/**
 * 시간대별 이슈 트렌드 집계 Tasklet
 *
 * 3시간 슬롯(09-12, 12-15, 15-18)별로:
 * - 대분류 카테고리별 빈도 + 비율
 * - 상담 처리 건수 + 평균 처리시간
 * - 키워드 빈도 순위 + 전일 대비 증감율 + 신규 진입
 * - 전체 키워드 분석 + 고객 유형별 키워드
 */
@Slf4j
@Component
@StepScope
public class HourlyConsultTasklet implements Tasklet {

    private final MongoTemplate mongoTemplate;
    private final ElasticsearchClient elasticsearchClient;
    private final String targetDateParam;
    private final String targetSlotParam;

    public static final String RESULT_KEY = "hourlyConsultResult";

    private static final String COLLECTION = "consultation_summary";
    private static final String SNAPSHOT_COLLECTION = "daily_report_snapshot";
    private static final int TOP_KEYWORD_SIZE = 10;

    /** 대분류 코드 매핑 */
    private static final Map<String, String> LARGE_CATEGORY_CODE = Map.of(
            "요금/납부", "FEE",
            "기기변경", "DEV",
            "장애/A/S", "TRB",
            "해지/재약정", "CHN",
            "부가서비스", "ADD",
            "기타", "ETC"
    );

    /** 슬롯 정의: slot label → {startHour, endHour} */
    private static final LinkedHashMap<String, int[]> SLOT_DEFS = new LinkedHashMap<>();

    static {
        SLOT_DEFS.put("09-12", new int[]{9, 12});
        SLOT_DEFS.put("12-15", new int[]{12, 15});
        SLOT_DEFS.put("15-18", new int[]{15, 18});
    }

    public HourlyConsultTasklet(
            MongoTemplate mongoTemplate,
            ElasticsearchClient elasticsearchClient,
            @Value("#{jobParameters['targetDate'] ?: null}") String targetDateParam,
            @Value("#{jobParameters['slot'] ?: null}") String targetSlotParam) {
        this.mongoTemplate = mongoTemplate;
        this.elasticsearchClient = elasticsearchClient;
        this.targetDateParam = targetDateParam;
        this.targetSlotParam = targetSlotParam;
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext)
            throws Exception {

        LocalDate targetDate = resolveTargetDate();
        String targetSlot = resolveTargetSlot();

        int[] hours = SLOT_DEFS.get(targetSlot);
        if (hours == null) {
            log.warn("[HourlyConsult] 유효하지 않은 슬롯: {}. 스킵합니다.", targetSlot);
            return RepeatStatus.FINISHED;
        }

        LocalDateTime slotStart = targetDate.atTime(hours[0], 0);
        LocalDateTime slotEnd = targetDate.atTime(hours[1], 0);
        LocalDateTime dayStart = targetDate.atStartOfDay();
        LocalDateTime dayEnd = targetDate.atTime(23, 59, 59);

        log.info("[HourlyConsult] {} 슬롯 {} 집계 시작", targetDate, targetSlot);

        // 1) 카테고리별 빈도 집계 (MongoDB)
        List<Document> categoryDocs = aggregateCategory(slotStart, slotEnd);
        int slotTotalCount = categoryDocs.stream()
                .mapToInt(d -> d.getInteger("count", 0))
                .sum();
        double slotTotalDurationSec = categoryDocs.stream()
                .mapToDouble(d -> {
                    Double avg = d.get("avgDurationSec") instanceof Number
                            ? ((Number) d.get("avgDurationSec")).doubleValue() : 0.0;
                    return avg * d.getInteger("count", 0);
                })
                .sum();
        double slotAvgDurationMin = slotTotalCount > 0
                ? (slotTotalDurationSec / slotTotalCount) / 60.0
                : 0;

        List<HourlyConsultResult.CategoryBreakdown> categoryBreakdown = buildCategoryBreakdown(
                categoryDocs, slotTotalCount);

        // 2) [변경] 키워드 집계: ES consult-keyword-index (인바운드만)
        //    MongoDB에서 인바운드 consultId 목록을 뽑아 ES ids 필터에 사용
        List<String> inboundIds = mongoTemplate.find(
                new Query(Criteria.where("consultedAt").gte(slotStart).lt(slotEnd)
                        .and("category.code").not().regex("^M_OTB")),
                Document.class, COLLECTION
        ).stream()
                .map(d -> String.valueOf(d.get("consultId")))
                .collect(Collectors.toList());

        Map<String, Long> currentKeywords = inboundIds.isEmpty()
                ? Collections.emptyMap()
                : aggregateKeywordsFromRawText(slotStart, slotEnd, inboundIds);

        // 3) 전일 같은 슬롯 키워드 조회 → 증감율/신규 계산
        Map<String, Long> previousKeywords = loadPreviousSlotKeywords(
                targetDate.minusDays(1), targetSlot);
        HourlyConsultResult.KeywordAnalysis keywordAnalysis = buildKeywordAnalysis(
                currentKeywords, previousKeywords);

        // 5) 슬롯 결과 조립
        HourlyConsultResult.TimeSlotResult slotResult = HourlyConsultResult.TimeSlotResult.builder()
                .slot(targetSlot)
                .consultCount(slotTotalCount)
                .avgDuration(Math.round(slotAvgDurationMin * 10.0) / 10.0)
                .categoryBreakdown(categoryBreakdown)
                .keywordAnalysis(keywordAnalysis)
                .build();

        // 6) 기존 snapshot에서 다른 슬롯 데이터 합산 → 전체 count/avg 계산
        List<HourlyConsultResult.TimeSlotResult> allSlots = mergeWithExistingSlots(
                dayStart, slotResult);
        int totalCount = allSlots.stream().mapToInt(HourlyConsultResult.TimeSlotResult::getConsultCount).sum();
        double totalAvgMin = allSlots.stream()
                .mapToDouble(s -> s.getAvgDuration() * s.getConsultCount())
                .sum();
        totalAvgMin = totalCount > 0 ? Math.round((totalAvgMin / totalCount) * 10.0) / 10.0 : 0;

        // 7) 최종 결과 빌드
        HourlyConsultResult result = HourlyConsultResult.builder()
                .totalConsultCount(totalCount)
                .avgDurationMinutes(totalAvgMin)
                .timeSlotTrend(allSlots)
                .build();

        contribution.getStepExecution()
                .getJobExecution()
                .getExecutionContext()
                .put(RESULT_KEY, result);

        // 8) daily_report_snapshot upsert
        upsertSnapshot(dayStart, dayEnd, result);

        log.info("[HourlyConsult] {} 슬롯 {} 집계 완료", targetDate, targetSlot);
        return RepeatStatus.FINISHED;
    }

    // ═══════════════════════════════════════════════════════
    //  날짜/슬롯 결정
    // ═══════════════════════════════════════════════════════

    private LocalDate resolveTargetDate() {
        if (targetDateParam != null && !targetDateParam.isEmpty()) {
            return LocalDate.parse(targetDateParam);
        }
        return LocalDate.now();
    }

    private String resolveTargetSlot() {
        if (targetSlotParam != null && !targetSlotParam.isEmpty()) {
            return targetSlotParam;
        }
        int hour = LocalTime.now().getHour();
        if (hour >= 18) return "15-18";
        if (hour >= 15) return "12-15";
        return "09-12";
    }

    // ═══════════════════════════════════════════════════════
    //  MongoDB: 카테고리 집계
    // ═══════════════════════════════════════════════════════

    private List<Document> aggregateCategory(LocalDateTime slotStart, LocalDateTime slotEnd) {
        Aggregation aggregation = Aggregation.newAggregation(
                Aggregation.match(
                        Criteria.where("consultedAt").gte(slotStart).lt(slotEnd)
                                .and("category.code").not().regex("^M_OTB")
                ),
                Aggregation.group("category.large")
                        .count().as("count")
                        .avg("durationSec").as("avgDurationSec"),
                Aggregation.sort(Sort.Direction.DESC, "count")
        );

        AggregationResults<Document> results = mongoTemplate.aggregate(
                aggregation, COLLECTION, Document.class);
        return results.getMappedResults();
    }

    private List<HourlyConsultResult.CategoryBreakdown> buildCategoryBreakdown(
            List<Document> docs, int totalCount) {

        return docs.stream().map(doc -> {
            String largeName = doc.getString("_id");
            int count = doc.getInteger("count", 0);
            double avgSec = doc.get("avgDurationSec") instanceof Number
                    ? ((Number) doc.get("avgDurationSec")).doubleValue() : 0.0;
            double rate = totalCount > 0
                    ? Math.round((count * 100.0 / totalCount) * 10.0) / 10.0
                    : 0;
            String code = LARGE_CATEGORY_CODE.getOrDefault(largeName, "ETC");

            return HourlyConsultResult.CategoryBreakdown.builder()
                    .code(code)
                    .name(largeName)
                    .count(count)
                    .rate(rate)
                    .build();
        }).collect(Collectors.toList());
    }

    // ═══════════════════════════════════════════════════════
    //  [변경] 키워드 집계: ES consult-keyword-index 직접 집계
    //  기존: MySQL 원문 → ES _analyze API × N건
    //  변경: ES terms aggregation 1회 (KeywordRankTasklet과 동일 패턴)
    // ═══════════════════════════════════════════════════════

    private Map<String, Long> aggregateKeywordsFromRawText(
            LocalDateTime start, LocalDateTime end, List<String> inboundIds) throws Exception {

        SearchResponse<Void> response = elasticsearchClient.search(s -> s
                .index("consult-keyword-index")
                .size(0)
                .query(q -> q
                    .ids(ids -> ids.values(inboundIds))
                )
                .aggregations("slot_keywords", a -> a
                    .terms(t -> t.field("customer.search").size(TOP_KEYWORD_SIZE * 2))
                ),
            Void.class
        );

        Map<String, Long> keywordCount = new LinkedHashMap<>();

        response.aggregations().get("slot_keywords").sterms()
            .buckets().array().stream()
            .filter(b -> !"null".equals(b.key().stringValue())
                         && b.key().stringValue().length() > 1)
            .forEach(b -> keywordCount.put(b.key().stringValue(), b.docCount()));

        return keywordCount;
    }

    // ═══════════════════════════════════════════════════════
    //  키워드 증감율 / 신규 진입 계산
    // ═══════════════════════════════════════════════════════

    private HourlyConsultResult.KeywordAnalysis buildKeywordAnalysis(
            Map<String, Long> current, Map<String, Long> previous) {

        List<HourlyConsultResult.TopKeyword> topKeywords = new ArrayList<>();
        List<HourlyConsultResult.NewKeyword> newKeywords = new ArrayList<>();

        int rank = 1;
        for (Map.Entry<String, Long> entry : current.entrySet()) {
            if (rank > TOP_KEYWORD_SIZE) break;

            String keyword = entry.getKey();
            long count = entry.getValue();

            if (previous.containsKey(keyword)) {
                long prevCount = previous.get(keyword);
                double changeRate = prevCount > 0
                        ? Math.round(((count - prevCount) * 100.0 / prevCount) * 10.0) / 10.0
                        : 0;
                topKeywords.add(HourlyConsultResult.TopKeyword.builder()
                        .keyword(keyword).count(count).rank(rank).changeRate(changeRate)
                        .build());
            } else {
                newKeywords.add(HourlyConsultResult.NewKeyword.builder()
                        .keyword(keyword).count(count)
                        .build());
                topKeywords.add(HourlyConsultResult.TopKeyword.builder()
                        .keyword(keyword).count(count).rank(rank).changeRate(0)
                        .build());
            }
            rank++;
        }

        return HourlyConsultResult.KeywordAnalysis.builder()
                .topKeywords(topKeywords)
                .newKeywords(newKeywords)
                .build();
    }

    // ═══════════════════════════════════════════════════════
    //  전일 키워드 조회 (MongoDB snapshot에서)
    // ═══════════════════════════════════════════════════════

    private Map<String, Long> loadPreviousSlotKeywords(LocalDate prevDate, String slot) {
        Map<String, Long> result = new LinkedHashMap<>();
        LocalDateTime prevStart = prevDate.atStartOfDay();
        Query query = new Query(Criteria.where("startAt").is(prevStart));
        Document snapshot = mongoTemplate.findOne(query, Document.class, SNAPSHOT_COLLECTION);
        if (snapshot == null) return result;

        List<Document> trends = snapshot.getList("timeSlotTrend", Document.class);
        if (trends == null) return result;

        for (Document trend : trends) {
            if (slot.equals(trend.getString("slot"))) {
                Document kwAnalysis = trend.get("keywordAnalysis", Document.class);
                if (kwAnalysis == null) break;
                List<Document> topKws = kwAnalysis.getList("topKeywords", Document.class);
                if (topKws != null) {
                    for (Document kw : topKws) {
                        result.put(kw.getString("keyword"),
                                kw.get("count") instanceof Number
                                        ? ((Number) kw.get("count")).longValue() : 0L);
                    }
                }
                break;
            }
        }
        return result;
    }


    // ═══════════════════════════════════════════════════════
    //  기존 슬롯 합산 (변경 없음)
    // ═══════════════════════════════════════════════════════

    private List<HourlyConsultResult.TimeSlotResult> mergeWithExistingSlots(
            LocalDateTime dayStart, HourlyConsultResult.TimeSlotResult currentSlot) {

        List<HourlyConsultResult.TimeSlotResult> merged = new ArrayList<>();

        Query query = new Query(Criteria.where("startAt").is(dayStart));
        Document snapshot = mongoTemplate.findOne(query, Document.class, SNAPSHOT_COLLECTION);

        if (snapshot != null) {
            List<Document> existingTrends = snapshot.getList("timeSlotTrend", Document.class);
            if (existingTrends != null) {
                for (Document trend : existingTrends) {
                    String slot = trend.getString("slot");
                    if (!currentSlot.getSlot().equals(slot)) {
                        merged.add(HourlyConsultResult.TimeSlotResult.builder()
                                .slot(slot)
                                .consultCount(trend.getInteger("consultCount", 0))
                                .avgDuration(trend.get("avgDuration") instanceof Number
                                        ? ((Number) trend.get("avgDuration")).doubleValue() : 0)
                                .categoryBreakdown(convertCategoryBreakdown(
                                        trend.getList("categoryBreakdown", Document.class)))
                                .keywordAnalysis(convertKeywordAnalysis(
                                        trend.get("keywordAnalysis", Document.class)))
                                .build());
                    }
                }
            }
        }

        merged.add(currentSlot);
        merged.sort(Comparator.comparing(HourlyConsultResult.TimeSlotResult::getSlot));
        return merged;
    }

    private List<HourlyConsultResult.CategoryBreakdown> convertCategoryBreakdown(
            List<Document> docs) {
        if (docs == null) return List.of();
        return docs.stream().map(d -> HourlyConsultResult.CategoryBreakdown.builder()
                .code(d.getString("code")).name(d.getString("name"))
                .count(d.getInteger("count", 0))
                .rate(d.get("rate") instanceof Number ? ((Number) d.get("rate")).doubleValue() : 0)
                .build()).collect(Collectors.toList());
    }

    private HourlyConsultResult.KeywordAnalysis convertKeywordAnalysis(Document doc) {
        if (doc == null) {
            return HourlyConsultResult.KeywordAnalysis.builder()
                    .topKeywords(List.of()).newKeywords(List.of()).build();
        }
        List<Document> topDocs = doc.getList("topKeywords", Document.class);
        List<Document> newDocs = doc.getList("newKeywords", Document.class);

        List<HourlyConsultResult.TopKeyword> topKws = topDocs == null ? List.of() :
                topDocs.stream().map(d -> HourlyConsultResult.TopKeyword.builder()
                        .keyword(d.getString("keyword"))
                        .count(d.get("count") instanceof Number ? ((Number) d.get("count")).longValue() : 0)
                        .rank(d.getInteger("rank", 0))
                        .changeRate(d.get("changeRate") instanceof Number
                                ? ((Number) d.get("changeRate")).doubleValue() : 0)
                        .build()).collect(Collectors.toList());

        List<HourlyConsultResult.NewKeyword> newKws = newDocs == null ? List.of() :
                newDocs.stream().map(d -> HourlyConsultResult.NewKeyword.builder()
                        .keyword(d.getString("keyword"))
                        .count(d.get("count") instanceof Number ? ((Number) d.get("count")).longValue() : 0)
                        .build()).collect(Collectors.toList());

        return HourlyConsultResult.KeywordAnalysis.builder()
                .topKeywords(topKws).newKeywords(newKws).build();
    }

    // ═══════════════════════════════════════════════════════
    //  MongoDB: snapshot upsert (변경 없음)
    // ═══════════════════════════════════════════════════════

    private void upsertSnapshot(LocalDateTime dayStart, LocalDateTime dayEnd,
                                HourlyConsultResult result) {

        List<Document> trendDocs = result.getTimeSlotTrend().stream().map(slot -> {
            List<Document> catDocs = slot.getCategoryBreakdown().stream()
                    .map(c -> new Document("code", c.getCode()).append("name", c.getName())
                            .append("count", c.getCount()).append("rate", c.getRate()))
                    .collect(Collectors.toList());

            Document kwDoc = new Document();
            kwDoc.append("topKeywords", slot.getKeywordAnalysis().getTopKeywords().stream()
                    .map(k -> new Document("keyword", k.getKeyword()).append("count", k.getCount())
                            .append("rank", k.getRank()).append("changeRate", k.getChangeRate()))
                    .collect(Collectors.toList()));
            kwDoc.append("newKeywords", slot.getKeywordAnalysis().getNewKeywords().stream()
                    .map(k -> new Document("keyword", k.getKeyword()).append("count", k.getCount()))
                    .collect(Collectors.toList()));

            return new Document("slot", slot.getSlot())
                    .append("consultCount", slot.getConsultCount())
                    .append("avgDuration", slot.getAvgDuration())
                    .append("categoryBreakdown", catDocs)
                    .append("keywordAnalysis", kwDoc);
        }).collect(Collectors.toList());


        Query upsertQuery = new Query(Criteria.where("startAt").is(dayStart));
        Update update = new Update()
                .set("startAt", dayStart).set("endAt", dayEnd)
                .set("timeSlotTrend", trendDocs)
                .setOnInsert("createdAt", LocalDateTime.now());

        mongoTemplate.upsert(upsertQuery, update, SNAPSHOT_COLLECTION);
    }
}
package com.uplus.batch.jobs.weekly_report.step.performance;

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
import java.util.*;
import java.util.stream.Collectors;

/**
 * 전체 상담 성과 집계 Tasklet (주간/월간 공용)
 *
 * consultation_summary에서 startDate~endDate 범위로 집계:
 * - 전체 요약: 총 상담수, 평균소요시간, 평균만족도
 * - 상담사별 성과: 처리건수, 평균소요시간, 평균만족도
 * - 결과를 targetCollection(weekly_report_snapshot / monthly_report_snapshot)에 upsert
 */
@Slf4j
@Component
@StepScope
public class PerformanceTasklet implements Tasklet {

    private final MongoTemplate mongoTemplate;
    private final String startDateParam;
    private final String endDateParam;
    private final String targetCollection;

    private static final String SOURCE_COLLECTION = "consultation_summary";

    /** 대분류 코드 매핑 */
    private static final Map<String, String> LARGE_CATEGORY_CODE = Map.of(
            "요금/납부", "FEE",
            "기기변경", "DEV",
            "장애/A/S", "TRB",
            "해지/재약정", "CHN",
            "부가서비스", "ADD",
            "기타", "ETC"
    );

    public PerformanceTasklet(
            MongoTemplate mongoTemplate,
            @Value("#{jobParameters['startDate']}") String startDateParam,
            @Value("#{jobParameters['endDate']}") String endDateParam,
            @Value("#{jobParameters['targetCollection'] ?: 'weekly_report_snapshot'}") String targetCollection) {
        this.mongoTemplate = mongoTemplate;
        this.startDateParam = startDateParam;
        this.endDateParam = endDateParam;
        this.targetCollection = targetCollection;
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) {

        LocalDate startDate = LocalDate.parse(startDateParam);
        LocalDate endDate = LocalDate.parse(endDateParam);
        LocalDateTime startAt = startDate.atStartOfDay();
        LocalDateTime endAt = endDate.atTime(23, 59, 59);

        log.info("[Performance] {} ~ {} 집계 시작 → {}", startDate, endDate, targetCollection);

        // 0) 상담사별 응대 품질 점수 조회 (agent report snapshot에서)
        Map<Long, Double> qualityScoreMap = loadQualityScores(startAt, endAt);

        // 1) 전체 요약 집계
        Aggregation summaryAgg = Aggregation.newAggregation(
                Aggregation.match(
                        Criteria.where("consultedAt").gte(startAt).lte(endAt)
                ),
                Aggregation.group()
                        .count().as("totalCount")
                        .avg("durationSec").as("avgDurationSec")
                        .avg("customer.satisfiedScore").as("avgSatisfiedScore")
        );

        AggregationResults<Document> summaryResults =
                mongoTemplate.aggregate(summaryAgg, SOURCE_COLLECTION, Document.class);
        Document summaryDoc = summaryResults.getUniqueMappedResult();

        if (summaryDoc == null) {
            log.info("[Performance] {} ~ {} 데이터 없음. 스킵.", startDate, endDate);
            return RepeatStatus.FINISHED;
        }

        int totalCount = summaryDoc.getInteger("totalCount", 0);
        double avgDurationSec = getDoubleOrZero(summaryDoc, "avgDurationSec");
        double avgSatisfiedScore = getDoubleOrZero(summaryDoc, "avgSatisfiedScore");

        // 2) 상담사별 성과 집계
        Aggregation agentAgg = Aggregation.newAggregation(
                Aggregation.match(
                        Criteria.where("consultedAt").gte(startAt).lte(endAt)
                ),
                Aggregation.group("agent._id")
                        .first("agent.name").as("agentName")
                        .count().as("consultCount")
                        .avg("durationSec").as("avgDurationSec")
                        .avg("customer.satisfiedScore").as("avgSatisfiedScore")
        );

        AggregationResults<Document> agentResults =
                mongoTemplate.aggregate(agentAgg, SOURCE_COLLECTION, Document.class);

        List<PerformanceResult.AgentPerformance> agentPerformance = agentResults.getMappedResults()
                .stream()
                .map(doc -> PerformanceResult.AgentPerformance.builder()
                        .agentId(getAgentId(doc))
                        .agentName(doc.getString("agentName"))
                        .consultCount(doc.getInteger("consultCount", 0))
                        .avgDurationMinutes(Math.round(getDoubleOrZero(doc, "avgDurationSec") / 60.0 * 10.0) / 10.0)
                        .avgSatisfiedScore(Math.round(getDoubleOrZero(doc, "avgSatisfiedScore") * 10.0) / 10.0)
                        .qualityScore(qualityScoreMap.get(getAgentId(doc)))
                        .build())
                .collect(Collectors.toList());

        // 상담사 수는 agent 집계 결과에서 직접 계산
        int agentCount = agentPerformance.size();
        double avgConsultCountPerAgent = agentCount > 0
                ? Math.round((double) totalCount / agentCount * 10.0) / 10.0
                : 0;

        // 종합 점수 기반 정렬: 처리건수(25%) + 소요시간(15%) + 응대품질(30%) + 만족도(30%)
        if (agentPerformance.size() > 1) {
            double maxConsult = agentPerformance.stream()
                    .mapToInt(PerformanceResult.AgentPerformance::getConsultCount).max().orElse(1);
            double minConsult = agentPerformance.stream()
                    .mapToInt(PerformanceResult.AgentPerformance::getConsultCount).min().orElse(0);
            double maxDuration = agentPerformance.stream()
                    .mapToDouble(PerformanceResult.AgentPerformance::getAvgDurationMinutes).max().orElse(1);
            double minDuration = agentPerformance.stream()
                    .mapToDouble(PerformanceResult.AgentPerformance::getAvgDurationMinutes).min().orElse(0);

            agentPerformance.sort(Comparator.comparingDouble(
                    (PerformanceResult.AgentPerformance ap) -> calculateCompositeScore(
                            ap, minConsult, maxConsult, minDuration, maxDuration)
            ).reversed());
        }

        log.info("[Performance] 전체 — {}건, 상담사 {}명, 평균 {}건/인, 평균소요 {}분, 만족도 {}",
                totalCount, agentCount, avgConsultCountPerAgent,
                Math.round(avgDurationSec / 60.0 * 10.0) / 10.0,
                Math.round(avgSatisfiedScore * 10.0) / 10.0);

        double avgQualityScore = agentPerformance.stream()
                .map(PerformanceResult.AgentPerformance::getQualityScore)
                .filter(q -> q != null)
                .mapToDouble(Double::doubleValue)
                .average().orElse(0.0);
        log.info("[Performance] 상담사 {}명 집계 완료, 평균 응대품질 {}",
                agentPerformance.size(), Math.round(avgQualityScore * 10.0) / 10.0);

        // 3) 카테고리별 빈도 집계
        List<Document> categoryRanking = aggregateCategory(startAt, endAt);
        log.info("[Performance] 카테고리 {}종 집계", categoryRanking.size());

        // 4) 결과 빌드
        PerformanceResult result = PerformanceResult.builder()
                .totalConsultCount(totalCount)
                .avgConsultCountPerAgent(avgConsultCountPerAgent)
                .avgDurationMinutes(Math.round(avgDurationSec / 60.0 * 10.0) / 10.0)
                .avgSatisfiedScore(Math.round(avgSatisfiedScore * 10.0) / 10.0)
                .agentPerformance(agentPerformance)
                .build();

        // 5) 스냅샷 upsert
        upsertSnapshot(startAt, endAt, result, categoryRanking);

        log.info("[Performance] {} ~ {} → {} upsert 완료", startDate, endDate, targetCollection);
        return RepeatStatus.FINISHED;
    }

    private void upsertSnapshot(LocalDateTime startAt, LocalDateTime endAt,
                                PerformanceResult result, List<Document> categoryRanking) {
        int rank = 1;
        List<Document> agentDocs = new ArrayList<>();
        for (var a : result.getAgentPerformance()) {
            agentDocs.add(new Document("agentId", a.getAgentId())
                    .append("agentName", a.getAgentName())
                    .append("consultCount", a.getConsultCount())
                    .append("avgDurationMinutes", a.getAvgDurationMinutes())
                    .append("qualityScore", a.getQualityScore())
                    .append("avgSatisfiedScore", a.getAvgSatisfiedScore())
                    .append("rank", rank++));
        }

        Document performanceSummary = new Document()
                .append("avgConsultPerAgent", result.getAvgConsultCountPerAgent())
                .append("avgSatisfiedScore", result.getAvgSatisfiedScore())
                .append("categoryRanking", categoryRanking)
                .append("agentRanking", agentDocs);

        Query query = new Query(Criteria.where("startAt").is(startAt));
        Update update = new Update()
                .set("startAt", startAt)
                .set("endAt", endAt)
                .set("totalConsultCount", result.getTotalConsultCount())
                .set("avgDurationMinutes", result.getAvgDurationMinutes())
                .set("performanceSummary", performanceSummary)
                .setOnInsert("createdAt", LocalDateTime.now());

        mongoTemplate.upsert(query, update, targetCollection);
    }

    /**
     * 카테고리별 상담 빈도 집계
     */
    private List<Document> aggregateCategory(LocalDateTime startAt, LocalDateTime endAt) {
        Aggregation agg = Aggregation.newAggregation(
                Aggregation.match(Criteria.where("consultedAt").gte(startAt).lte(endAt)),
                Aggregation.group("category.large").count().as("count"),
                Aggregation.sort(Sort.Direction.DESC, "count")
        );

        List<Document> results = mongoTemplate.aggregate(agg, SOURCE_COLLECTION, Document.class)
                .getMappedResults();

        List<Document> categoryRanking = new ArrayList<>();
        int rank = 1;
        for (Document doc : results) {
            String name = doc.getString("_id");
            if (name == null) continue;
            String code = LARGE_CATEGORY_CODE.getOrDefault(name, "ETC");
            categoryRanking.add(new Document("code", code)
                    .append("name", name)
                    .append("count", doc.getInteger("count", 0))
                    .append("rank", rank++));
        }
        return categoryRanking;
    }

    private double getDoubleOrZero(Document doc, String field) {
        Object val = doc.get(field);
        return val instanceof Number ? ((Number) val).doubleValue() : 0.0;
    }

    private long getAgentId(Document doc) {
        Object id = doc.get("_id");
        return id instanceof Number ? ((Number) id).longValue() : 0L;
    }

    /**
     * 상담사별 응대 품질 점수(totalScore)를 agent report snapshot에서 조회.
     * targetCollection에 따라 대응하는 agent report 컬렉션을 결정:
     *   weekly_report_snapshot  → weekly_agent_report_snapshot
     *   monthly_report_snapshot → monthly_agent_report_snapshot
     */
    private Map<Long, Double> loadQualityScores(LocalDateTime startAt, LocalDateTime endAt) {
        String agentCollection = targetCollection.replace("_report_snapshot", "_agent_report_snapshot");
        Map<Long, Double> scoreMap = new HashMap<>();

        try {
            Query query = new Query(
                    Criteria.where("startAt").gte(startAt)
                            .and("endAt").lte(endAt)
                            .and("qualityAnalysis").ne(null)
            );
            query.fields().include("agentId").include("qualityAnalysis.totalScore");

            List<Document> docs = mongoTemplate.find(query, Document.class, agentCollection);
            for (Document doc : docs) {
                long agentId = getLongFromDoc(doc, "agentId");
                Document qa = doc.get("qualityAnalysis", Document.class);
                if (agentId > 0 && qa != null) {
                    double totalScore = getDoubleOrZero(qa, "totalScore");
                    scoreMap.put(agentId, Math.round(totalScore * 10.0) / 10.0);
                }
            }
            log.info("[Performance] 응대 품질 점수 {}명 로드 ({})", scoreMap.size(), agentCollection);
        } catch (Exception e) {
            log.warn("[Performance] 응대 품질 점수 로드 실패: {}", e.getMessage());
        }

        return scoreMap;
    }

    private long getLongFromDoc(Document doc, String field) {
        Object val = doc.get(field);
        return val instanceof Number ? ((Number) val).longValue() : 0L;
    }

    /**
     * 종합 순위 점수 계산 (0~1 스케일)
     *
     * 가중치:
     *   처리 건수   25% — 생산성 (많을수록 높은 점수)
     *   소요 시간   15% — 효율성 (짧을수록 높은 점수)
     *   응대 품질   30% — 서비스 품질 (0~5)
     *   고객 만족도  30% — 고객 평가 (0~5)
     */
    private double calculateCompositeScore(
            PerformanceResult.AgentPerformance ap,
            double minConsult, double maxConsult,
            double minDuration, double maxDuration) {

        // 처리 건수: min-max 정규화 (높을수록 좋음)
        double consultNorm = (maxConsult == minConsult)
                ? 1.0 : (ap.getConsultCount() - minConsult) / (maxConsult - minConsult);

        // 소요 시간: min-max 정규화 반전 (낮을수록 좋음)
        double durationNorm = (maxDuration == minDuration)
                ? 1.0 : 1.0 - (ap.getAvgDurationMinutes() - minDuration) / (maxDuration - minDuration);

        // 응대 품질: 0~5 → 0~1
        double qualityNorm = (ap.getQualityScore() != null ? ap.getQualityScore() : 0.0) / 5.0;

        // 고객 만족도: 0~5 → 0~1
        double satisfactionNorm = ap.getAvgSatisfiedScore() / 5.0;

        return consultNorm * 0.25 + durationNorm * 0.15 + qualityNorm * 0.30 + satisfactionNorm * 0.30;
    }
}

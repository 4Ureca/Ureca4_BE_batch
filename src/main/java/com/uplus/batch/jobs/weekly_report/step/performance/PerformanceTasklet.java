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
import java.util.Comparator;
import java.util.List;
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
                Aggregation.group("agent.id")
                        .first("agent.name").as("agentName")
                        .count().as("consultCount")
                        .avg("durationSec").as("avgDurationSec")
                        .avg("customer.satisfiedScore").as("avgSatisfiedScore"),
                Aggregation.sort(Sort.Direction.DESC, "consultCount")
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
                        .qualityScore(null)
                        .build())
                .collect(Collectors.toList());

        // 상담사 수는 agent 집계 결과에서 직접 계산
        int agentCount = agentPerformance.size();
        double avgConsultCountPerAgent = agentCount > 0
                ? Math.round((double) totalCount / agentCount * 10.0) / 10.0
                : 0;

        log.info("[Performance] 전체 — {}건, 상담사 {}명, 평균 {}건/인, 평균소요 {}분, 만족도 {}",
                totalCount, agentCount, avgConsultCountPerAgent,
                Math.round(avgDurationSec / 60.0 * 10.0) / 10.0,
                Math.round(avgSatisfiedScore * 10.0) / 10.0);
        log.info("[Performance] 상담사 {}명 집계 완료", agentPerformance.size());

        // 3) 결과 빌드
        PerformanceResult result = PerformanceResult.builder()
                .totalConsultCount(totalCount)
                .avgConsultCountPerAgent(avgConsultCountPerAgent)
                .avgDurationMinutes(Math.round(avgDurationSec / 60.0 * 10.0) / 10.0)
                .avgSatisfiedScore(Math.round(avgSatisfiedScore * 10.0) / 10.0)
                .agentPerformance(agentPerformance)
                .build();

        // 4) 스냅샷 upsert
        upsertSnapshot(startAt, endAt, result);

        log.info("[Performance] {} ~ {} → {} upsert 완료", startDate, endDate, targetCollection);
        return RepeatStatus.FINISHED;
    }

    private void upsertSnapshot(LocalDateTime startAt, LocalDateTime endAt, PerformanceResult result) {
        List<Document> agentDocs = result.getAgentPerformance().stream()
                .map(a -> new Document("agentId", a.getAgentId())
                        .append("agentName", a.getAgentName())
                        .append("consultCount", a.getConsultCount())
                        .append("avgDurationMinutes", a.getAvgDurationMinutes())
                        .append("avgSatisfiedScore", a.getAvgSatisfiedScore())
                        .append("qualityScore", a.getQualityScore()))
                .collect(Collectors.toList());

        Query query = new Query(Criteria.where("startAt").is(startAt));
        Update update = new Update()
                .set("startAt", startAt)
                .set("endAt", endAt)
                .set("totalConsultCount", result.getTotalConsultCount())
                .set("avgConsultCountPerAgent", result.getAvgConsultCountPerAgent())
                .set("avgDurationMinutes", result.getAvgDurationMinutes())
                .set("avgSatisfiedScore", result.getAvgSatisfiedScore())
                .set("agentPerformance", agentDocs)
                .setOnInsert("createdAt", LocalDateTime.now());

        mongoTemplate.upsert(query, update, targetCollection);
    }

    private double getDoubleOrZero(Document doc, String field) {
        Object val = doc.get(field);
        return val instanceof Number ? ((Number) val).doubleValue() : 0.0;
    }

    private long getAgentId(Document doc) {
        Object id = doc.get("_id");
        return id instanceof Number ? ((Number) id).longValue() : 0L;
    }
}

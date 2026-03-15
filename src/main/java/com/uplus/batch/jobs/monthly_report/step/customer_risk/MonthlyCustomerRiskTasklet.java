package com.uplus.batch.jobs.monthly_report.step.customer_risk;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;

/**
 * 월별 고객 특이사항(위험) 집계 Tasklet
 *
 * consultation_summary의 riskFlags를 월 단위로 집계하여
 * monthly_report_snapshot에 customerRiskAnalysis 필드로 upsert.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class MonthlyCustomerRiskTasklet implements Tasklet {

    private final MongoTemplate mongoTemplate;
    private final JdbcTemplate jdbcTemplate;

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) {

        // jobParameters에서 날짜 조회, 없으면 지난달 기준
        Map<String, Object> params = chunkContext.getStepContext().getJobParameters();
        String startDateStr = (String) params.get("startDate");
        String endDateStr = (String) params.get("endDate");

        LocalDateTime startAt;
        LocalDateTime endAt;

        if (startDateStr != null && endDateStr != null) {
            startAt = LocalDate.parse(startDateStr).atStartOfDay();
            endAt = LocalDate.parse(endDateStr).atTime(23, 59, 59);
        } else {
            LocalDate lastMonth = LocalDate.now().minusMonths(1);
            startAt = lastMonth.withDayOfMonth(1).atStartOfDay();
            endAt = lastMonth.withDayOfMonth(lastMonth.lengthOfMonth()).atTime(23, 59, 59);
        }

        log.info("[MonthlyCustomerRisk] {} ~ {} 집계 시작", startAt, endAt);

        // 1. MySQL에서 활성 위험유형 로드
        Map<String, String> activeRiskTypes = loadActiveRiskTypes();

        // 2. MongoDB에서 riskFlags 집계
        Map<String, Integer> counts = aggregateRiskFlags(startAt, endAt, activeRiskTypes.keySet());

        int totalRisk = counts.values().stream().mapToInt(Integer::intValue).sum();

        // 3. monthly_report_snapshot에 customerRiskAnalysis upsert
        Document riskDoc = new Document()
                .append("fraudSuspect", counts.getOrDefault("FRAUD", 0))
                .append("maliciousComplaint", counts.getOrDefault("ABUSE", 0))
                .append("policyAbuse", counts.getOrDefault("POLICY", 0))
                .append("excessiveCompensation", counts.getOrDefault("COMP", 0))
                .append("repeatedComplaint", counts.getOrDefault("REPEAT", 0))
                .append("phishingVictim", counts.getOrDefault("PHISHING", 0))
                .append("churnRisk", counts.getOrDefault("CHURN", 0))
                .append("totalRiskCount", totalRisk);

        Query upsertQuery = new Query(Criteria.where("startAt").is(startAt));
        Update update = new Update()
                .set("startAt", startAt)
                .set("endAt", endAt)
                .set("customerRiskAnalysis", riskDoc)
                .setOnInsert("createdAt", LocalDateTime.now());

        mongoTemplate.upsert(upsertQuery, update, "monthly_report_snapshot");

        log.info("[MonthlyCustomerRisk] {} ~ {} 집계 완료", startAt, endAt);
        return RepeatStatus.FINISHED;
    }

    private Map<String, String> loadActiveRiskTypes() {
        String sql = "SELECT type_code, type_name FROM risk_type_policy WHERE is_active = 1";

        Map<String, String> riskTypes = new LinkedHashMap<>();
        jdbcTemplate.query(sql, rs -> {
            riskTypes.put(rs.getString("type_code"), rs.getString("type_name"));
        });

        return riskTypes;
    }

    private Map<String, Integer> aggregateRiskFlags(LocalDateTime start, LocalDateTime end,
                                                     Set<String> activeTypeCodes) {
        Aggregation aggregation = Aggregation.newAggregation(
                Aggregation.match(
                        Criteria.where("consultedAt").gte(start).lte(end)
                                .and("category.code").not().regex("^M_OTB")
                                .and("riskFlags.0").exists(true)
                ),
                Aggregation.unwind("riskFlags"),
                Aggregation.group("riskFlags.riskType").count().as("count"),
                Aggregation.sort(Sort.Direction.DESC, "count")
        );

        AggregationResults<Document> results = mongoTemplate.aggregate(
                aggregation, "consultation_summary", Document.class);

        Map<String, Integer> counts = new LinkedHashMap<>();
        activeTypeCodes.forEach(code -> counts.put(code, 0));

        for (Document doc : results.getMappedResults()) {
            Object idObj = doc.get("_id");
            if (!(idObj instanceof String)) continue;
            String typeCode = (String) idObj;
            int count = doc.getInteger("count", 0);
            if (activeTypeCodes.contains(typeCode)) {
                counts.put(typeCode, count);
            }
        }

        return counts;
    }
}

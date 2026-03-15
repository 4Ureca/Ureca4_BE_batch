package com.uplus.batch.jobs.daily_report.step.customer_risk;

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
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;

/**
 * 고객 특이사항(위험) 일별 집계 Tasklet (daily 전용)
 */
@Slf4j
@Component
@StepScope
public class CustomerRiskTasklet implements Tasklet {

    private final MongoTemplate mongoTemplate;
    private final JdbcTemplate jdbcTemplate;
    private final String targetDateParam;

    public static final String RESULT_KEY = "customerRiskResult";

    public CustomerRiskTasklet(
            MongoTemplate mongoTemplate,
            JdbcTemplate jdbcTemplate,
            @Value("#{jobParameters['targetDate'] ?: null}") String targetDateParam) {
        this.mongoTemplate = mongoTemplate;
        this.jdbcTemplate = jdbcTemplate;
        this.targetDateParam = targetDateParam;
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) {

        LocalDate targetDate = (targetDateParam != null && !targetDateParam.isEmpty())
                ? LocalDate.parse(targetDateParam)
                : LocalDate.now().minusDays(1);
        LocalDateTime start = targetDate.atStartOfDay();
        LocalDateTime end = targetDate.atTime(23, 59, 59);

        log.info("[CustomerRisk] {} 집계 시작", targetDate);

        Map<String, String> activeRiskTypes = loadActiveRiskTypes();

        Map<String, Integer> todayCounts = aggregateRiskFlags(start, end, activeRiskTypes.keySet());

        CustomerRiskResult result = buildResult(todayCounts);

        contribution.getStepExecution()
                .getJobExecution()
                .getExecutionContext()
                .put(RESULT_KEY, result);

        // 6) daily_report_snapshot에 customerRiskAnalysis upsert
        Document riskDoc = new Document()
                .append("fraudSuspect", result.getFraudSuspect())
                .append("maliciousComplaint", result.getMaliciousComplaint())
                .append("policyAbuse", result.getPolicyAbuse())
                .append("excessiveCompensation", result.getExcessiveCompensation())
                .append("repeatedComplaint", result.getRepeatedComplaint())
                .append("phishingVictim", result.getPhishingVictim())
                .append("churnRisk", result.getChurnRisk())
                .append("totalRiskCount", result.getTotalRiskCount());

        Query upsertQuery = new Query(Criteria.where("startAt").is(start));
        Update update = new Update()
                .set("startAt", start)
                .set("endAt", end)
                .set("customerRiskAnalysis", riskDoc)
                .setOnInsert("createdAt", LocalDateTime.now());

        mongoTemplate.upsert(upsertQuery, update, "daily_report_snapshot");

        log.info("[CustomerRisk] {} 집계 완료", targetDate);
        return RepeatStatus.FINISHED;
    }

    private Map<String, String> loadActiveRiskTypes() {
        String sql = "SELECT type_code, type_name FROM risk_type_policy "
                   + "WHERE is_active = 1";

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

    private CustomerRiskResult buildResult(Map<String, Integer> todayCounts) {
        return CustomerRiskResult.builder()
                .fraudSuspect(todayCounts.getOrDefault("FRAUD", 0))
                .maliciousComplaint(todayCounts.getOrDefault("ABUSE", 0))
                .policyAbuse(todayCounts.getOrDefault("POLICY", 0))
                .excessiveCompensation(todayCounts.getOrDefault("COMP", 0))
                .repeatedComplaint(todayCounts.getOrDefault("REPEAT", 0))
                .phishingVictim(todayCounts.getOrDefault("PHISHING", 0))
                .churnRisk(todayCounts.getOrDefault("CHURN", 0))
                .build();
    }
}
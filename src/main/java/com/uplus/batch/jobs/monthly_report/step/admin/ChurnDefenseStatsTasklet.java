package com.uplus.batch.jobs.monthly_report.step.admin;

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
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

/**
 * 월별 해지방어 패턴 분석 Tasklet
 *
 * consultation_summary.cancellation 필드를 집계하여
 * monthly_report_snapshot.churnDefenseAnalysis에 upsert합니다.
 *
 * 집계 항목:
 *  - totalAttempts      : 해지방어 시도 건수
 *  - successCount       : 방어 성공 건수
 *  - successRate        : 성공률 (%)
 *  - avgDurationSec     : 해지 의향 상담 평균 소요 시간(초)
 *  - complaintCategory[] : 불만 사유별 {reason, attempts, successCount, successRate, avgDurationSec}
 *  - byCustomerType[]   : 연령+성별 기준 {type, mainComplaintReason, attempts, successRate}
 *  - byAction[]         : 방어 액션별 {action, attempts, successRate}
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ChurnDefenseStatsTasklet implements Tasklet {

    private final MongoTemplate mongoTemplate;

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) {

        // 1. 집계 기간 설정
        var jobParams = chunkContext.getStepContext().getJobParameters();
        String startDateStr = (String) jobParams.get("startDate");
        String endDateStr = (String) jobParams.get("endDate");

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

        log.info("[ChurnDefense] {} ~ {} 집계 시작", startAt, endAt);

        // 2. consultation_summary에서 해지 의향(cancellation.intent=true) 건 조회
        Query query = new Query(Criteria.where("consultedAt").gte(startAt).lte(endAt)
                .and("cancellation.intent").is(true));
        List<Document> intentDocs = mongoTemplate.find(query, Document.class, "consultation_summary");

        if (intentDocs.isEmpty()) {
            return RepeatStatus.FINISHED;
        }

        // 3. 집계 변수 초기화
        int totalAttempts = 0;
        int successCount = 0;
        long totalDurationSec = 0;
        int durationCount = 0;

        // 불만 사유별: reason → {attempts, success, totalDuration, durationCount}
        Map<String, int[]> complaintMap = new HashMap<>();
        // 고객 유형별(연령+성별): "20대 남성" → {attempts, success, reason별 Map}
        Map<String, int[]> customerTypeMap = new HashMap<>();
        Map<String, Map<String, Integer>> customerTypeReasonMap = new HashMap<>();
        // 방어 액션별: action → {attempts, success}
        Map<String, int[]> actionMap = new HashMap<>();
        // 액션별 불만 사유: action → reason → {attempts, success}
        Map<String, Map<String, int[]>> actionReasonMap = new HashMap<>();

        // 4. 데이터 순회 및 집계
        for (Document doc : intentDocs) {
            Document cancellation = doc.get("cancellation", Document.class);
            if (cancellation == null) continue;

            boolean defenseAttempted = Boolean.TRUE.equals(cancellation.getBoolean("defenseAttempted"));
            boolean defenseSuccess = Boolean.TRUE.equals(cancellation.getBoolean("defenseSuccess"));

            if (defenseAttempted) {
                totalAttempts++;
                if (defenseSuccess) {
                    successCount++;
                }
            }

            // 소요시간
            Integer durationSec = doc.getInteger("durationSec");
            if (durationSec != null) {
                totalDurationSec += durationSec;
                durationCount++;
            }

            // 불만 사유
            String complaintReason = cancellation.getString("complaintCategory");
            if (complaintReason != null && !complaintReason.isBlank()) {
                // [0]=attempts, [1]=success, [2]=totalDuration, [3]=durationCount
                int[] stats = complaintMap.computeIfAbsent(complaintReason, k -> new int[4]);
                if (defenseAttempted) {
                    stats[0]++;
                    if (defenseSuccess) stats[1]++;
                }
                if (durationSec != null) {
                    stats[2] += durationSec;
                    stats[3]++;
                }
            }

            // 고객 유형 (연령+성별)
            Document customer = doc.get("customer", Document.class);
            String ageGroup = (customer != null) ? customer.getString("ageGroup") : null;
            String gender = (customer != null) ? customer.getString("gender") : null;

            String customerType;
            if (ageGroup != null) {
                customerType = ageGroup + " " + (gender != null ? gender : "미상");
            } else {
                customerType = "기타";
            }

            if (defenseAttempted) {
                int[] ctStats = customerTypeMap.computeIfAbsent(customerType, k -> new int[2]);
                ctStats[0]++; // attempts
                if (defenseSuccess) ctStats[1]++; // success
            }

            // 고객 유형별 불만 사유 (주요 불만 사유 산출용)
            if (complaintReason != null && !complaintReason.isBlank()) {
                customerTypeReasonMap
                        .computeIfAbsent(customerType, k -> new HashMap<>())
                        .merge(complaintReason, 1, Integer::sum);
            }

            // 방어 액션별
          String defenseAction = cancellation.getString("defenseCategory");
          if (defenseAction != null && !defenseAction.isBlank() && defenseAttempted) {
            // 기존에 for문을 돌던 로직을 단일 액션 처리로 변경
            int[] aCounts = actionMap.computeIfAbsent(defenseAction, k -> new int[2]);
            aCounts[0]++; // attempts
            if (defenseSuccess) aCounts[1]++; // success

            // 액션별 불만 사유 교차 집계
            if (complaintReason != null && !complaintReason.isBlank()) {
              int[] arStats = actionReasonMap
                  .computeIfAbsent(defenseAction, k -> new HashMap<>())
                  .computeIfAbsent(complaintReason, k -> new int[2]);
              arStats[0]++;
              if (defenseSuccess) arStats[1]++;
            }
          }
        }

        // 5. 결과 Document 조립
        double successRate = calcRate(successCount, totalAttempts);
        int avgDurationSec = durationCount > 0 ? (int) (totalDurationSec / durationCount) : 0;

        Document churnDefense = new Document();
        churnDefense.put("totalAttempts", totalAttempts);
        churnDefense.put("successCount", successCount);
        churnDefense.put("successRate", successRate);
        churnDefense.put("avgDurationSec", avgDurationSec);

        // ── 불만 사유별 방어율 (시도 건수 내림차순) ──
        List<Document> complaintCategory = complaintMap.entrySet().stream()
                .sorted((a, b) -> Integer.compare(b.getValue()[0], a.getValue()[0]))
                .map(e -> {
                    int[] s = e.getValue();
                    int avgDur = s[3] > 0 ? s[2] / s[3] : 0;

                    return new Document("reason", e.getKey())
                            .append("attempts", s[0])
                            .append("successCount", s[1])
                            .append("successRate", calcRate(s[1], s[0]))
                            .append("avgDurationSec", avgDur);
                })
                .collect(Collectors.toList());
        churnDefense.put("complaintReasons", complaintCategory);

        // ── 고객 유형별 (연령+성별, 건수 내림차순) ──
        List<Document> byCustomerType = customerTypeMap.entrySet().stream()
                .sorted((a, b) -> Integer.compare(b.getValue()[0], a.getValue()[0]))
                .map(e -> {
                    int attempts = e.getValue()[0];
                    int success = e.getValue()[1];
                    // 주요 불만 사유 산출
                    String mainReason = findMainReason(customerTypeReasonMap.get(e.getKey()));
                    return new Document("type", e.getKey())
                            .append("mainComplaintReason", mainReason)
                            .append("attempts", attempts)
                            .append("successRate", calcRate(success, attempts));
                })
                .collect(Collectors.toList());
        churnDefense.put("byCustomerType", byCustomerType);

        // ── 방어 액션별 (시도 건수 내림차순, rank 부여) ──
        List<Map.Entry<String, int[]>> sortedGlobalActions = actionMap.entrySet().stream()
                .sorted((a, b) -> Integer.compare(b.getValue()[0], a.getValue()[0]))
                .collect(Collectors.toList());

        List<Document> byAction = new ArrayList<>();
        for (int i = 0; i < sortedGlobalActions.size(); i++) {
            var entry = sortedGlobalActions.get(i);
            int attempts = entry.getValue()[0];
            int success = entry.getValue()[1];

            // 해당 액션의 불만 사유별 분석
            Map<String, int[]> reasons = actionReasonMap.getOrDefault(entry.getKey(), Map.of());
            List<Document> reasonDocs = reasons.entrySet().stream()
                    .sorted((x, y) -> Integer.compare(y.getValue()[0], x.getValue()[0]))
                    .map(r -> new Document("reason", r.getKey())
                            .append("attempts", r.getValue()[0])
                            .append("successRate", calcRate(r.getValue()[1], r.getValue()[0])))
                    .collect(Collectors.toList());

            byAction.add(new Document("action", entry.getKey())
                    .append("attempts", attempts)
                    .append("successRate", calcRate(success, attempts))
                    .append("rank", i + 1)
                    .append("byReason", reasonDocs));
        }
        churnDefense.put("byAction", byAction);

        // 6. monthly_report_snapshot에 upsert
        Query upsertQuery = new Query(
                Criteria.where("startAt").is(startAt)
                        .and("endAt").is(endAt)
        );

        Update update = new Update()
                .set("churnDefenseAnalysis", churnDefense)
                .setOnInsert("startAt", startAt)
                .setOnInsert("endAt", endAt);

        mongoTemplate.upsert(upsertQuery, update, "monthly_report_snapshot");

        log.info("[ChurnDefense] {} ~ {} 집계 완료", startAt, endAt);
        return RepeatStatus.FINISHED;
    }

    /** 성공률 계산 (소수점 1자리) */
    private double calcRate(int success, int total) {
        return total > 0 ? Math.round((double) success / total * 1000.0) / 10.0 : 0.0;
    }

    /** 가장 빈도 높은 불만 사유 반환 */
    private String findMainReason(Map<String, Integer> reasonMap) {
        if (reasonMap == null || reasonMap.isEmpty()) return null;
        return reasonMap.entrySet().stream()
                .max(Map.Entry.comparingByValue())
                .map(Map.Entry::getKey)
                .orElse(null);
    }
}

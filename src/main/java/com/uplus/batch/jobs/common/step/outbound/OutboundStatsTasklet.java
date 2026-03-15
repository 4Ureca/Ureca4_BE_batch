package com.uplus.batch.jobs.common.step.outbound;

import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;
import java.util.stream.Collectors;

/**
 * 아웃바운드 상담 통계 집계 Tasklet (일간/주간/월간 공용)
 *
 * consultation_summary에서 category.code가 M_OTB로 시작하는 아웃바운드 데이터를 집계하여
 * 기존 스냅샷 컬렉션(daily/weekly/monthly_report_snapshot)의
 * outboundAnalysis 필드에 upsert.
 */
@Slf4j
@Component
@StepScope
public class OutboundStatsTasklet implements Tasklet {

    private final MongoTemplate mongoTemplate;
    private final JdbcTemplate jdbcTemplate;
    private final String startDateParam;
    private final String endDateParam;
    private final String targetCollection;

    private static final String SOURCE = "consultation_summary";
    private static final String[] DAY_NAMES = {"일", "월", "화", "수", "목", "금", "토"};
    private static final ZoneId KST = ZoneId.of("Asia/Seoul");

    /** 상품 코드 → 월정액(원) 매핑 */
    private Map<String, Integer> productPriceMap;

    public OutboundStatsTasklet(
            MongoTemplate mongoTemplate,
            JdbcTemplate jdbcTemplate,
            @Value("#{jobParameters['startDate']}") String startDateParam,
            @Value("#{jobParameters['endDate']}") String endDateParam,
            @Value("#{jobParameters['targetCollection'] ?: 'daily_report_snapshot'}") String targetCollection) {
        this.mongoTemplate = mongoTemplate;
        this.jdbcTemplate = jdbcTemplate;
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

        log.info("[OutboundStats] {} ~ {} 집계 시작", startDate, endDate);

        productPriceMap = loadProductPriceMap();

        Criteria baseCriteria = Criteria.where("category.code").regex("^M_OTB")
                .and("consultedAt").gte(startAt).lte(endAt);

        List<Document> allDocs = mongoTemplate.find(new Query(baseCriteria), Document.class, SOURCE)
                .stream()
                .filter(doc -> {
                    LocalDateTime kst = toKstDateTime(doc.get("consultedAt"));
                    if (kst == null) return false;
                    int dow = kst.getDayOfWeek().getValue(); // 1=월 ~ 7=일
                    return dow >= 1 && dow <= 5; // 평일만
                })
                .collect(Collectors.toList());

        if (allDocs.isEmpty()) {
            log.info("[OutboundStats] 아웃바운드 데이터 없음, 스킵");
            return RepeatStatus.FINISHED;
        }

        // 7개 지표 집계
        Document kpiDoc = aggregateKpi(allDocs);
        List<Document> campaignDocs = aggregateCampaigns(allDocs);
        Document callResultDoc = aggregateCallResults(allDocs);
        List<Document> agentDocs = aggregateAgents(allDocs);
        List<Document> heatmapDocs = aggregateHeatmap(allDocs);
        List<Document> conversionDocs = aggregateConversionByCategory(allDocs);
        List<Document> optimalTimeDocs = aggregateOptimalTime(allDocs);

        // outboundAnalysis로 네스팅하여 기존 스냅샷에 upsert
        Document outboundAnalysis = new Document()
                .append("kpi", kpiDoc)
                .append("campaigns", campaignDocs)
                .append("callResults", callResultDoc)
                .append("agents", agentDocs)
                .append("heatmap", new Document("conversionRate", heatmapDocs))
                .append("conversionByCategory", conversionDocs)
                .append("optimalTime", optimalTimeDocs);

        Query query = new Query(Criteria.where("startAt").is(startAt));
        Update update = new Update().set("outboundAnalysis", outboundAnalysis);
        mongoTemplate.upsert(query, update, targetCollection);

        log.info("[OutboundStats] {} ~ {} 집계 완료 ({}건)", startDate, endDate, allDocs.size());
        return RepeatStatus.FINISHED;
    }

    // ==================== KPI ====================

    private Document aggregateKpi(List<Document> docs) {
        int total = docs.size();
        int converted = 0, rejected = 0;
        double totalDuration = 0;
        long totalRevenue = 0;

        for (Document doc : docs) {
            Document outbound = doc.get("outbound", Document.class);
            if (outbound == null) continue;

            String callResult = outbound.getString("callResult");
            if ("CONVERTED".equals(callResult)) {
                converted++;
                totalRevenue += calculateRevenueFromProducts(doc);
            } else if ("REJECTED".equals(callResult)) {
                rejected++;
            }
            totalDuration += getDouble(doc, "durationSec");
        }

        return new Document()
                .append("totalCount", total)
                .append("convertedCount", converted)
                .append("rejectedCount", rejected)
                .append("conversionRate", ratio(converted, total))
                .append("avgDurationSec", avg(totalDuration, total))
                .append("estimatedRevenue", totalRevenue);
    }

    // ==================== 캠페인 성과 ====================

    private List<Document> aggregateCampaigns(List<Document> docs) {
        Map<String, List<Document>> byCategory = docs.stream()
                .collect(Collectors.groupingBy(doc -> {
                    Document cat = doc.get("category", Document.class);
                    return cat != null ? cat.getString("code") : "UNKNOWN";
                }));

        List<Document> result = new ArrayList<>();
        for (Map.Entry<String, List<Document>> entry : byCategory.entrySet()) {
            List<Document> group = entry.getValue();
            int total = group.size();
            int converted = 0;
            double totalDuration = 0;
            double totalSatisfied = 0;
            int satisfiedCount = 0;
            long totalRevenue = 0;

            for (Document doc : group) {
                Document outbound = doc.get("outbound", Document.class);
                if (outbound != null && "CONVERTED".equals(outbound.getString("callResult"))) {
                    converted++;
                    totalRevenue += calculateRevenueFromProducts(doc);
                }
                totalDuration += getDouble(doc, "durationSec");

                Document customer = doc.get("customer", Document.class);
                if (customer != null) {
                    Object score = customer.get("satisfiedScore");
                    if (score instanceof Number) {
                        totalSatisfied += ((Number) score).doubleValue();
                        satisfiedCount++;
                    }
                }
            }

            String categoryName = "";
            Document firstCat = group.get(0).get("category", Document.class);
            if (firstCat != null) {
                categoryName = firstCat.getString("medium") != null ? firstCat.getString("medium") : "";
            }

            result.add(new Document()
                    .append("categoryCode", entry.getKey())
                    .append("categoryName", categoryName)
                    .append("totalCount", total)
                    .append("convertedCount", converted)
                    .append("conversionRate", ratio(converted, total))
                    .append("avgDurationSec", avg(totalDuration, total))
                    .append("avgSatisfiedScore", avg(totalSatisfied, satisfiedCount))
                    .append("estimatedRevenue", totalRevenue));
        }

        result.sort((a, b) -> Integer.compare(b.getInteger("totalCount", 0), a.getInteger("totalCount", 0)));
        return result;
    }

    // ==================== 발신 결과 + 거절 사유 ====================

    private Document aggregateCallResults(List<Document> docs) {
        int converted = 0, rejected = 0;
        Map<String, Integer> reasonCount = new LinkedHashMap<>();

        for (Document doc : docs) {
            Document outbound = doc.get("outbound", Document.class);
            if (outbound == null) continue;

            String callResult = outbound.getString("callResult");
            if ("CONVERTED".equals(callResult)) {
                converted++;
            } else if ("REJECTED".equals(callResult)) {
                rejected++;
                String reason = outbound.getString("rejectReason");
                if (reason != null && !reason.isEmpty()) {
                    reasonCount.merge(reason, 1, Integer::sum);
                }
            }
        }

        List<Document> distribution = List.of(
                new Document("result", "CONVERTED").append("count", converted),
                new Document("result", "REJECTED").append("count", rejected));

        List<Document> rejectReasons = reasonCount.entrySet().stream()
                .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
                .map(e -> new Document("code", e.getKey()).append("count", e.getValue()))
                .collect(Collectors.toList());

        return new Document()
                .append("distribution", distribution)
                .append("rejectReasons", rejectReasons);
    }

    // ==================== 상담사별 실적 ====================

    private List<Document> aggregateAgents(List<Document> docs) {
        Map<Long, List<Document>> byAgent = docs.stream()
                .collect(Collectors.groupingBy(doc -> {
                    Document agent = doc.get("agent", Document.class);
                    if (agent == null) return 0L;
                    Object id = agent.get("_id");
                    return id instanceof Number ? ((Number) id).longValue() : 0L;
                }));

        List<Document> result = new ArrayList<>();
        for (Map.Entry<Long, List<Document>> entry : byAgent.entrySet()) {
            long agentId = entry.getKey();
            if (agentId == 0L) continue;

            List<Document> group = entry.getValue();
            int total = group.size();
            int converted = 0;
            double totalDuration = 0;

            for (Document doc : group) {
                Document outbound = doc.get("outbound", Document.class);
                if (outbound != null && "CONVERTED".equals(outbound.getString("callResult"))) {
                    converted++;
                }
                totalDuration += getDouble(doc, "durationSec");
            }

            String agentName = "";
            Document firstAgent = group.get(0).get("agent", Document.class);
            if (firstAgent != null) {
                agentName = firstAgent.getString("name") != null ? firstAgent.getString("name") : "";
            }

            result.add(new Document()
                    .append("agentId", agentId)
                    .append("agentName", agentName)
                    .append("totalCount", total)
                    .append("convertedCount", converted)
                    .append("conversionRate", ratio(converted, total))
                    .append("avgDurationSec", avg(totalDuration, total)));
        }

        result.sort((a, b) -> Integer.compare(b.getInteger("convertedCount", 0), a.getInteger("convertedCount", 0)));
        for (int i = 0; i < result.size(); i++) {
            result.get(i).append("rank", i + 1);
        }
        return result;
    }

    // ==================== 히트맵 (전환율 x 시간대 x 요일) ====================

    private List<Document> aggregateHeatmap(List<Document> docs) {
        Map<Integer, Map<Integer, int[]>> matrix = new LinkedHashMap<>();

        for (Document doc : docs) {
            LocalDateTime consultedAt = toKstDateTime(doc.get("consultedAt"));
            if (consultedAt == null) continue;

            int hour = consultedAt.getHour();
            int dow = consultedAt.getDayOfWeek().getValue(); // 1=월 ~ 7=일

            Document outbound = doc.get("outbound", Document.class);
            boolean isConverted = outbound != null && "CONVERTED".equals(outbound.getString("callResult"));

            matrix.computeIfAbsent(hour, k -> new HashMap<>())
                    .computeIfAbsent(dow, k -> new int[]{0, 0});
            int[] counts = matrix.get(hour).get(dow);
            counts[0]++;
            if (isConverted) counts[1]++;
        }

        List<Document> rows = new ArrayList<>();
        for (int hour = 9; hour <= 18; hour++) {
            Map<Integer, int[]> dayMap = matrix.getOrDefault(hour, Collections.emptyMap());
            List<Double> days = new ArrayList<>();
            for (int dow = 1; dow <= 5; dow++) { // 월~금만
                int[] counts = dayMap.getOrDefault(dow, new int[]{0, 0});
                days.add(ratio(counts[1], counts[0]));
            }
            rows.add(new Document("hour", hour).append("days", days));
        }
        return rows;
    }

    // ==================== 카테고리별 전환율 ====================

    private List<Document> aggregateConversionByCategory(List<Document> docs) {
        Map<String, int[]> stats = new LinkedHashMap<>();
        Map<String, String> names = new HashMap<>();

        for (Document doc : docs) {
            Document cat = doc.get("category", Document.class);
            String code = cat != null ? cat.getString("code") : "UNKNOWN";
            String name = cat != null && cat.getString("medium") != null ? cat.getString("medium") : "";

            stats.computeIfAbsent(code, k -> new int[]{0, 0});
            stats.get(code)[0]++;
            names.putIfAbsent(code, name);

            Document outbound = doc.get("outbound", Document.class);
            if (outbound != null && "CONVERTED".equals(outbound.getString("callResult"))) {
                stats.get(code)[1]++;
            }
        }

        return stats.entrySet().stream()
                .map(e -> {
                    int[] s = e.getValue();
                    return new Document()
                            .append("categoryCode", e.getKey())
                            .append("categoryName", names.get(e.getKey()))
                            .append("convertedCount", s[1])
                            .append("totalCount", s[0])
                            .append("conversionRate", ratio(s[1], s[0]));
                })
                .sorted((a, b) -> Double.compare(b.getDouble("conversionRate"), a.getDouble("conversionRate")))
                .collect(Collectors.toList());
    }

    // ==================== 최적 연락 시간 ====================

    private List<Document> aggregateOptimalTime(List<Document> docs) {
        Map<String, Map<Integer, int[]>> catHourStats = new LinkedHashMap<>();
        Map<String, Map<Integer, int[]>> catDowStats = new LinkedHashMap<>();
        Map<String, String> catNames = new HashMap<>();

        for (Document doc : docs) {
            Document cat = doc.get("category", Document.class);
            String code = cat != null ? cat.getString("code") : "UNKNOWN";
            String name = cat != null && cat.getString("medium") != null ? cat.getString("medium") : "";
            catNames.putIfAbsent(code, name);

            LocalDateTime consultedAt = toKstDateTime(doc.get("consultedAt"));
            if (consultedAt == null) continue;

            int hour = consultedAt.getHour();
            int dow = consultedAt.getDayOfWeek().getValue();

            Document outbound = doc.get("outbound", Document.class);
            boolean isConverted = outbound != null && "CONVERTED".equals(outbound.getString("callResult"));

            catHourStats.computeIfAbsent(code, k -> new HashMap<>())
                    .computeIfAbsent(hour, k -> new int[]{0, 0});
            catHourStats.get(code).get(hour)[0]++;
            if (isConverted) catHourStats.get(code).get(hour)[1]++;

            catDowStats.computeIfAbsent(code, k -> new HashMap<>())
                    .computeIfAbsent(dow, k -> new int[]{0, 0});
            catDowStats.get(code).get(dow)[0]++;
            if (isConverted) catDowStats.get(code).get(dow)[1]++;
        }

        List<Document> result = new ArrayList<>();
        for (String code : catHourStats.keySet()) {
            Map<Integer, int[]> hourMap = catHourStats.get(code);
            Map<Integer, int[]> dowMap = catDowStats.getOrDefault(code, Collections.emptyMap());

            int bestHour = -1;
            double bestHourRate = -1;
            for (Map.Entry<Integer, int[]> e : hourMap.entrySet()) {
                double rate = e.getValue()[0] > 0 ? (double) e.getValue()[1] / e.getValue()[0] : 0;
                if (rate > bestHourRate) {
                    bestHourRate = rate;
                    bestHour = e.getKey();
                }
            }

            String bestHourRange = bestHour >= 0
                    ? String.format("%02d:00 ~ %02d:00", bestHour, bestHour + 1)
                    : "N/A";

            List<String> bestDays = dowMap.entrySet().stream()
                    .filter(e -> e.getKey() >= 1 && e.getKey() <= 5) // 평일만
                    .sorted((a, b) -> {
                        double rateA = a.getValue()[0] > 0 ? (double) a.getValue()[1] / a.getValue()[0] : 0;
                        double rateB = b.getValue()[0] > 0 ? (double) b.getValue()[1] / b.getValue()[0] : 0;
                        return Double.compare(rateB, rateA);
                    })
                    .limit(2)
                    .map(e -> DAY_NAMES[e.getKey()])
                    .collect(Collectors.toList());

            result.add(new Document()
                    .append("categoryCode", code)
                    .append("categoryName", catNames.get(code))
                    .append("bestHourRange", bestHourRange)
                    .append("bestConversionRate", Math.round(bestHourRate * 1000.0) / 10.0)
                    .append("bestDays", bestDays));
        }
        return result;
    }

    // ==================== 매출 계산 ====================

    private Map<String, Integer> loadProductPriceMap() {
        Map<String, Integer> priceMap = new HashMap<>();
        jdbcTemplate.query("SELECT home_code, monthly_fee FROM product_home", (rs) -> {
            priceMap.put(rs.getString("home_code"), rs.getInt("monthly_fee"));
        });
        jdbcTemplate.query("SELECT mobile_code, monthly_fee FROM product_mobile", (rs) -> {
            priceMap.put(rs.getString("mobile_code"), rs.getInt("monthly_fee"));
        });
        jdbcTemplate.query("SELECT additional_code, monthly_fee FROM product_additional", (rs) -> {
            priceMap.put(rs.getString("additional_code"), rs.getInt("monthly_fee"));
        });
        log.info("[OutboundStats] 상품 가격 로딩 완료: {}개", priceMap.size());
        return priceMap;
    }

    private long calculateRevenueFromProducts(Document doc) {
        List<Document> resultProducts = doc.getList("resultProducts", Document.class);
        if (resultProducts == null || resultProducts.isEmpty()) return 0L;
        long revenue = 0;
        for (Document rp : resultProducts) {
            // SummarySyncItemWriter는 NEW/CANCEL만 생성
            if ("NEW".equals(rp.getString("changeType"))) {
                List<String> subscribed = rp.getList("subscribed", String.class);
                if (subscribed != null) {
                    for (String code : subscribed) {
                        revenue += productPriceMap.getOrDefault(code, 0);
                    }
                }
            }
        }
        return revenue;
    }

    // ==================== 유틸 ====================

    private LocalDateTime toKstDateTime(Object obj) {
        if (obj instanceof Date) {
            return ((Date) obj).toInstant().atZone(KST).toLocalDateTime();
        } else if (obj instanceof LocalDateTime) {
            // MongoDB에서 LocalDateTime으로 역직렬화된 경우 UTC 기준이므로 +9시간
            return ((LocalDateTime) obj).plusHours(9);
        }
        return null;
    }

    private double ratio(int numerator, int denominator) {
        return denominator > 0 ? Math.round((double) numerator / denominator * 1000.0) / 10.0 : 0;
    }

    private double avg(double total, int count) {
        return count > 0 ? Math.round(total / count * 10.0) / 10.0 : 0;
    }

    private double getDouble(Document doc, String field) {
        Object val = doc.get(field);
        return val instanceof Number ? ((Number) val).doubleValue() : 0.0;
    }
}

package com.uplus.batch.jobs.weekly_report.step.admin;


import com.uplus.batch.jobs.weekly_report.step.admin.model.WeeklyReportSnapshot;
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
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Component
@RequiredArgsConstructor
public class WeeklySubscriptionStatsTasklet implements Tasklet {

  private final MongoTemplate mongoTemplate;
  private final JdbcTemplate jdbcTemplate;

  @Override
  public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) {

    // mysql 에서 상품 정보 가져옴
    Map<String, String> productMasterMap = loadProductMaster();

    // 1. 집계 기간 설정 (데이터가 있는 2025년 1월로 테스트)
    LocalDateTime startAt = LocalDateTime.of(2025, 1, 13, 0, 0, 0);
    LocalDateTime endAt = LocalDateTime.of(2025, 1, 19, 23, 59, 59);

    /** [운영용] 배치가 실행되는 시점 기준 "지난주 월요일 ~ 일요일" 계산 **/
//    LocalDate now = LocalDate.now();
//    LocalDateTime startAt = now.minusWeeks(1)
//        .with(java.time.temporal.TemporalAdjusters.previousOrSame(java.time.DayOfWeek.MONDAY))
//        .atStartOfDay(); // 지난주 월요일 00:00:00
//
//    LocalDateTime endAt = startAt.plusDays(6)
//        .with(java.time.LocalTime.of(23, 59, 59)); // 지난주 일요일 23:59:59


    log.info("[WeeklyStats] 주별 리포트 집계 시작: {} ~ {}", startAt, endAt);

    // 2. MongoDB에서 상담 요약 데이터 조회 (컬렉션명: consultation_summary)
    Query query = new Query(Criteria.where("consultedAt").gte(startAt).lte(endAt));
    List<Document> summaries = mongoTemplate.find(query, Document.class, "consultation_summary");

    log.info("[WeeklyStats] 조회된 상담 건수: {}건", summaries.size());

    // 3. 집계를 위한 임시 Map (ID별 Stats 저장)
    Map<String, ProductStats> newSubMap = new HashMap<>();
    Map<String, ProductStats> cancelSubMap = new HashMap<>();

    // 연령대별 선호도: Map<"40대", Map<"5G 프리미어 에센셜", 카운트>>
    Map<String, Map<String, Long>> ageGroupPrefs = new HashMap<>();


    // 4. 데이터 가공
    for (Document doc : summaries) {

      // MongoDB에서 바로 ageGroup 추출
      Document customer = (Document) doc.get("customer");
      String ageGroup = (customer != null) ? customer.getString("ageGroup") : "기타";

      List<Document> resultProducts = doc.getList("resultProducts", Document.class);
      if (resultProducts == null) continue;

      for (Document rp : resultProducts) {

        // --- [신규 가입 및 연령대 통계] ---
        List<String> subIds = rp.getList("subscribed", String.class);
        if (subIds != null) {
          processAction(subIds, newSubMap, productMasterMap);

          // 연령대별 상품 카운트를 수행 (productId 기준)
          for (String id : subIds) {
            ageGroupPrefs
                .computeIfAbsent(ageGroup, k -> new HashMap<>())
                .merge(id, 1L, Long::sum);
          }
        }

        // --- [해지 처리] ---
        processAction(rp.getList("canceled", String.class), cancelSubMap, productMasterMap);

      }
    }

    // 5. 최종 리포트 스냅샷 객체 생성 및 데이터 매핑
    WeeklyReportSnapshot snapshot = new WeeklyReportSnapshot();
    snapshot.setStartAt(startAt);
    snapshot.setEndAt(endAt);
    WeeklyReportSnapshot.SubscriptionAnalysis analysis = new WeeklyReportSnapshot.SubscriptionAnalysis();

    // Map 데이터를 명세서 구조(List<ProductCount>)로 변환
    analysis.setNewSubscriptions(convertToProductCountList(newSubMap));
    analysis.setCanceledSubscriptions(convertToProductCountList(cancelSubMap));



    // --- [연령대별 데이터 변환 로직 추가] ---
    List<WeeklyReportSnapshot.ByAgeGroup> agePrefsList = ageGroupPrefs.entrySet().stream()
        .map(entry -> {
          List<WeeklyReportSnapshot.ProductCount> top3 = entry.getValue().entrySet().stream()
              .map(e -> new WeeklyReportSnapshot.ProductCount(e.getKey(), productMasterMap.getOrDefault(e.getKey(), e.getKey()), e.getValue()))
              .sorted(Comparator.comparingLong(WeeklyReportSnapshot.ProductCount::getCount).reversed())
              .limit(3)
              .collect(Collectors.toList());
          return new WeeklyReportSnapshot.ByAgeGroup(entry.getKey(), top3);
        })
        .sorted(Comparator.comparing(WeeklyReportSnapshot.ByAgeGroup::getAgeGroup)) // 연령대 순 정렬 (20대, 30대...)
        .collect(Collectors.toList());

    analysis.setByAgeGroup(agePrefsList);


    snapshot.setSubscriptionAnalysis(analysis);

    // 6. MongoDB 저장 및 로그 출력
    log.info("[WeeklyStats] 최종 집계 결과: 신규 {}건, 해지 {}건",
        analysis.getNewSubscriptions().size(),
        analysis.getCanceledSubscriptions().size());

    // 실제 저장
    Query upsertQuery = new Query(Criteria.where("startAt").is(startAt));

    Update update = new Update()
        .set("startAt", startAt)
        .set("endAt", endAt)
        .set("subscriptionAnalysis", analysis)
        .setOnInsert("createdAt", LocalDateTime.now());

    mongoTemplate.upsert(upsertQuery, update, "weekly_report_snapshot");
    log.info("[WeeklyStats] MongoDB 저장 완료 (ID: {})", snapshot.getId());

    return RepeatStatus.FINISHED;
  }

  private void processAction(List<String> ids, Map<String, ProductStats> statsMap, Map<String, String> master) {
    if (ids == null)
      return;
    for (String id : ids) {
      String realName = master.getOrDefault(id, id);
      updateStatsMap(statsMap, id, realName);
    }
  }

  /**
   * MySQL에서 상품 코드별 실제 이름을 가져와서 Map으로 만듦.
   */
  private Map<String, String> loadProductMaster() {
    Map<String, String> master = new HashMap<>();

    // Mobile (plan_name)
    jdbcTemplate.query("SELECT mobile_code, plan_name FROM product_mobile", (rs) -> {
      master.put(rs.getString("mobile_code"), rs.getString("plan_name"));
    });

    // Additional (additional_name)
    jdbcTemplate.query("SELECT additional_code, additional_name FROM product_additional", (rs) -> {
      master.put(rs.getString("additional_code"), rs.getString("additional_name"));
    });

    // Home (product_name)
    jdbcTemplate.query("SELECT home_code, product_name FROM product_home", (rs) -> {
      master.put(rs.getString("home_code"), rs.getString("product_name"));
    });

    return master;
  }


  /**
   * Stats Map 업데이트
   */
  private void updateStatsMap(Map<String, ProductStats> map, String id, String name) {
    ProductStats stats = map.getOrDefault(id, new ProductStats(id, name, 0L));
    stats.count++;
    map.put(id, stats);
  }

  /**
   * Map을 DTO 리스트로 변환
   */
  private List<WeeklyReportSnapshot.ProductCount> convertToProductCountList(Map<String, ProductStats> map) {
    return map.values().stream()
        .map(s -> new WeeklyReportSnapshot.ProductCount(s.productId, s.name, s.count))
        // 상위 6개만
        .sorted(Comparator.comparingLong(WeeklyReportSnapshot.ProductCount::getCount).reversed())
        .limit(6)
        .collect(Collectors.toList());
  }

  /**
   * 내부 집계용 클래스
   */
  private static class ProductStats {
    String productId;
    String name;
    long count;

    ProductStats(String productId, String name, long count) {
      this.productId = productId;
      this.name = name;
      this.count = count;
    }
  }

}
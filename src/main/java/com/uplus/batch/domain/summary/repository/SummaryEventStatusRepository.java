package com.uplus.batch.domain.summary.repository;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.uplus.batch.domain.summary.dto.*;
import com.uplus.batch.domain.summary.entity.ConsultationSummary;
import com.uplus.batch.domain.summary.entity.SummaryEventStatus;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

@Repository
@RequiredArgsConstructor
public class SummaryEventStatusRepository {

  private final JdbcTemplate jdbcTemplate;
  private final ObjectMapper objectMapper = new ObjectMapper();

  // ── ConsultationSummaryGenerator / RetryScheduler 에서 사용하는 엔티티 RowMapper ──

  private final RowMapper<SummaryEventStatus> entityRowMapper = (rs, rowNum) -> {
    Timestamp ca = rs.getTimestamp("created_at");
    Timestamp ua = rs.getTimestamp("updated_at");
    return SummaryEventStatus.reconstruct(
        rs.getLong("id"),
        rs.getLong("consult_id"),
        rs.getString("status"),
        rs.getInt("retry_count"),
        rs.getString("fail_reason"),
        ca != null ? ca.toLocalDateTime() : null,
        ua != null ? ua.toLocalDateTime() : null
    );
  };

  // ── ConsultationSummaryGenerator / RetryScheduler 용 엔티티 기반 CRUD ──────────

  /** REQUESTED 상태 중 오래된 순으로 최대 100건 조회 */
  public List<SummaryEventStatus> findTop100ByStatusOrderByCreatedAtAsc(String status) {
    return jdbcTemplate.query(
        "SELECT id, consult_id, status, retry_count, fail_reason, created_at, updated_at " +
        "FROM summary_event_status WHERE status = ? ORDER BY created_at ASC LIMIT 100",
        entityRowMapper,
        status
    );
  }

  /** FAILED 상태이고 retryCount < retryLimit 인 건 조회 (RetryScheduler 재배치용) */
  public List<SummaryEventStatus> findByStatusAndRetryCountLessThan(String status, int retryLimit) {
    return jdbcTemplate.query(
        "SELECT id, consult_id, status, retry_count, fail_reason, created_at, updated_at " +
        "FROM summary_event_status WHERE status = ? AND retry_count < ?",
        entityRowMapper,
        status, retryLimit
    );
  }

  /** 단건 상태 갱신 (ConsultationSummaryGenerator의 processSingleTask 에서 사용) */
  public void save(SummaryEventStatus entity) {
    jdbcTemplate.update(
        "UPDATE summary_event_status SET status = ?, retry_count = ?, fail_reason = ?, updated_at = NOW() WHERE id = ?",
        entity.getStatus(),
        entity.getRetryCount(),
        entity.getFailReason(),
        entity.getSummaryEventId()
    );
  }

  /** JPA saveAndFlush 호환 — JDBC에서는 UPDATE가 즉시 반영되므로 save()와 동일 */
  public void saveAndFlush(SummaryEventStatus entity) {
    save(entity);
  }

  /**
   * 다건 저장 — id가 null이면 INSERT, 존재하면 UPDATE.
   * triggerSummaryGeneration(신규 INSERT) / RetryScheduler(기존 UPDATE) 양쪽에서 사용.
   */
  public void saveAll(List<SummaryEventStatus> entities) {
    List<SummaryEventStatus> toInsert = entities.stream().filter(e -> e.getSummaryEventId() == null).toList();
    List<SummaryEventStatus> toUpdate = entities.stream().filter(e -> e.getSummaryEventId() != null).toList();

    if (!toInsert.isEmpty()) {
      jdbcTemplate.batchUpdate(
          "INSERT INTO summary_event_status (consult_id, status, retry_count, fail_reason, created_at) " +
          "VALUES (?, ?, ?, ?, NOW())",
          toInsert,
          toInsert.size(),
          (ps, e) -> {
            ps.setLong(1, e.getConsultId());
            ps.setString(2, e.getStatus());
            ps.setInt(3, e.getRetryCount());
            ps.setString(4, e.getFailReason());
          }
      );
    }

    if (!toUpdate.isEmpty()) {
      jdbcTemplate.batchUpdate(
          "UPDATE summary_event_status SET status = ?, retry_count = ?, fail_reason = ?, updated_at = NOW() WHERE id = ?",
          toUpdate,
          toUpdate.size(),
          (ps, e) -> {
            ps.setString(1, e.getStatus());
            ps.setInt(2, e.getRetryCount());
            ps.setString(3, e.getFailReason());
            ps.setLong(4, e.getSummaryEventId());
          }
      );
    }
  }

  // ── develop — ES 동기화 파이프라인용 메서드 ──────────────────────────────────────

  private final RowMapper<ConsultationResultSyncRow> consultationResultRowMapper = (rs, rowNum) ->
      new ConsultationResultSyncRow(
          rs.getLong("consult_id"),
          rs.getTimestamp("created_at").toLocalDateTime(),
          rs.getString("channel"),
          rs.getInt("duration_sec"),
          rs.getString("iam_issue"),
          rs.getString("iam_action"),
          rs.getString("iam_memo"),
          rs.getLong("emp_id"),
          rs.getString("employee_name"),
          rs.getLong("customer_id"),
          rs.getString("customer_name"),
          rs.getString("customer_phone"),
          rs.getString("age_group"),
          rs.getString("grade_code"),
          rs.getString("customer_type"),
          rs.getString("gender"),
          rs.getString("category_code"),
          rs.getString("large_category"),
          rs.getString("medium_category"),
          rs.getString("small_category")
      );

  public List<SummaryEventStatusRow> findRequestedBatchAfterIdBeforeCreatedAt(
      long lastEventId,
      int batchSize,
      LocalDateTime jobStartTime
  ) {

    String sql = """
      SELECT
          s.id,
          s.consult_id,
          s.retry_count
      FROM summary_event_status s
      JOIN result_event_status r
        ON r.consult_id = s.consult_id
       AND r.status = 'COMPLETED'
      WHERE s.status = ?
        AND s.id > ?
        AND s.created_at < ?
      ORDER BY s.id
      LIMIT ?
      """;

    return jdbcTemplate.query(
        sql,
        (rs, rowNum) -> new SummaryEventStatusRow(
            rs.getLong("id"),
            rs.getLong("consult_id"),
            rs.getInt("retry_count")
        ),
        com.uplus.batch.domain.summary.enums.SummaryEventStatusCode.REQUESTED.getValue(),
        lastEventId,
        Timestamp.valueOf(jobStartTime),
        batchSize
    );
  }

  public Map<Long, ConsultationResultSyncRow> findConsultationResultsByConsultIds(List<Long> consultIds) {

    if (consultIds == null || consultIds.isEmpty()) return Map.of();

    String inSql = consultIds.stream().map(id -> "?").collect(Collectors.joining(","));

    String sql = """
        SELECT
            r.consult_id,
            r.created_at,
            r.channel,
            r.duration_sec,
            r.iam_issue,
            r.iam_action,
            r.iam_memo,

            e.emp_id,
            e.name AS employee_name,

            c.customer_id,
            c.name AS customer_name,
            c.phone AS customer_phone,
            c.customer_type,
            c.grade_code,
            c.gender,

            CASE
                WHEN TIMESTAMPDIFF(YEAR, c.birth_date, CURDATE()) < 20 THEN '10대'
                WHEN TIMESTAMPDIFF(YEAR, c.birth_date, CURDATE()) < 30 THEN '20대'
                WHEN TIMESTAMPDIFF(YEAR, c.birth_date, CURDATE()) < 40 THEN '30대'
                WHEN TIMESTAMPDIFF(YEAR, c.birth_date, CURDATE()) < 50 THEN '40대'
                WHEN TIMESTAMPDIFF(YEAR, c.birth_date, CURDATE()) < 60 THEN '50대'
                ELSE '60대 이상'
            END AS age_group,

            r.category_code,
            cat.large_category,
            cat.medium_category,
            cat.small_category

        FROM consultation_results r
        LEFT JOIN employees e ON r.emp_id = e.emp_id
        LEFT JOIN customers c ON r.customer_id = c.customer_id
        LEFT JOIN consultation_category_policy cat ON r.category_code = cat.category_code
        WHERE r.consult_id IN (%s)
        """.formatted(inSql);

    List<ConsultationResultSyncRow> rows =
        jdbcTemplate.query(sql, consultationResultRowMapper, consultIds.toArray(new Object[0]));

    return rows.stream()
        .collect(Collectors.toMap(
            ConsultationResultSyncRow::consultId,
            r -> r,
            (a, b) -> a
        ));
  }

  public Map<Long, List<ConsultProductLogSyncRow>> findConsultProductLogs(List<Long> consultIds) {

    if (consultIds == null || consultIds.isEmpty()) return Map.of();

    String inSql = consultIds.stream().map(id -> "?").collect(Collectors.joining(","));

    String sql = """
      SELECT
        consult_id,
        contract_type,
        product_type,
        new_product_home,
        new_product_mobile,
        new_product_service,
        canceled_product_home,
        canceled_product_mobile,
        canceled_product_service
      FROM consult_product_logs
      WHERE consult_id IN (%s)
        AND deleted_at IS NULL
      """.formatted(inSql);

    List<ConsultProductLogSyncRow> rows =
        jdbcTemplate.query(sql, (rs, rowNum) ->
            new ConsultProductLogSyncRow(
                rs.getLong("consult_id"),
                rs.getString("contract_type"),
                rs.getString("product_type"),
                rs.getString("new_product_home"),
                rs.getString("new_product_mobile"),
                rs.getString("new_product_service"),
                rs.getString("canceled_product_home"),
                rs.getString("canceled_product_mobile"),
                rs.getString("canceled_product_service")
            ), consultIds.toArray(new Object[0]));

    return rows.stream()
        .collect(Collectors.groupingBy(ConsultProductLogSyncRow::consultId));
  }

  public Map<Long, List<ConsultationSummary.RiskFlag>> findRiskFlags(List<Long> consultIds) {

    if (consultIds == null || consultIds.isEmpty()) return Map.of();

    String inSql = consultIds.stream().map(i -> "?").collect(Collectors.joining(","));

    String sql = """
        SELECT
            r.consult_id,
            r.type_code,
            r.level_code
        FROM customer_risk_logs r
        JOIN risk_level_policy p ON r.level_code = p.level_code
        WHERE r.consult_id IN (%s)
          AND r.deleted_at IS NULL
        ORDER BY p.sort_order
        """.formatted(inSql);

    List<RiskFlagRow> rows =
        jdbcTemplate.query(sql, (rs, rowNum) -> new RiskFlagRow(
            rs.getLong("consult_id"),
            rs.getString("type_code"),
            rs.getString("level_code")
        ), consultIds.toArray(new Object[0]));

    return rows.stream()
        .collect(Collectors.groupingBy(
            RiskFlagRow::consultId,
            Collectors.mapping(
                r -> ConsultationSummary.RiskFlag.builder()
                    .riskType(r.riskType())
                    .riskLevel(r.riskLevel())
                    .build(),
                Collectors.toList()
            )
        ));
  }

  public Map<Long, RetentionAnalysisRow> findRetentionAnalysis(List<Long> consultIds) {

    if (consultIds == null || consultIds.isEmpty()) return Map.of();

    String inSql = consultIds.stream().map(i -> "?").collect(Collectors.joining(","));

    String sql = """
        SELECT
            consult_id,
            has_intent,
            defense_attempted,
            defense_success,
            defense_actions,
            complaint_reason,
            raw_summary
        FROM retention_analysis
        WHERE consult_id IN (%s)
        """.formatted(inSql);

    List<RetentionAnalysisRow> rows = jdbcTemplate.query(sql, (rs, rowNum) -> {

      List<String> actions = null;

      try {
        String json = rs.getString("defense_actions");
        if (json != null) {
          actions = objectMapper.readValue(json, List.class);
        }
      } catch (Exception ignored) {}

      return new RetentionAnalysisRow(
          rs.getLong("consult_id"),
          rs.getBoolean("has_intent"),
          rs.getBoolean("defense_attempted"),
          rs.getBoolean("defense_success"),
          actions,
          rs.getString("complaint_reason"),
          rs.getString("raw_summary")
      );

    }, consultIds.toArray(new Object[0]));

    return rows.stream()
        .collect(Collectors.toMap(
            RetentionAnalysisRow::consultId,
            r -> r,
            (a, b) -> a
        ));
  }

  public Map<Long, CustomerReviewRow> findCustomerReviews(List<Long> consultIds) {

    if (consultIds == null || consultIds.isEmpty()) return Map.of();

    String inSql = consultIds.stream().map(i -> "?").collect(Collectors.joining(","));

    String sql = """
        SELECT
            consult_id,
            score_1,
            score_2,
            score_3,
            score_4,
            score_5
        FROM client_review
        WHERE consult_id IN (%s)
        """.formatted(inSql);

    List<CustomerReviewRow> rows =
        jdbcTemplate.query(sql, (rs, rowNum) -> new CustomerReviewRow(
            rs.getLong("consult_id"),
            rs.getInt("score_1"),
            rs.getInt("score_2"),
            rs.getInt("score_3"),
            rs.getInt("score_4"),
            rs.getInt("score_5")
        ), consultIds.toArray(new Object[0]));

    return rows.stream()
        .collect(Collectors.toMap(
            CustomerReviewRow::consultId,
            r -> r,
            (a, b) -> a
        ));
  }

  public Map<Long, RawTextRow> findRawTexts(List<Long> consultIds) {

    if (consultIds == null || consultIds.isEmpty()) return Map.of();

    String inSql = consultIds.stream().map(i -> "?").collect(Collectors.joining(","));

    String sql = """
        SELECT consult_id, raw_text_json
        FROM consultation_raw_texts
        WHERE consult_id IN (%s)
        """.formatted(inSql);

    List<RawTextRow> rows =
        jdbcTemplate.query(sql, (rs, rowNum) -> new RawTextRow(
            rs.getLong("consult_id"),
            rs.getString("raw_text_json")
        ), consultIds.toArray(new Object[0]));

    return rows.stream()
        .collect(Collectors.toMap(
            RawTextRow::consultId,
            r -> r,
            (a, b) -> a
        ));
  }

  public void markCompletedBatch(List<Long> ids) {

    if (ids == null || ids.isEmpty()) return;

    String sql = """
        UPDATE summary_event_status
        SET status = 'completed',
            updated_at = NOW()
        WHERE id IN (%s)
        """.formatted(ids.stream().map(i -> "?").collect(Collectors.joining(",")));

    jdbcTemplate.update(sql, ids.toArray(new Object[0]));
  }

  public void markRetryBatch(List<Long> ids) {

    if (ids == null || ids.isEmpty()) return;

    String sql = """
        UPDATE summary_event_status
        SET retry_count = retry_count + 1,
            status = 'requested',
            updated_at = NOW()
        WHERE id IN (%s)
        """.formatted(ids.stream().map(i -> "?").collect(Collectors.joining(",")));

    jdbcTemplate.update(sql, ids.toArray(new Object[0]));
  }

  public void markFailedBatch(List<Long> ids) {

    if (ids == null || ids.isEmpty()) return;

    String sql = """
        UPDATE summary_event_status
        SET retry_count = retry_count + 1,
            status = 'failed',
            updated_at = NOW()
        WHERE id IN (%s)
        """.formatted(ids.stream().map(i -> "?").collect(Collectors.joining(",")));

    jdbcTemplate.update(sql, ids.toArray(new Object[0]));
  }
}

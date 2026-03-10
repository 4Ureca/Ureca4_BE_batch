package com.uplus.batch.domain.summary.repository;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.uplus.batch.domain.summary.dto.*;
import com.uplus.batch.domain.summary.enums.SummaryEventStatus;
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
        SummaryEventStatus.REQUESTED.getValue(),
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
            (a,b)->a
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

  public Map<Long, List<String>> findRiskFlags(List<Long> consultIds) {

    if (consultIds == null || consultIds.isEmpty()) return Map.of();

    String inSql = consultIds.stream().map(i -> "?").collect(Collectors.joining(","));

    String sql = """
        SELECT consult_id, type_code
        FROM customer_risk_logs
        WHERE consult_id IN (%s)
        """.formatted(inSql);

    List<RiskFlagRow> rows =
        jdbcTemplate.query(sql, (rs,rowNum)->new RiskFlagRow(
            rs.getLong("consult_id"),
            rs.getString("type_code")
        ), consultIds.toArray(new Object[0]));

    return rows.stream()
        .collect(Collectors.groupingBy(
            RiskFlagRow::consultId,
            Collectors.mapping(RiskFlagRow::typeCode, Collectors.toList())
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

    List<RetentionAnalysisRow> rows = jdbcTemplate.query(sql, (rs,rowNum)->{

      List<String> actions = null;

      try {
        String json = rs.getString("defense_actions");
        if(json!=null){
          actions = objectMapper.readValue(json,List.class);
        }
      } catch(Exception ignored){}

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
            r->r,
            (a,b)->a
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
        jdbcTemplate.query(sql,(rs,rowNum)->new CustomerReviewRow(
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
            r->r,
            (a,b)->a
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
        jdbcTemplate.query(sql,(rs,rowNum)->new RawTextRow(
            rs.getLong("consult_id"),
            rs.getString("raw_text_json")
        ), consultIds.toArray(new Object[0]));

    return rows.stream()
        .collect(Collectors.toMap(
            RawTextRow::consultId,
            r->r,
            (a,b)->a
        ));
  }

  public void markCompletedBatch(List<Long> ids) {

    if (ids==null || ids.isEmpty()) return;

    String sql = """
        UPDATE summary_event_status
        SET status = 'COMPLETED',
            updated_at = NOW()
        WHERE id IN (%s)
        """.formatted(ids.stream().map(i->"?").collect(Collectors.joining(",")));

    jdbcTemplate.update(sql, ids.toArray(new Object[0]));
  }

  public void markRetryBatch(List<Long> ids) {

    if (ids==null || ids.isEmpty()) return;

    String sql = """
        UPDATE summary_event_status
        SET retry_count = retry_count + 1,
            status = 'REQUESTED',
            updated_at = NOW()
        WHERE id IN (%s)
        """.formatted(ids.stream().map(i->"?").collect(Collectors.joining(",")));

    jdbcTemplate.update(sql, ids.toArray(new Object[0]));
  }

  public void markFailedBatch(List<Long> ids) {

    if (ids==null || ids.isEmpty()) return;

    String sql = """
        UPDATE summary_event_status
        SET retry_count = retry_count + 1,
            status = 'FAILED',
            updated_at = NOW()
        WHERE id IN (%s)
        """.formatted(ids.stream().map(i->"?").collect(Collectors.joining(",")));

    jdbcTemplate.update(sql, ids.toArray(new Object[0]));
  }
}
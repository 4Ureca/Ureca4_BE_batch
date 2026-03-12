package com.uplus.batch.jobs.summary_dummy.repository;

import com.uplus.batch.jobs.summary_dummy.dto.ConsultationResultRow;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
@RequiredArgsConstructor
public class ConsultationResultRepository {

  private final JdbcTemplate jdbcTemplate;

  public List<ConsultationResultRow> findByRange(long startId, long endId) {

    String sql = """
      SELECT cr.consult_id,
             cr.created_at,
             cr.emp_id,
             e.name AS agent_name,
             cr.customer_id,
             c.name AS customer_name,
             c.customer_type,
             c.phone,
             c.grade_code,
             c.birth_date,
             c.gender,
             cr.category_code,
             cp.large_category,
             cp.medium_category,
             cp.small_category,
             cr.channel,
             cr.duration_sec,
             cr.iam_issue,
             cr.iam_action,
             cr.iam_memo
      FROM consultation_results cr
      JOIN employees e ON cr.emp_id = e.emp_id
      JOIN customers c ON cr.customer_id = c.customer_id
      JOIN consultation_category_policy cp
           ON cr.category_code = cp.category_code
      WHERE cr.consult_id BETWEEN ? AND ?
      ORDER BY cr.consult_id
  """;

    return jdbcTemplate.query(sql,
        (rs, rowNum) -> new ConsultationResultRow(
            rs.getLong("consult_id"),
            rs.getTimestamp("created_at").toLocalDateTime(),

            rs.getLong("emp_id"),
            rs.getString("agent_name"),

            rs.getLong("customer_id"),
            rs.getString("customer_name"),
            rs.getString("customer_type"),
            rs.getString("phone"),
            rs.getString("grade_code"),
            rs.getDate("birth_date") != null
                ? rs.getDate("birth_date").toLocalDate() : null,
            rs.getString("gender"),

            rs.getString("category_code"),
            rs.getString("large_category"),
            rs.getString("medium_category"),
            rs.getString("small_category"),

            rs.getString("channel"),
            rs.getInt("duration_sec"),

            rs.getString("iam_issue"),
            rs.getString("iam_action"),
            rs.getString("iam_memo")
        ),
        startId,
        endId
    );
  }
}
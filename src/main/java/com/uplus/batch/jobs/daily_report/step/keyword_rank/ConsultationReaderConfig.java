package com.uplus.batch.jobs.daily_report.step.keyword_rank;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;

@Configuration
@RequiredArgsConstructor
public class ConsultationReaderConfig {

  private final DataSource dataSource;

  @Bean
  public JdbcCursorItemReader<Consultation> dailyConsultationReader() {
    JdbcCursorItemReader<Consultation> reader = new JdbcCursorItemReader<>();
    reader.setDataSource(dataSource);

    // 어제 날짜 상담만 읽기 (MySQL 기준)
    //   WHERE cr.consult_date >= (CURDATE() - INTERVAL 1 DAY)
//    AND cr.consult_date < CURDATE()
    reader.setSql("""
            SELECT
              cr.consult_id,
              rt.raw_text_json,
              cu.grade_code,
              cr.created_at
            FROM consultation_results cr
            JOIN consultation_raw_texts rt
              ON cr.consult_id = rt.consult_id
            JOIN customers cu
              ON cr.customer_id = cu.customer_id
             WHERE DATE(cr.created_at) = '2025-01-15'
            ORDER BY cr.consult_id
        """);

    reader.setRowMapper((rs, rowNum) -> {
      Consultation c = new Consultation();
      c.setId(rs.getLong("consult_id"));

      String rawJson = rs.getString("raw_text_json");

      try {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode array = mapper.readTree(rawJson);

        StringBuilder fullText = new StringBuilder();


        for (JsonNode node : array) {
          String speaker = node.path("speaker").asText("");
          if (!"고객".equals(speaker)) continue;

          String msg = node.path("message").asText("");
          if (msg.length() < 2) continue;

          fullText.append(msg).append(" ");
        }


        c.setContent(fullText.toString());

      } catch (Exception e) {
        throw new RuntimeException("JSON 파싱 실패 - consult_id: " + c.getId(), e);
      }

      c.setGradeCode(rs.getString("grade_code"));
      c.setConsultDate(rs.getTimestamp("created_at").toLocalDateTime());

      return c;
    });

    return reader;
  }
}
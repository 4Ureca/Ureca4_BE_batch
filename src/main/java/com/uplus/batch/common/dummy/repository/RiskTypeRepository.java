package com.uplus.batch.common.dummy.repository;

import com.uplus.batch.common.dummy.dto.RiskTypeDummyDto;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
@RequiredArgsConstructor
public class RiskTypeRepository {

  private final JdbcTemplate jdbcTemplate;

  public List<RiskTypeDummyDto> findActiveRiskTypes() {

    String sql = """
                SELECT type_code,
                       type_name
                FROM risk_type_policy
                WHERE is_active = 1
                ORDER BY type_code ASC
                """;

    return jdbcTemplate.query(sql,
        (rs, rowNum) -> new RiskTypeDummyDto(
            rs.getString("type_code"),
            rs.getString("type_name")
        )
    );
  }
}
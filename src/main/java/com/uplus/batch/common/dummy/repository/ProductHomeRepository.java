package com.uplus.batch.common.dummy.repository;

import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
@RequiredArgsConstructor
public class ProductHomeRepository {

  private final JdbcTemplate jdbcTemplate;

  public List<String> findAllCodes() {
    return jdbcTemplate.queryForList(
        "SELECT home_code FROM product_home",
        String.class
    );
  }
}
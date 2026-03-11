package com.uplus.batch.domain.summary.repository;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@Repository
@RequiredArgsConstructor
public class ProductRepository {

  private final NamedParameterJdbcTemplate jdbcTemplate;

  // 코드 → 상품명, 애플리케이션 시작 시 1회 로드
  private Map<String, String> productNameMap = Collections.emptyMap();

  @PostConstruct
  public void init() {
    Map<String, String> map = new HashMap<>();

    jdbcTemplate.query("""
        SELECT home_code, product_name FROM product_home
        UNION ALL
        SELECT mobile_code, plan_name  FROM product_mobile
        UNION ALL
        SELECT additional_code, additional_name FROM product_additional
        """,
        Collections.emptyMap(),
        rs -> {
          map.put(rs.getString(1), rs.getString(2));
        }
    );

    productNameMap = Collections.unmodifiableMap(map);
    log.info("Product map loaded: {} items", productNameMap.size());
  }

  /**
   * 상품 코드로 상품명 조회. O(1).
   * 없으면 null 반환.
   */
  public String findProductName(String productCode) {
    return productNameMap.get(productCode);
  }
}
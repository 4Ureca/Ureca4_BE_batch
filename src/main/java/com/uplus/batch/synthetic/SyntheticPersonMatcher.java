package com.uplus.batch.synthetic;

import jakarta.annotation.PostConstruct;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 합성 데이터 생성에 필요한 상담사·고객·카테고리 정보를 DB에서 샘플링한다.
 *
 * <p>애플리케이션 시작 시 한 번만 로드하여 메모리에 캐싱. 스케줄러 매 실행마다 랜덤 선택에 사용.
 */
@Slf4j
@Component
@Profile("!test")
@RequiredArgsConstructor
public class SyntheticPersonMatcher {

    private final JdbcTemplate jdbcTemplate;

    // ─── 캐시된 데이터 ───────────────────────────────────────
    @Getter private List<AgentInfo>    agents      = new ArrayList<>();
    @Getter private List<CustomerInfo> customers   = new ArrayList<>();

    /** 카테고리 코드 — 유형별 분류 (§2-1 분포 조건) */
    @Getter private List<String> chnCodes;   // 해지/재약정 40%
    @Getter private List<String> trbCodes;   // 장애/AS    20%
    @Getter private List<String> feeCodes;   // 요금/납부  20%
    @Getter private List<String> otherCodes; // 기타       20%

    // ─────────────────────────────────────────────────────────
    //  DTO
    // ─────────────────────────────────────────────────────────

    public record AgentInfo(int empId, String name) {}

    public record CustomerInfo(
            long customerId,
            String name,
            String customerType,
            String phone,
            String gradeCode,
            LocalDate birthDate,
            String gender          // 'M' | 'F' | null
    ) {}

    // ─────────────────────────────────────────────────────────
    //  초기화
    // ─────────────────────────────────────────────────────────

    @PostConstruct
    public void init() {
        loadAgents();
        loadCustomers();
        loadCategoryCodesByType();
        log.info("[SyntheticPersonMatcher] 초기화 완료 — 상담사: {}명, 고객: {}명, CHN: {}, TRB: {}, FEE: {}, 기타: {}",
                agents.size(), customers.size(),
                chnCodes.size(), trbCodes.size(), feeCodes.size(), otherCodes.size());
    }

    private void loadAgents() {
        agents = jdbcTemplate.query(
                "SELECT e.emp_id, e.name " +
                "FROM employees e " +
                "JOIN employee_details ed ON e.emp_id = ed.emp_id " +
                "WHERE e.is_active = 1 AND ed.job_role_id = 1",
                (rs, rowNum) -> new AgentInfo(
                        rs.getInt("emp_id"),
                        rs.getString("name")
                )
        );
    }

    private void loadCustomers() {
        customers = jdbcTemplate.query(
                "SELECT customer_id, name, customer_type, phone, grade_code, birth_date, gender " +
                "FROM customers",
                (rs, rowNum) -> new CustomerInfo(
                        rs.getLong("customer_id"),
                        rs.getString("name"),
                        rs.getString("customer_type"),
                        rs.getString("phone"),
                        rs.getString("grade_code"),
                        rs.getDate("birth_date") != null
                                ? rs.getDate("birth_date").toLocalDate() : null,
                        rs.getString("gender")
                )
        );
    }

    private void loadCategoryCodesByType() {
        List<String> all = jdbcTemplate.queryForList(
                "SELECT category_code FROM consultation_category_policy WHERE is_active = 1",
                String.class
        );

        chnCodes   = all.stream().filter(c -> c.contains("CHN")).collect(Collectors.toList());
        trbCodes   = all.stream().filter(c -> c.contains("TRB")).collect(Collectors.toList());
        feeCodes   = all.stream().filter(c -> c.contains("FEE")).collect(Collectors.toList());
        otherCodes = all.stream()
                .filter(c -> !c.contains("CHN") && !c.contains("TRB") && !c.contains("FEE"))
                .collect(Collectors.toList());

        // 폴백: 코드가 없는 유형은 전체에서 샘플링
        if (chnCodes.isEmpty())   chnCodes   = all;
        if (trbCodes.isEmpty())   trbCodes   = all;
        if (feeCodes.isEmpty())   feeCodes   = all;
        if (otherCodes.isEmpty()) otherCodes = all;
    }
}

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
    @Getter private List<String> chnCodes;   // 해지/재약정 40% (인바운드 전용)
    @Getter private List<String> trbCodes;   // 장애/AS    20% (인바운드 전용)
    @Getter private List<String> feeCodes;   // 요금/납부  20% (인바운드 전용)
    @Getter private List<String> otherCodes; // 기타       20% (인바운드 전용, OTB 제외)
    @Getter private List<String> otbChnCodes;    // 아웃바운드 CHN (M_OTB_01 재약정권유, M_OTB_05 윈백)
    @Getter private List<String> otbUpsellCodes; // 아웃바운드 UPSELL (M_OTB_02 업셀링)
    @Getter private List<String> otbArrearsCodes;// 아웃바운드 ARREARS (M_OTB_04 연체안내)
    @Getter private List<String> otbTrbCodes;    // 아웃바운드 TRB (M_OTB_03 사후관리, M_OTB_06 해피콜)

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
            String gender,         // 'M' | 'F' | null
            String mobilePlanName, // 현재 활성 모바일 상품명 (nullable)
            String homePlanName,   // 현재 활성 홈/인터넷 상품명 (nullable)
            String mobileCode,     // 현재 활성 모바일 상품 코드 (nullable)
            String homeCode        // 현재 활성 홈/인터넷 상품 코드 (nullable)
    ) {}

    // ─────────────────────────────────────────────────────────
    //  초기화
    // ─────────────────────────────────────────────────────────

    @PostConstruct
    public void init() {
        loadAgents();
        loadCustomers();
        loadCategoryCodesByType();
        log.info("[SyntheticPersonMatcher] 초기화 완료 — 상담사: {}명, 고객: {}명, " +
                "인바운드[CHN:{} TRB:{} FEE:{} 기타:{}] 아웃바운드[CHN:{} UPSELL:{} ARREARS:{} TRB:{}]",
                agents.size(), customers.size(),
                chnCodes.size(), trbCodes.size(), feeCodes.size(), otherCodes.size(),
                otbChnCodes.size(), otbUpsellCodes.size(), otbArrearsCodes.size(), otbTrbCodes.size());
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
                """
                SELECT
                    c.customer_id, c.name, c.customer_type, c.phone,
                    c.grade_code, c.birth_date, c.gender,
                    (SELECT csm.mobile_code
                     FROM customer_subscription_mobile csm
                     WHERE csm.customer_id = c.customer_id
                       AND csm.extinguish_at IS NULL
                     LIMIT 1) AS mobile_code,
                    (SELECT pm.plan_name
                     FROM customer_subscription_mobile csm
                     JOIN product_mobile pm ON csm.mobile_code = pm.mobile_code
                     WHERE csm.customer_id = c.customer_id
                       AND csm.extinguish_at IS NULL
                     LIMIT 1) AS mobile_plan_name,
                    (SELECT csh.home_code
                     FROM customer_subscription_home csh
                     WHERE csh.customer_id = c.customer_id
                       AND csh.extinguish_at IS NULL
                     LIMIT 1) AS home_code,
                    (SELECT ph.product_name
                     FROM customer_subscription_home csh
                     JOIN product_home ph ON csh.home_code = ph.home_code
                     WHERE csh.customer_id = c.customer_id
                       AND csh.extinguish_at IS NULL
                     LIMIT 1) AS home_plan_name
                FROM customers c
                """,
                (rs, rowNum) -> new CustomerInfo(
                        rs.getLong("customer_id"),
                        rs.getString("name"),
                        rs.getString("customer_type"),
                        rs.getString("phone"),
                        rs.getString("grade_code"),
                        rs.getDate("birth_date") != null
                                ? rs.getDate("birth_date").toLocalDate() : null,
                        rs.getString("gender"),
                        rs.getString("mobile_plan_name"),
                        rs.getString("home_plan_name"),
                        rs.getString("mobile_code"),
                        rs.getString("home_code")
                )
        );
    }

    private void loadCategoryCodesByType() {
        List<String> all = jdbcTemplate.queryForList(
                "SELECT category_code FROM consultation_category_policy WHERE is_active = 1",
                String.class
        );

        otbChnCodes    = all.stream().filter(c -> c.equals("M_OTB_01") || c.equals("M_OTB_05")).collect(Collectors.toList());
        otbUpsellCodes = all.stream().filter(c -> c.equals("M_OTB_02")).collect(Collectors.toList());
        otbArrearsCodes= all.stream().filter(c -> c.equals("M_OTB_04")).collect(Collectors.toList());
        otbTrbCodes    = all.stream().filter(c -> c.equals("M_OTB_03") || c.equals("M_OTB_06")).collect(Collectors.toList());
        chnCodes   = all.stream().filter(c -> c.contains("CHN")).collect(Collectors.toList());
        trbCodes   = all.stream().filter(c -> c.contains("TRB")).collect(Collectors.toList());
        feeCodes   = all.stream().filter(c -> c.contains("FEE")).collect(Collectors.toList());
        otherCodes = all.stream()
                .filter(c -> !c.startsWith("M_OTB") && !c.contains("CHN") && !c.contains("TRB") && !c.contains("FEE"))
                .collect(Collectors.toList());

        // 폴백: 코드가 없는 유형은 M_OTB 전체에서 샘플링
        List<String> allOtb = all.stream().filter(c -> c.startsWith("M_OTB")).collect(Collectors.toList());
        if (otbChnCodes.isEmpty())     otbChnCodes    = allOtb;
        if (otbUpsellCodes.isEmpty())  otbUpsellCodes = allOtb;
        if (otbArrearsCodes.isEmpty()) otbArrearsCodes= allOtb;
        if (otbTrbCodes.isEmpty())     otbTrbCodes    = allOtb;
        if (chnCodes.isEmpty())   chnCodes   = all;
        if (trbCodes.isEmpty())   trbCodes   = all;
        if (feeCodes.isEmpty())   feeCodes   = all;
        if (otherCodes.isEmpty()) otherCodes = all;
    }
}

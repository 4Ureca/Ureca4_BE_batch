package com.uplus.batch.jobs.consultation_dummy.processor;

import com.uplus.batch.domain.consultation.dto.ConsultationRow;
import jakarta.annotation.PostConstruct;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.stereotype.Component;

@Component
@StepScope
@RequiredArgsConstructor
public class ConsultationProcessor implements ItemProcessor<Integer, ConsultationRow> {

    private final JdbcTemplate jdbcTemplate;
    private final Random random = new Random();

    private List<Integer> empIds;
    private List<Long> customerIds;
    private List<String> categoryCodes;
    private Map<Long, String> customerGradeMap; // customer_id → grade_code

    // ── IAM 세트 (issue + action + memo 가 항상 같은 인덱스로 묶임) ───────────

    private record IamTemplate(String issue, String action, String memo) {}

    /** categoryCode 접두어(FEE, DEV, TRB, CHN, ADD, ETC) → IAM 세트 목록 */
    private static final Map<String, List<IamTemplate>> IAM_BY_CATEGORY = new HashMap<>();

    static {
        IAM_BY_CATEGORY.put("FEE", List.of(
                new IamTemplate(
                        "5G 요금제 변경 시 혜택 및 비용 문의",
                        "현행 요금제 기준 최적 5G 요금제 안내 후 변경 처리 완료",
                        "다음 달부터 신규 요금 적용 예정"),
                new IamTemplate(
                        "청구서 항목 이중 청구 이상 문의",
                        "시스템 조회 후 이중 청구 오류 확인 및 정정 처리",
                        "익월 청구서에서 차감 예정, 고객 동의 완료"),
                new IamTemplate(
                        "결합 할인 미적용 건 확인 요청",
                        "결합 할인 재적용 요청 시스템 등록 완료",
                        "1~3 영업일 내 처리 후 SMS 발송 예정"),
                new IamTemplate(
                        "장기 미납 요금 분할납부 방법 문의",
                        "미납 현황 안내 후 분할납부 일정 협의 및 등록",
                        "분할납부 계획 등록 완료, 자동이체 설정 권고")
        ));

        IAM_BY_CATEGORY.put("DEV", List.of(
                new IamTemplate(
                        "기기변경 공시지원금 적용 조건 문의",
                        "적용 가능 기기 목록 및 공시지원금 조건 안내 후 기기변경 예약 접수",
                        "기기변경 예약 접수 완료, 방문 일정 SMS 발송"),
                new IamTemplate(
                        "단말기 할부금 잔여액 및 소멸 일정 확인 요청",
                        "현행 할부 계획 조회 후 잔여 할부금 및 소멸 일정 상세 안내",
                        "안내 완료, 추가 문의 없음"),
                new IamTemplate(
                        "유심 분실로 인한 재발급 요청",
                        "본인 인증 후 유심 재발급 접수 처리",
                        "3~5 영업일 이내 등기 배송 예정"),
                new IamTemplate(
                        "타사에서 U+ 번호이동 조건 및 프로모션 문의",
                        "번호이동 절차 및 진행 중인 프로모션 안내",
                        "신청 의사 있음, 내방 예약 접수 완료")
        ));

        IAM_BY_CATEGORY.put("TRB", List.of(
                new IamTemplate(
                        "인터넷 속도 저하 불만 접수",
                        "장비 원격 점검 후 속도 저하 원인 파악, 현장 기사 출동 요청 등록",
                        "출동 일정 고객 협의 완료, 익일 방문 예정"),
                new IamTemplate(
                        "TV 화면 재생 오류 및 셋톱박스 이상 문의",
                        "셋톱박스 재부팅 가이드 안내 후 원격 점검 실시",
                        "원격 조치 완료, 정상 작동 확인"),
                new IamTemplate(
                        "모바일 데이터 연결 불가 문의",
                        "APN 설정 초기화 및 네트워크 재설정 가이드 안내",
                        "고객 직접 조치 후 정상 연결 확인"),
                new IamTemplate(
                        "신규 인터넷 설치 접수 요청",
                        "설치 가능 일정 조회 후 방문 예약 등록",
                        "설치 기사 배정 완료, 예정일 SMS 발송")
        ));

        IAM_BY_CATEGORY.put("CHN", List.of(
                new IamTemplate(
                        "약정 만료 후 해지 의사 표명",
                        "잔여 혜택 및 재약정 조건 비교 안내, 재약정 유도",
                        "고객 재약정 결정, 신규 약정 등록 완료"),
                new IamTemplate(
                        "중도 해지 시 위약금 금액 조회 요청",
                        "현행 약정 기준 위약금 계산 결과 상세 안내",
                        "위약금 확인 완료, 해지 여부는 고객 재검토 예정"),
                new IamTemplate(
                        "타사 이동을 위한 번호이동 해지 요청",
                        "번호이동 절차 안내 및 잔류 혜택 비교 제안 진행",
                        "이탈 방어 미성공, 번호이동 해지 처리 완료"),
                new IamTemplate(
                        "재약정 시 적용 혜택 및 기간 문의",
                        "재약정 할인 및 기기변경 혜택 전반 안내",
                        "재약정 결정, 약정 등록 및 혜택 적용 완료")
        ));

        IAM_BY_CATEGORY.put("ADD", List.of(
                new IamTemplate(
                        "해외 출국 전 로밍 서비스 가입 문의",
                        "이용 국가별 로밍 요금제 비교 안내 및 최적 상품 추천",
                        "출국 3일 전 자동 개통 예약 완료"),
                new IamTemplate(
                        "구독 중인 OTT 서비스 변경 요청",
                        "현행 부가서비스 현황 조회 후 요청 서비스로 변경 처리",
                        "다음 달 1일부터 변경 사항 적용 예정"),
                new IamTemplate(
                        "U+ 폰케어 보험 가입 조건 및 비용 문의",
                        "보험 상품 약관 및 월 납입금 상세 안내",
                        "보험 가입 완료, 익월 청구서에 반영 예정"),
                new IamTemplate(
                        "멤버십 포인트 적립 및 사용 방법 문의",
                        "멤버십 혜택 전반 및 포인트 적립·사용처 안내",
                        "안내 완료, 고객 만족 확인")
        ));

        IAM_BY_CATEGORY.put("ETC", List.of(
                new IamTemplate(
                        "인근 U+ 공식 매장 위치 문의",
                        "고객 주소 기반 인근 매장 3곳 안내",
                        "안내 완료"),
                new IamTemplate(
                        "서비스 품질 개선 제안 접수",
                        "고객 제안 사항 청취 및 내용 시스템 등록",
                        "제안 내용 접수 완료, 담당 부서 전달 예정"),
                new IamTemplate(
                        "서비스 이용 불만 접수 및 처리 요청",
                        "불만 내용 청취 후 담당 부서 이관 처리",
                        "처리 결과 3 영업일 이내 회신 예정"),
                new IamTemplate(
                        "이용 요금 영수증 발급 방법 문의",
                        "이용 내역 기준 영수증 발급 방법 및 이메일 발송 안내",
                        "영수증 이메일 발송 완료")
        ));
    }

    @PostConstruct
    public void init() {
        empIds = jdbcTemplate.queryForList(
                "SELECT e.emp_id FROM employees e " +
                "JOIN employee_details ed ON e.emp_id = ed.emp_id " +
                "WHERE e.is_active = 1 AND ed.job_role_id = 1", Integer.class);
        customerIds = jdbcTemplate.queryForList(
                "SELECT customer_id FROM customers", Long.class);
        categoryCodes = jdbcTemplate.queryForList(
                "SELECT category_code FROM consultation_category_policy WHERE is_active = 1",
                String.class);

        // 고객 등급 정보 로드 (VIP 우선 처리 메모 반영용)
        customerGradeMap = new HashMap<>();
        jdbcTemplate.query(
                "SELECT customer_id, grade_code FROM customers",
                (RowCallbackHandler) rs -> customerGradeMap.put(rs.getLong("customer_id"), rs.getString("grade_code"))
        );
    }

    @Override
    public ConsultationRow process(Integer item) {
        Integer empId = empIds.get(random.nextInt(empIds.size()));
        Long customerId = customerIds.get(random.nextInt(customerIds.size()));
        String categoryCode = categoryCodes.get(random.nextInt(categoryCodes.size()));

        // 80:20 비율 (팀원 코드 패턴 동일)
        String channel = random.nextInt(100) < 80 ? "CALL" : "CHATTING";
        int durationSec = 30 + random.nextInt(1771); // 30초 ~ 30분

        IamTemplate template = selectTemplate(categoryCode);
        String iamIssue = template.issue();
        String iamAction = template.action();
        // 고객 등급에 따라 메모에 우선처리 표기 추가
        String gradeNote = gradeNote(customerGradeMap.get(customerId));
        String iamMemo = template.memo() + gradeNote;

        return ConsultationRow.builder()
                .empId(empId)
                .customerId(customerId)
                .channel(channel)
                .categoryCode(categoryCode)
                .durationSec(durationSec)
                .iamIssue(iamIssue)
                .iamAction(iamAction)
                .iamMemo(iamMemo)
                .createdAt(randomCreatedAt())
                .build();
    }

    /**
     * categoryCode 접두어로 IAM 세트 목록을 조회해 랜덤 1건 반환.
     * 매핑 실패 시 ETC 세트로 폴백.
     * 예) "M_FEE_01" → split("_")[1] = "FEE"
     */
    private IamTemplate selectTemplate(String categoryCode) {
        String key = "ETC";
        String[] parts = categoryCode.split("_");
        if (parts.length >= 2) {
            key = parts[1]; // FEE, DEV, TRB, CHN, ADD, ETC
        }
        List<IamTemplate> templates = IAM_BY_CATEGORY.getOrDefault(key, IAM_BY_CATEGORY.get("ETC"));
        return templates.get(random.nextInt(templates.size()));
    }

    /** VVIP/VIP 고객은 메모에 우선처리 표기 추가 */
    private String gradeNote(String gradeCode) {
        if ("VVIP".equals(gradeCode)) return " [VVIP 최우선 처리 완료]";
        if ("VIP".equals(gradeCode))  return " [VIP 우선 처리 완료]";
        return "";
    }

    private LocalDateTime randomCreatedAt() {
        LocalDate start = LocalDate.of(2024, 1, 1);
        long days = ChronoUnit.DAYS.between(start, LocalDate.of(2026, 3, 1));
        long offset = (long) (days * random.nextDouble());
        int hour = 9 + random.nextInt(9); // 09:00 ~ 17:59
        int minute = random.nextInt(60);
        return start.plusDays(offset).atTime(hour, minute);
    }
}

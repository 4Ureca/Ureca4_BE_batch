package com.uplus.batch.domain.extraction.controller;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

@RestController
@RequestMapping("/dummy")
@RequiredArgsConstructor
@Slf4j
public class DummyEventController {

    private final JdbcTemplate jdbcTemplate;

    @PostMapping("/setup-manuals")
    @Transactional
    public String setupManuals() {
        log.info("[Dummy] 구두 고지 중심 지시형 매뉴얼(44개) 생성 시작...");
        
        try {
            jdbcTemplate.execute("SET FOREIGN_KEY_CHECKS = 0");
            jdbcTemplate.execute("TRUNCATE TABLE manuals");
            jdbcTemplate.execute("SET FOREIGN_KEY_CHECKS = 1");

            List<Object[]> batchArgs = new ArrayList<>();
            
            // --- 1. 부가서비스 (M_ADD) ---
            addManual(batchArgs, "M_ADD_01", "[상세] 스마트폰즈 가입 가이드", "원격 지원 편의성 강조 및 유료 가입 유도", "\"댁에서 원격으로 해결 가능한 서비스를 이용해보시겠어요?\"", "단말 OS 정보를 전산에서 확인하고 최신으로 업데이트하겠다는 내용 고지");
            addManual(batchArgs, "M_ADD_02", "[상세] 멤버십 등급별 혜택", "등급별 맞춤 혜택 안내 및 만족도 제고", "\"고객님 등급에서는 영화 무료 예매와 편의점 할인을 이용하실 수 있습니다.\"", "멤버십 관리 메뉴에서 [나만의 콕] 미설정 여부 확인 결과 안내");
            addManual(batchArgs, "M_ADD_03", "[상세] 해외 로밍 요금 설정", "출국 전 최적 요금제 제안 및 과금 방지", "\"로밍패스를 이용하시면 한국 번호 그대로 데이터를 나눠 쓰실 수 있습니다.\"", "방문 국가가 로밍패스 지원 대상인지 전산망 교차 검증 결과 고지");
            addManual(batchArgs, "M_ADD_04", "[절차] 부가서비스 해지/위약금", "불필요 비용 불만 해소 및 유지 시도", "\"지금 해지하시면 클라우드 저장 용량이 축소되는데 괜찮으실까요?\"", "해지 시 요금 변동액 전산 시뮬레이션 결과값 안내");
            addManual(batchArgs, "M_ADD_05", "[상세] OTT 구독 서비스 변경", "통신사 결합 할인 및 편의성 제공", "\"유튜브 프리미엄을 결합하시면 매달 3천원 더 저렴하게 이용 가능합니다.\"", "가입 후 전송될 [계정 연동 링크]를 통한 본인 인증 필요성 강조");
            addManual(batchArgs, "M_ADD_06", "[상세] U+ 폰케어 가입 및 보상", "파손 리스크 안내 및 보험 가입 유도", "\"최신폰 수리비는 기기값의 상당 부분이므로 안심하고 쓰시는 것이 좋습니다.\"", "전산상 개통일자 확인을 통한 보험 가입 가능 여부 확답");

            // --- 2. 해지/재약정 (M_CHN) ---
            addManual(batchArgs, "M_CHN_01", "[전략] 약정 만료 및 재약정", "재약정 혜택 제안을 통한 타사 이탈 방어", "\"재약정 시 상품권 증정 및 홈 장비 무상 교체 혜택을 드립니다.\"", "평균 납부액 기반 산출된 최대 사은품 가이드라인 정보 제공");
            addManual(batchArgs, "M_CHN_02", "[안내] 해지 위약금 산정 고지", "정확한 위약금 고지를 통한 민원 예방", "\"지금 해지 시 반환금이 00원 발생함을 확인해 드립니다.\"", "인터넷/모바일/TV별 위약금 전산 합산 총액 구두 전달");
            addManual(batchArgs, "M_CHN_03", "[전략] 번호이동 시 결합 주의", "결합 붕괴로 인한 가족 요금 인상 경고", "\"고객님 해지 시 가족 전체 요금이 크게 인상됩니다.\"", "결합 관리 메뉴상의 현재 구성원 리스트 및 혜택 소멸 정보 안내");
            addManual(batchArgs, "M_CHN_04", "[절차] 인터넷 해지 및 장비 회수", "원만한 해지 절차 안내 및 장비 분쟁 방지", "\"셋톱박스는 기사님이 방문하여 수거할 예정이니 보관 부탁드립니다.\"", "장비 수거 방문 희망일자 전산 예약 내용 최종 확인");
            addManual(batchArgs, "M_CHN_05", "[전략] 이탈 방지 업그레이드", "상위 상품 체험 제안 및 불만 해소", "\"3개월간 기가 인터넷으로 무료 업그레이드해 드릴 테니 써보세요.\"", "프로모션 코드 적용에 따른 요금 변동 및 자동 복구 시점 안내");
            addManual(batchArgs, "M_CHN_06", "[절차] 재약정 혜택 지급 절차", "재약정 동의 확정 및 지급 일정 안내", "\"선택하신 상품권은 7일 이내에 문자로 발송될 예정입니다.\"", "사은품 발송 시스템 등록을 위한 수령 번호 및 종류 최종 확인");
            addManual(batchArgs, "M_CHN_07", "[상세] 약정 기간 및 시점 확인", "무약정 고객 대상 신규 약정 할인 유도", "\"다시 약정만 거셔도 요금이 25%% 줄어듭니다.\"", "전산상 [약정종료] 대상자임을 확인하여 맞춤 혜택 제안");

            // --- 3. 기기변경 (M_DEV) ---
            addManual(batchArgs, "M_DEV_01", "[상세] 기기변경 혜택 및 절차", "신모델 강점 강조 및 타사 이탈 차단", "\"이번 신모델은 고객님 사용 패턴에 최적화되어 있습니다.\"", "실시간 재고 관리 시스템을 통한 선점 가능 색상/용량 정보 안내");
            addManual(batchArgs, "M_DEV_02", "[상세] 공시지원금 실시간 조회", "할인 방식(지원금 vs 약정) 비교 제안", "\"기기값 할인보다 매달 요금 할인을 받는 것이 더 이득입니다.\"", "당일 기준 최신 공시지원금 단가 산정 결과 고지");
            addManual(batchArgs, "M_DEV_03", "[절차] 할부금 승계 및 일시납", "잔여 할부 부담 최소화 방식 제안", "\"남은 기기값은 새 할부와 합치거나 카드로 완납 가능합니다.\"", "할부 원금 잔액 전산 조회 결과 및 수납 방식 선택 안내");
            addManual(batchArgs, "M_DEV_04", "[전략] 타사 이동 및 U+ 신규", "당사 결합/멤버십 강점 강조 및 가입 유도", "\"이동하시면 가족 결합을 통해 요금을 반값 수준으로 낮출 수 있습니다.\"", "가족 결합 구성 시 시뮬레이션된 예상 할인 총액 정보 제공");
            addManual(batchArgs, "M_DEV_05", "[절차] 단말 A/S 및 대여폰", "고장 고객 케어 및 서비스 만족도 유지", "\"수리 동안 불편 없으시도록 대여폰을 예약해 드리겠습니다.\"", "대여폰 재고 매장 전산 조회 및 방문 예약 정보 안내");
            addManual(batchArgs, "M_DEV_06", "[상세] 유심 및 eSIM 발급", "유심 오류 해결 및 eSIM 가입 안내", "\"바로 개통 가능한 eSIM으로 다운로드해보시는 건 어떠세요?\"", "단말의 eSIM 지원 여부 전산 조회 결과 안내");

            // --- 4. 기타 (M_ETC) ---
            addManual(batchArgs, "M_ETC_01", "[안내] 대리점 위치 및 방문 예약", "대면 업무 필요 고객 매장 연결 지원", "\"직영점으로 지금 바로 방문 예약을 잡아드릴 테니 대기 없이 업무 보세요.\"", "매장별 주차 가능 여부 및 당일 마감 시간 전산 확인 내용 고지");
            addManual(batchArgs, "M_ETC_02", "[절차] 영수증 및 세금계산서 발급", "지출 증빙 지원 및 비대면 발급 안내", "\"요청하신 영수증을 이메일로 발송해 드렸습니다.\"", "이메일/팩스 전산 발송 성공 여부 최종 확인 결과 안내");
            addManual(batchArgs, "M_ETC_03", "[절차] 서비스 제안/칭찬 접수", "고객 긍정 피드백 기록 및 사기 제고", "\"고객님 말씀이 해당 직원에게 큰 힘이 됩니다. 감사합니다.\"", "VOC 시스템 [칭찬] 카테고리 정식 등록 예정임 고지");
            addManual(batchArgs, "M_ETC_04", "[절차] 민원 및 불편 사항 접수", "강성 고객 불만 경청 및 확산 방지", "\"책임지고 담당 팀에 전달하여 내일까지 방안을 드리겠습니다.\"", "민원 경위 및 요구 사항의 전산 메모 기록 사실 안내");

            // --- 5. 요금/납부 (M_FEE) ---
            addManual(batchArgs, "M_FEE_01", "[상세] 5G 요금제 구성/혜택", "데이터 수요 맞춤 요금제 제안", "\"무제한 요금제는 테더링 용량도 넉넉하여 유용합니다.\"", "사용량 패턴 분석 데이터에 기반한 최적 요금제 추천 근거 고지");
            addManual(batchArgs, "M_FEE_02", "[상세] LTE 요금제/안심 옵션", "과금 걱정 고객 안심 옵션 제안", "\"추가 요금 걱정 없는 안심 옵션을 설정해 드릴까요?\"", "청소년 요금제 등 데이터 자동 차단 기능 활성화 여부 확인 안내");
            addManual(batchArgs, "M_FEE_03", "[상세] 데이터 쉐어링 가입", "다회선 사용 고객 지원 및 혜택 강화", "\"휴대폰 데이터를 태블릿에서도 함께 쓰실 수 있게 도와드리겠습니다.\"", "요금제별 무료 제공 대수 초과 여부 확인 결과 안내");
            addManual(batchArgs, "M_FEE_04", "[안내] 미납 확인 및 납부 독려", "정지 전 신속 납부 유도 및 연체 관리", "\"지금 바로 결제하셔서 서비스 정지를 방지해 드릴까요?\"", "미납 내역 중 소액결제 및 기본료 상세 구분 정보 제공");
            addManual(batchArgs, "M_FEE_05", "[절차] 미납 분할 납부 신청", "일시 완납 곤란 고객 이탈 방지", "\"이번 달에 일부를 내고 나머지는 나눠 내실 수 있습니다.\"", "전산상 분납 약정 등록 완료 및 차회 납부 약속일 복창");
            addManual(batchArgs, "M_FEE_06", "[절차] 자동이체 결제 수단 변경", "수납 안정성 확보 및 연체 사전 차단", "\"자동이체로 변경해두시면 요금 할인 혜택도 받으실 수 있습니다.\"", "출금 희망일 등록 정보 및 당월 청구분 반영 시점 안내");
            addManual(batchArgs, "M_FEE_07", "[상세] 청구서 항목 상세 설명", "요금 의문점 해소 및 신뢰도 제고", "\"요금이 인상된 원인은 소액결제 항목이 포함되었기 때문입니다.\"", "전월 대비 증감액 비교 분석 결과 특정 과금 항목 안내");
            addManual(batchArgs, "M_FEE_08", "[절차] 복지 감면 신청 가이드", "사회적 배려 대상자 혜택 적용", "\"복지 할인 자격이 확인되어 전산으로 즉시 신청해 드리겠습니다.\"", "행안부 시스템 실시간 조회 결과 및 감면 자격 승인 여부 안내");
            addManual(batchArgs, "M_FEE_09", "[상세] 유무선 가족 결합 할인", "가족 단위 락인(Lock-in) 효과 강화", "\"인터넷과 휴대폰을 묶으시면 매달 큰 폭의 할인을 받으십니다.\"", "가족 관계 전산 조회 결과 및 인원별 상세 할인액 고지");
            addManual(batchArgs, "M_FEE_10", "[상세] 장기 고객 할인 및 혜택", "장기 이용 고객 가치 인정 및 충성도 강화", "\"5년 이상 이용해주셔서 감사 의미로 데이터 쿠폰을 발행해 드립니다.\"", "장기 고객 전용 쿠폰함(Gift) 발행 가능 수량 확인 결과 안내");

            // --- 6. 장애/AS (M_TRB) ---
            addManual(batchArgs, "M_TRB_01", "[기술] 인터넷 속도 저하 점검", "원격 점검을 통한 단순 장비 오류 해결", "\"인터넷 신호를 다시 보내드릴 테니 전원을 껐다 켜보시겠어요?\"", "국사에서 댁내 장비까지의 신호 감쇄율 원격 진단 결과 고지");
            addManual(batchArgs, "M_TRB_02", "[기술] 인터넷 접속 불가 조치", "서비스 중단 상황 공감 및 최우선 방문 예약", "\"가장 빠른 시간으로 기사님 배정을 도와드리겠습니다.\"", "실시간 관제 시스템상 지역 광역 장애 여부 확인 결과 안내");
            addManual(batchArgs, "M_TRB_03", "[기술] Wi-Fi 신호 불안정 해소", "음영 지역 해소 및 최적 무선 환경 제안", "\"메쉬 공유기를 설치하시면 집안 어디서든 해결 가능합니다.\"", "원격 공유기 관리 페이지 접속을 통한 채널 변경 조치 사실 안내");
            addManual(batchArgs, "M_TRB_04", "[기술] IPTV 화면 깨짐/싱크 오류", "방송 품질 이슈 해결 및 신속 복구", "\"셋톱박스 리셋이나 HDMI 재연결로 해결 가능합니다.\"", "셋톱박스 펌웨어 버전 확인 및 원격 업데이트 수행 정보 고지");
            addManual(batchArgs, "M_TRB_05", "[기술] 리모컨 작동 불량 조치", "단순 페어링 문제 해결 및 자가 조치 유도", "\"리모컨 페어링 방법을 문자로 보내드릴게요.\"", "상담 매뉴얼 [페어링 조치법] 링크 SMS 발송 사실 고지");
            addManual(batchArgs, "M_TRB_06", "[기술] 셋톱박스 에러 코드별 대응", "화면 코드 분석 및 정확한 원인 파악", "\"화면의 에러 코드는 인증 오류입니다. 신호를 다시 쏴드릴게요.\"", "에러 코드별 조치 가이드 조회 결과 및 프로세스 단계 안내");
            addManual(batchArgs, "M_TRB_07", "[기술] 모바일 통화 품질/중계기", "음영 지역 접수 및 이탈 방어", "\"방문 확인 후 중계기를 무상으로 설치해 드리겠습니다.\"", "장애 발생 지점 기지국 수신 감도 로그 분석 결과 안내");
            addManual(batchArgs, "M_TRB_08", "[기술] 데이터 접속 불가/QoS 점검", "데이터 오류 해결 및 속도 제한 상태 설명", "\"데이터 기본량 소진으로 현재 속도가 제한된 상태입니다.\"", "실시간 데이터 사용량 및 요금제별 제한 속도 정보 제공");
            addManual(batchArgs, "M_TRB_09", "[기술] 스마트홈 연동 실패 대응", "IoT 장비 연결 안정성 및 이용 지원", "\"기기 초기화 후 와이파이 대역을 다시 선택해 보시겠어요?\"", "스마트홈 허브 등록 상태 및 계정 연동 로그 확인 결과 고지");
            addManual(batchArgs, "M_TRB_10", "[절차] 인터넷 신규 설치 신청", "설치 가능 여부 신속 조회 및 가입 확정", "\"고객님 댁은 기가 인터넷 설치가 가능합니다.\"", "주소지 전산 조회를 통한 [광랜/기가/대칭형] 제공 가능 품질 안내");
            addManual(batchArgs, "M_TRB_11", "[절차] 주거지 이전 설치 비용", "이사 시 서비스 공백 최소화 및 유지 도모", "\"이사 날짜에 맞춰 기사님 예약을 도와드리겠습니다.\"", "이전 주소지 설치 가능 상품 재확인 및 출동비 발생 내역 고지");

            // --- 7. 아웃바운드 (M_OTB) ---
            addManual(batchArgs, "M_OTB_01", "[전략] 재약정 권유 및 혜택 제안", "약정 만료 고객 대상 락인 및 사은품 혜택 안내", "\"고객님, 약정 만료 시점에 맞춰 풍성한 재약정 사은품과 요금 할인 혜택을 준비했습니다.\"", "전산상 약정 만료일자 확인 및 가용 상품권/할인 총액 가이드라인 정보 제공");
            addManual(batchArgs, "M_OTB_02", "[전략] 요금제 업셀링 및 맞춤 추천", "데이터 부족 고객 대상 상위 요금제 가치 제안 및 만족도 향상", "\"데이터 사용량이 많으신 고객님께 더 유리한 무제한 요금제를 추천해 드리고자 연락드렸습니다.\"", "최근 3개월 평균 사용량 분석 데이터 기반 요금제 전환 시 체감 혜택 안내");
            addManual(batchArgs, "M_OTB_03", "[관리] 해지 철회 사후 만족도 케어", "해지 철회 고객 대상 서비스 안정성 확인 및 신뢰 회복", "\"해지 철회 후 서비스 이용에 불편함은 없으신지 확인하고 감사의 마음을 전하고자 연락드렸습니다.\"", "이전 해지 신청 사유에 대한 조치 완료 여부 재확인 및 추가 불만 사항 선제적 청취");
            addManual(batchArgs, "M_OTB_04", "[안내] 연체 납부 독려 및 정지 예방", "미납 고객 대상 서비스 중단 방지 및 유연한 납부 방식 제안", "\"서비스 정지를 방지하기 위해 미납된 요금의 빠른 납부와 분할 납부 방법을 안내해 드리고자 합니다.\"", "현재 총 미납액 상세 내역 고지 및 가상계좌/즉시 수납 채널 정보 제공");
            addManual(batchArgs, "M_OTB_05", "[전략] 윈백(Win-back) 복귀 유도", "타사 이탈 고객 대상 재가입 특별 혜택 강조 및 복귀 제안", "\"U+를 다시 이용해 주시면 신규 가입 이상의 특별한 복귀 혜택과 결합 할인을 적용해 드리겠습니다.\"", "타사 이용 위약금 보전 혜택 및 유무선 결합 재구성 시 예상 할인 총액 안내");
            addManual(batchArgs, "M_OTB_06", "[안내] 개통 해피콜 및 초기 품질 확인", "설치/개통 직후 초기 품질 확인 및 추가 니즈 발굴", "\"기사님 방문 설치는 만족스러우셨나요? 이용 초기 궁금하신 점이나 불편한 점은 없으신지요?\"", "설치 완료 전산 리포트 기반 품질 측정값 안내 및 초기 설정 가이드 SMS 발송 사실 고지");
            String sql = "INSERT INTO manuals (category_code, title, content, is_active, emp_id, created_at, updated_at) " +
                         "VALUES (?, ?, ?, 1, 1, NOW(), NOW())";

            jdbcTemplate.batchUpdate(sql, batchArgs);

            log.info("[Dummy] 지시형 매뉴얼 {}건 생성 완료 (구두 안내 검증용)", batchArgs.size());
            return "Successfully created " + batchArgs.size() + " manuals focused on verbal verification.";
        } catch (Exception e) {
            log.error("[Dummy] 오류: {}", e.getMessage());
            return "Error: " + e.getMessage();
        }
    }

    /**
     * 상담 원문(Transcript) 분석을 위해 "구두 안내"에 초점을 맞춘 가이드 생성
     */
    private void addManual(List<Object[]> args, String code, String title, String goal, String script, String infoToInform) {
        String formattedContent = String.format("""
            # %s 채점 및 상담 가이드
            
            ## 📋 1. 상담 목표 (AI 요약 포인트)
            - %s
            
            ## ✅ 2. 필수 구두 안내 지침 (Must-Inform)
            - **표준 멘트**: %s (유사한 의미의 발화 포함)
            - **전산 관련 고지**: 상담원은 반드시 고객에게 [%s]에 대해 언급하고 안내해야 함.
            - 상담 시작 시 소속과 이름을 밝히는 인사가 포함되어야 함.
            - 상담 종료 전 궁금한 사항이 더 있는지 확인하는 멘트가 있어야 함.
            
            ## 🚫 3. 금지 및 주의사항 (Warning)
            - 고객의 말을 중간에 끊거나 감정적으로 대응하지 않음.
            - 확인되지 않은 정보를 확정적으로 안내하지 않음.
            - 전문 용어보다는 고객이 이해하기 쉬운 단어로 전산 처리 내용을 설명해야 함.
            """, title, goal, script, infoToInform);

        args.add(new Object[]{code, title, formattedContent});
    }

    /**
     * 2. 분석 관련 모든 데이터 초기화 (분석 결과 + 상태 테이블)
     */
    @PostMapping("/clear-all")
    @Transactional
    public String clearAnalysisData() {
        log.info("[Dummy] 분석 및 채점 테이블 초기화 시작 (retention_analysis, consultation_evaluations)");
        try {
            jdbcTemplate.execute("SET FOREIGN_KEY_CHECKS = 0");

            jdbcTemplate.execute("TRUNCATE TABLE retention_analysis");      // 요약 및 해지방어 추출 결과
            jdbcTemplate.execute("TRUNCATE TABLE consultation_evaluations"); // AI 점수 및 채점 결과
            
            // 상태 관리 테이블들
            jdbcTemplate.execute("TRUNCATE TABLE result_event_status");
            jdbcTemplate.execute("TRUNCATE TABLE excellent_event_status");

            jdbcTemplate.execute("SET FOREIGN_KEY_CHECKS = 1");

            log.info("[Dummy] 모든 관련 테이블 초기화 완료");
            return "Successfully cleared: retention_analysis, consultation_evaluations, and status tables.";
        } catch (Exception e) {
            log.error("[Dummy] 초기화 중 오류 발생: {}", e.getMessage());
            return "Error: " + e.getMessage();
        }
    }

    /**
     * 3. 통합 분석 대기열 생성
     */
    @PostMapping("/event-status")
    @Transactional
    public String generateEventStatus(@RequestParam Long startId, @RequestParam Long endId) {
        log.info("[Dummy] 대기열 생성: consultId {} ~ {}", startId, endId);
        String sqlR = "INSERT INTO result_event_status (consult_id, category_code, status, retry_count, created_at, updated_at) " +
                      "SELECT consult_id, category_code, 'REQUESTED', 0, NOW(), NOW() FROM consultation_results WHERE consult_id BETWEEN ? AND ?";
        String sqlE = "INSERT INTO excellent_event_status (consult_id, status, retry_count, created_at, updated_at) " +
                      "SELECT consult_id, 'REQUESTED', 0, NOW(), NOW() FROM consultation_results WHERE consult_id BETWEEN ? AND ?";
        try {
            int r = jdbcTemplate.update(sqlR, startId, endId);
            int e = jdbcTemplate.update(sqlE, startId, endId);
            return String.format("Created %d pairs (REQUESTED)", r);
        } catch (Exception e) { return "Error: " + e.getMessage(); }
    }
}
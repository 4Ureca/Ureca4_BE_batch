package com.uplus.batch.synthetic;

import com.uplus.batch.common.dummy.dto.CacheDummy;
import com.uplus.batch.domain.summary.entity.SummaryEventStatus;
import com.uplus.batch.domain.summary.repository.SummaryEventStatusRepository;
import com.uplus.batch.jobs.raw_text_dummy.generator.RawTextGenerator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.sql.PreparedStatement;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 합성 상담 데이터 배치 생성 오케스트레이터.
 *
 * <p>호출 방식 (Spring @Transactional 우회 방지):
 * 각 Step 메서드를 스케줄러에서 순차적으로 외부 호출한다.
 * — self-invocation(this.method())으로 호출하면 @Transactional이 무시되므로
 *   {@link SyntheticDataGeneratorScheduler}가 아래 순서로 직접 호출:
 * <ol>
 *   <li>{@link #executeStep1(int)} — @Transactional, 반환 시 커밋 완료</li>
 *   <li>{@link #triggerAiExtraction(List, List)} — Step1 커밋 후 별도 실행</li>
 *   <li>{@link #triggerSummaryGeneration(List)} — Step1 커밋 후 별도 실행</li>
 * </ol>
 *
 * <p>§2 분포 조건:
 * <ul>
 *   <li>카테고리: CHN 40% / TRB 20% / FEE 20% / 기타(ADD·ETC) 20%</li>
 *   <li>채널: CALL 70% / CHATTING 30%</li>
 *   <li>durationSec: CALL 120~600s / CHATTING 60~300s</li>
 *   <li>합성 데이터 식별: iam_memo에 [SYNTHETIC] 태그 삽입</li>
 * </ul>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class SyntheticConsultationFactory {

    private final SyntheticPersonMatcher personMatcher;
    private final RawTextGenerator rawTextGenerator;
    private final JdbcTemplate jdbcTemplate;
    private final SummaryEventStatusRepository summaryEventStatusRepo;
    private final CacheDummy cacheDummy;

    // ── 위험 유형·등급 (risk_type_policy / risk_level_policy 참조) ─────────
    private static final List<String> RISK_TYPES  = List.of("CHURN","FRAUD","PHISHING","ABUSE","COMP");
    // LOW 가중치 높게 (실제 분포 반영)
    private static final List<String> RISK_LEVELS = List.of("LOW","LOW","LOW","MEDIUM","MEDIUM","HIGH","CRITICAL");

    // ─── IAM 템플릿 (카테고리 접두사 → [iam_issue, iam_action, iam_memo]) ─────────────
    private static final Map<String, List<String[]>> IAM_TEMPLATES = Map.of(
            "FEE", List.of(
                    new String[]{"5G 요금제 변경 시 혜택 및 비용 문의", "최적 5G 요금제 안내 후 변경 처리 완료", "다음 달부터 신규 요금 적용 예정"},
                    new String[]{"청구서 항목 이중 청구 이상 문의", "이중 청구 오류 확인 및 정정 처리", "익월 청구서에서 차감 예정, 고객 동의 완료"},
                    new String[]{"결합 할인 미적용 건 확인 요청", "결합 할인 재적용 요청 시스템 등록 완료", "1~3 영업일 내 처리 후 SMS 발송 예정"},
                    new String[]{"납부 방법 변경 요청 (자동이체→카드)", "납부 방법 변경 처리 완료", "다음 달 청구서부터 적용"},
                    new String[]{"미납 요금 연체 이자 발생 문의", "미납 내역 및 연체 이자 상세 안내 후 납부 유도", "당일 납부 완료, 이자 면제 처리"},
                    new String[]{"부가세 포함 요금 산정 방식 문의", "요금 산정 기준 및 부가세 포함 내역 상세 안내", "안내 완료, 추가 문의 없음"},
                    new String[]{"약정 할인 만료 후 요금 인상 확인 요청", "약정 만료 일정 및 요금 변동 사항 안내, 재약정 유도", "재약정 결정, 신규 요금 적용 완료"},
                    new String[]{"포인트 및 마일리지 요금 차감 요청", "가용 포인트 조회 후 요금 차감 처리 완료", "포인트 차감 후 잔여 포인트 안내"},
                    new String[]{"패밀리 결합 할인 신청 문의", "결합 대상 회선 확인 후 패밀리 결합 신청 처리", "다음 달 1일부터 결합 할인 적용 예정"},
                    new String[]{"선택약정 25% 할인 신청 문의", "선택약정 할인 적용 조건 확인 후 신청 처리 완료", "다음 달부터 25% 할인 적용 예정"},
                    new String[]{"분리 청구 통신비 영수증 발급 요청", "청구 내역 확인 후 사업자 영수증 발급 처리", "영수증 이메일 발송 완료"},
                    new String[]{"실시간 요금 조회 방법 문의", "U+ 앱 요금 조회 기능 이용 방법 안내", "앱 이용 방법 안내 완료"},
                    new String[]{"통신비 지원 바우처(K-디지털 크레딧) 문의", "지원 대상 확인 후 신청 방법 안내", "신청 처리 완료, 다음 달 반영 예정"},
                    new String[]{"데이터 초과 요금 발생 이유 문의", "이번 달 데이터 사용 내역 조회 후 초과 사유 상세 안내", "안내 완료, 초과 방지 알림 설정 도움"},
                    new String[]{"인터넷 TV 월정액 요금 일할 계산 요청", "개통일 기준 일할 요금 계산 내역 안내", "이의 제기 접수 후 정정 처리 완료"},
                    new String[]{"무약정 요금제 변경 시 혜택 차이 문의", "무약정 vs 약정 비교 안내 후 고객 판단 유도", "비교 안내 완료, 고객 약정 유지 결정"},
                    new String[]{"청구서 수령 방식 변경 요청 (이메일→앱)", "청구서 수령 방식 변경 처리", "다음 달 청구분부터 앱 push로 발송 예정"}
            ),
            "DEV", List.of(
                    new String[]{"기기변경 공시지원금 적용 조건 문의", "기기 목록 및 지원금 조건 안내 후 예약 접수", "기기변경 예약 접수 완료, 방문 일정 SMS 발송"},
                    new String[]{"단말기 할부금 잔여액 및 소멸 일정 확인 요청", "현행 할부 계획 조회 후 잔여 할부금 상세 안내", "안내 완료, 추가 문의 없음"},
                    new String[]{"유심 분실로 인한 재발급 요청", "본인 인증 후 유심 재발급 접수 처리", "3~5 영업일 이내 등기 배송 예정"},
                    new String[]{"번호이동 절차 및 단말기 교체 문의", "번호이동 절차 안내 및 단말기 재고 조회 후 예약 접수", "번호이동 예약 완료, 개통 일정 안내"},
                    new String[]{"단말기 액정 파손 수리 접수 요청", "공식 AS 센터 접수 절차 안내 및 수리 비용 안내", "AS 접수 완료, 수리 기간 3~5일 안내"},
                    new String[]{"eSIM 전환 신청 문의", "eSIM 지원 단말기 확인 후 전환 절차 안내 및 처리", "eSIM 전환 완료, 기존 유심 비활성화 처리"},
                    new String[]{"중고 단말기 구입 후 개통 문의", "중고 단말기 개통 가능 여부 확인 후 개통 처리", "개통 완료, 요금제 적용 안내"},
                    new String[]{"단말기 보험 가입 여부 확인 요청", "가입 이력 조회 후 보험 혜택 및 청구 방법 안내", "안내 완료, 보험 청구 방법 SMS 발송"},
                    new String[]{"공기계 유심 개통 문의", "공기계 유심 개통 절차 및 필요 서류 안내 후 처리", "유심 개통 완료, 서비스 정상 이용 안내"},
                    new String[]{"단말기 분실 신고 및 회선 정지 요청", "분실 신고 접수 후 회선 이용 정지 처리 완료", "분실 신고 완료, 재발급 시 재연락 안내"},
                    new String[]{"해외 직구 단말기 국내 개통 가능 여부 문의", "해외 직구 단말기 주파수 밴드 확인 후 개통 가능 여부 안내", "개통 가능 확인, 유심 개통 처리 완료"},
                    new String[]{"분실 신고 해제 및 정지 해제 요청", "본인 인증 후 분실 신고 해제 및 회선 정지 해제 처리", "정지 해제 완료, 서비스 정상 이용 가능"},
                    new String[]{"선불폰 후불 전환 방법 문의", "선불에서 후불 전환 절차 및 필요 서류 안내", "전환 신청 접수 완료, 개통까지 1~2 영업일 소요"},
                    new String[]{"약정 기간 내 단말기 교체 가능 여부 문의", "약정 내 단말기 교체 시 위약금 발생 여부 안내", "조건 안내 완료, 고객 약정 만료 후 교체 결정"},
                    new String[]{"NFC 결제 설정 방법 문의", "단말기별 NFC 결제 설정 방법 안내", "설정 방법 안내 완료, 결제 등록 성공 확인"},
                    new String[]{"단말기 구입 후 초기 설정 도움 요청", "단말기 초기 설정 가이드 안내 및 U+ 앱 설치 도움", "초기 설정 완료, 주요 기능 안내"},
                    new String[]{"중고 단말기 할부 이전 가능 여부 문의", "할부 이전 불가 안내 및 새 할부 조건 설명", "안내 완료, 신규 할부 조건 SMS 발송"}
            ),
            "TRB", List.of(
                    new String[]{"인터넷 속도 저하 불만 접수", "원격 점검 후 현장 기사 출동 요청 등록", "출동 일정 고객 협의 완료, 익일 방문 예정"},
                    new String[]{"TV 화면 재생 오류 및 셋톱박스 이상 문의", "셋톱박스 재부팅 가이드 및 원격 점검 실시", "원격 조치 완료, 정상 작동 확인"},
                    new String[]{"모바일 데이터 연결 불가 문의", "APN 설정 초기화 및 네트워크 재설정 가이드 안내", "고객 직접 조치 후 정상 연결 확인"},
                    new String[]{"Wi-Fi 공유기 연결 불가 문의", "공유기 설정 초기화 가이드 및 원격 점검 실시", "원격 점검 후 정상 연결 확인"},
                    new String[]{"문자 수신 불가 문제 접수", "네트워크 설정 초기화 안내 및 기지국 장애 여부 확인", "기지국 정상 확인, 단말기 재설정 후 수신 정상"},
                    new String[]{"통화 품질 불량(잡음·끊김) 접수", "통화 품질 점검 요청 등록 및 기지국 신호 세기 확인", "기지국 담당팀 이관 완료, 2영업일 내 조치 예정"},
                    new String[]{"인터넷 TV 채널 수신 불량 접수", "셋톱박스 재설정 및 케이블 연결 상태 점검 안내", "원격 점검 후 채널 수신 정상 확인"},
                    new String[]{"모바일 앱 로그인 오류 접수", "앱 캐시 삭제 및 재설치 가이드 안내", "고객 직접 조치 후 로그인 정상 확인"},
                    new String[]{"인터넷 공사 지연으로 인한 불편 접수", "공사 지연 사유 안내 및 예상 완료 일정 재통보", "완료 일정 재안내, 불편 사과 처리"},
                    new String[]{"특정 앱에서만 인터넷 불가 문제 접수", "앱별 네트워크 설정 확인 및 캐시 삭제 가이드 안내", "고객 직접 조치 후 정상 이용 확인"},
                    new String[]{"번호 도용 문자 발송 의심 신고", "번호 도용 신고 접수 후 보안팀 이관 처리", "신고 완료, 임시 발신 차단 및 보안팀 조사 예정"},
                    new String[]{"유심 인식 불량 문제 접수", "유심 트레이 이물질 제거 안내 후 유심 재삽입 지도", "고객 직접 조치 후 정상 인식 확인"},
                    new String[]{"아파트 입주 후 인터넷 불통 문제 접수", "입주 현장 인터넷 인입 공사 여부 확인 후 기사 출동 예약", "출동 일정 예약 완료, 다음 날 방문 예정"},
                    new String[]{"통화 중 잡음 심화 문제 접수", "통화 품질 원격 진단 실시 및 네트워크 신호 세기 점검", "신호 불량 지역 확인 후 기술팀 이관 처리"},
                    new String[]{"인터넷 가입 후 개통 지연 불만 접수", "개통 처리 현황 확인 후 지연 사유 안내", "당일 개통 처리 완료, 불편 사과 처리"},
                    new String[]{"음성 사서함 해제 요청", "음성 사서함 서비스 해제 절차 안내 및 처리", "사서함 서비스 해제 완료"},
                    new String[]{"국제 전화 수신 차단 문의", "국제전화 수신 차단 기능 안내 및 설정 처리", "국제전화 수신 차단 설정 완료"}
            ),
            "CHN", List.of(
                    new String[]{"약정 만료 후 해지 의사 표명", "잔여 혜택 및 재약정 조건 비교 안내, 재약정 유도", "고객 재약정 결정, 신규 약정 등록 완료"},
                    new String[]{"중도 해지 시 위약금 금액 조회 요청", "현행 약정 기준 위약금 계산 결과 상세 안내", "위약금 확인 완료, 해지 여부 고객 재검토 예정"},
                    new String[]{"타사 이동을 위한 번호이동 해지 요청", "번호이동 절차 안내 및 잔류 혜택 비교 제안 진행", "이탈 방어 미성공, 번호이동 해지 처리 완료"},
                    new String[]{"기기변경 후 기존 약정 해지 여부 문의", "기기변경 시 약정 갱신 구조 안내, 해지 없이 유지 가능 설명", "기기변경 유지 결정, 신규 약정 등록 완료"},
                    new String[]{"가족 결합 해지 시 위약금 문의", "결합 해지 위약금 및 개별 요금 변동 사항 안내", "결합 유지 결정, 위약금 면제 확인 완료"},
                    new String[]{"재약정 혜택 부족하다며 조건 개선 요구", "추가 혜택 적용 가능 항목 검토 후 조건 개선 제안", "개선된 조건으로 재약정 완료"},
                    new String[]{"중도 해지 후 재가입 가능 여부 문의", "해지 후 재가입 조건 및 공백 기간 패널티 안내", "해지 의사 철회, 약정 유지 결정"},
                    new String[]{"해지 신청 후 철회 요청", "해지 신청 철회 절차 안내 및 처리", "해지 철회 완료, 기존 약정 유지 처리"},
                    new String[]{"모바일 단독 해지 후 인터넷 요금 변동 문의", "결합 상품 구조 안내 및 모바일 해지 시 요금 변동 상세 설명", "설명 이해 후 모바일 유지 결정"},
                    new String[]{"장기 약정 해지 시 포인트 소멸 여부 문의", "해지 시 잔여 포인트 소멸 기준 안내", "포인트 소멸 전 사용 유도, 고객 포인트 즉시 사용 완료"},
                    new String[]{"해지 후 동일 번호 유지 가능 여부 문의", "번호이동 외 동일 번호 재부여 불가 안내", "안내 완료, 해지 의사 철회 및 약정 유지 결정"},
                    new String[]{"타사에서 U+로 번호이동 재약정 문의", "번호이동 재약정 혜택 및 개통 절차 안내", "번호이동 재약정 신청 완료"},
                    new String[]{"약정 자동 연장 해제 요청", "약정 자동 연장 해제 절차 안내 및 처리", "자동 연장 해제 완료, 약정 만료 시 무약정 전환 예정"},
                    new String[]{"2회선 중 1개 해지 시 결합 할인 변동 문의", "1회선 해지 시 결합 할인 구조 변경 사항 상세 안내", "변동 내역 확인 후 2회선 유지 결정"},
                    new String[]{"인터넷 단독 해지 후 TV 유지 가능 여부 문의", "결합 상품 구조 안내 및 TV 단독 이용 시 요금 변동 설명", "설명 이해 후 인터넷 유지 결정"},
                    new String[]{"U+에서 SKT로 번호이동 절차 문의", "타사 번호이동 절차 안내", "이탈 방어 시도 후 고객 번호이동 의사 확인"},
                    new String[]{"약정 만료 전 해지 위약금 면제 방법 문의", "위약금 면제 적용 가능 사유(이사·사망 등) 안내", "안내 완료, 고객 해지 철회 결정"}
            ),
            "ADD", List.of(
                    new String[]{"해외 출국 전 로밍 서비스 가입 문의", "이용 국가별 로밍 요금제 비교 안내 및 최적 상품 추천", "출국 3일 전 자동 개통 예약 완료"},
                    new String[]{"구독 중인 OTT 서비스 변경 요청", "현행 부가서비스 현황 조회 후 요청 서비스로 변경 처리", "다음 달 1일부터 변경 사항 적용 예정"},
                    new String[]{"넷플릭스 결합 요금제 문의", "넷플릭스 결합 가능 요금제 안내 및 추가 비용 설명", "넷플릭스 결합 신청 완료"},
                    new String[]{"부가서비스 무료 체험 기간 종료 안내", "무료 체험 종료 일정 안내 및 유료 전환 또는 해지 선택", "고객 요청에 따라 부가서비스 해지 처리 완료"},
                    new String[]{"해외 로밍 데이터 초과 요금 문의", "로밍 데이터 초과 요금 산정 기준 안내 및 무제한 로밍 업그레이드 제안", "로밍 무제한으로 업그레이드 완료"},
                    new String[]{"자녀 안심 서비스 가입 요청", "자녀 안심 서비스 상품 안내 및 가입 처리", "서비스 가입 완료, 이용 방법 SMS 발송"},
                    new String[]{"부가통화 서비스(U+콜) 문의", "U+ 부가통화 서비스 종류 및 요금 안내", "서비스 가입 완료"},
                    new String[]{"게임 데이터 무제한 패키지 문의", "게임 특화 데이터 패키지 상품 안내 및 가입", "게임 데이터 패키지 가입 완료"},
                    new String[]{"스팸 문자 차단 서비스 가입 요청", "스팸 차단 서비스 안내 및 즉시 가입 처리", "스팸 차단 서비스 가입 완료, 즉시 적용"},
                    new String[]{"클라우드 백업 서비스 가입 문의", "U+ 클라우드 서비스 용량별 요금 안내 후 가입 처리", "클라우드 서비스 30일 무료 체험 가입 완료"},
                    new String[]{"어린이 안심 통화 부가서비스 해지 요청", "어린이 안심 서비스 해지 절차 안내 및 처리", "서비스 해지 완료, 익월 청구 반영"},
                    new String[]{"가족 결합 회선 부가서비스 일괄 조회 요청", "결합 회선 전체 부가서비스 현황 조회 후 안내", "현황 안내 완료, 불필요 서비스 해지 처리"}
            ),
            "ETC", List.of(
                    new String[]{"인근 U+ 공식 매장 위치 문의", "고객 주소 기반 인근 매장 3곳 안내", "안내 완료"},
                    new String[]{"서비스 이용 불만 접수 및 처리 요청", "불만 내용 청취 후 담당 부서 이관 처리", "처리 결과 3 영업일 이내 회신 예정"},
                    new String[]{"개인정보 처리 방침 문의", "개인정보 수집·이용 항목 및 처리 방침 안내", "안내 완료, 개인정보 처리방침 URL SMS 발송"},
                    new String[]{"고객센터 운영 시간 문의", "고객센터 채널별 운영 시간 안내 (전화·챗봇·앱)", "안내 완료"},
                    new String[]{"경품 당첨 사기 문자 신고", "스미싱 문자 내용 접수 및 신고 절차 안내", "신고 접수 완료, 보안팀 이관 처리"},
                    new String[]{"서비스 이용 내역 조회 방법 문의", "마이페이지 이용 내역 조회 방법 안내", "안내 완료, 앱 이용 방법 SMS 발송"},
                    new String[]{"U+ 앱 가입 및 이용 방법 문의", "U+ 앱 다운로드 및 주요 기능 이용 방법 안내", "앱 설치 완료, 주요 기능 안내 SMS 발송"},
                    new String[]{"사망자 명의 회선 해지 방법 문의", "사망자 명의 해지 시 필요 서류 및 절차 안내", "필요 서류 안내 완료, 가까운 지점 방문 안내"},
                    new String[]{"청소년 보호 차단 서비스 문의", "청소년 유해 콘텐츠 차단 서비스 종류 및 설정 방법 안내", "차단 서비스 설정 완료"},
                    new String[]{"장애인 요금 감면 신청 방법 문의", "장애인 통신요금 감면 대상 및 신청 방법 안내", "신청 접수 완료, 처리 후 SMS 발송 예정"},
                    new String[]{"고객 정보 변경(주소·이메일) 요청", "본인 인증 후 고객 정보 변경 처리 완료", "변경 정보 업데이트 완료"},
                    new String[]{"법인 대표 번호 변경 요청", "법인 정보 확인 후 대표 번호 변경 처리", "대표 번호 변경 완료, 법인 고객 포털 반영 예정"}
            )
    );

    // ─────────────────────────────────────────────────────────
    //  Step 1: consultation_results + raw_texts INSERT (단일 트랜잭션)
    // ─────────────────────────────────────────────────────────

    /**
     * consultation_results와 consultation_raw_texts를 하나의 트랜잭션으로 삽입한다.
     * 이 메서드는 반드시 외부 Bean(SyntheticDataGeneratorScheduler)에서 호출해야
     * Spring @Transactional이 정상 동작한다.
     *
     * @return 생성된 (consultId 목록, categoryCode 목록) 쌍
     */
    @Transactional
    public BatchResult executeStep1(int batchSize) {
        return executeStep1WithDate(batchSize, null);
    }

    /**
     * 과거 데이터 생성용 오버로드 — targetDate가 지정되면 해당 날짜의 랜덤 업무시간으로 created_at을 설정한다.
     *
     * <p>HistoricalBatchService에서 호출하여 특정 날짜의 상담 데이터를 소급 생성한다.
     *
     * @param batchSize  생성할 건수
     * @param targetDate null이면 현재 시각 사용, 지정하면 해당 날짜 8~18시 내 랜덤 시각
     * @return 생성된 (consultId 목록, categoryCode 목록) 쌍
     */
    @Transactional
    public BatchResult executeStep1WithDate(int batchSize, LocalDate targetDate) {
        var random    = ThreadLocalRandom.current();
        var agents    = personMatcher.getAgents();
        var customers = personMatcher.getCustomers();

        if (agents.isEmpty() || customers.isEmpty()) {
            log.warn("[SyntheticFactory] 상담사 또는 고객 데이터 없음 — Step1 스킵");
            return new BatchResult(List.of(), List.of());
        }

        String resultSql = """
                INSERT INTO consultation_results
                    (emp_id, customer_id, channel, category_code, duration_sec,
                     iam_issue, iam_action, iam_memo, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """;

        List<Long>    consultIds       = new ArrayList<>(batchSize);
        List<String>  categoryCodes    = new ArrayList<>(batchSize);
        List<String>  channels         = new ArrayList<>(batchSize);
        List<Integer> empIds           = new ArrayList<>(batchSize);
        List<Long>    customerIds      = new ArrayList<>(batchSize);
        List<SyntheticPersonMatcher.CustomerInfo> consultCustomers = new ArrayList<>(batchSize);

        for (int i = 0; i < batchSize; i++) {
            var agent    = agents.get(random.nextInt(agents.size()));
            var customer = customers.get(random.nextInt(customers.size()));

            String categoryCode = pickCategoryCode(random.nextInt(100));
            String channel      = random.nextInt(100) < 70 ? "CALL" : "CHATTING";
            int    durationSec  = "CALL".equals(channel)
                    ? 120 + random.nextInt(481)   // CALL: 120~600s
                    : 60  + random.nextInt(241);  // CHATTING: 60~300s

            String[] iam  = pickIamTemplate(categoryCode, random.nextInt(10));
            String   memo = iam[2];

            // ── PreparedStatementCreator는 effectively-final 변수만 캡처 가능 ──
            final int    empId      = agent.empId();
            final long   customerId = customer.customerId();
            final String ch         = channel;
            final String catCode    = categoryCode;
            final int    dur        = durationSec;
            final String issue      = iam[0];
            final String action     = iam[1];
            final String finalMemo  = memo;
            final LocalDateTime now = (targetDate != null)
                    ? randomBusinessTime(targetDate, random)
                    : LocalDateTime.now();

            KeyHolder keyHolder = new GeneratedKeyHolder();
            jdbcTemplate.update(con -> {
                PreparedStatement ps = con.prepareStatement(resultSql, Statement.RETURN_GENERATED_KEYS);
                ps.setInt(1, empId);
                ps.setLong(2, customerId);
                ps.setString(3, ch);
                ps.setString(4, catCode);
                ps.setInt(5, dur);
                ps.setString(6, issue);
                ps.setString(7, action);
                ps.setString(8, finalMemo);
                ps.setObject(9, now);
                return ps;
            }, keyHolder);

            consultIds.add(keyHolder.getKey().longValue());
            categoryCodes.add(categoryCode);
            channels.add(channel);
            empIds.add(empId);
            customerIds.add(customerId);
            consultCustomers.add(customer);
        }

        // ── consultation_raw_texts Bulk INSERT ──
        List<Object[]> rawArgs = new ArrayList<>(batchSize);
        for (int i = 0; i < batchSize; i++) {
            SyntheticPersonMatcher.CustomerInfo ci = consultCustomers.get(i);
            // TRB 카테고리는 홈/인터넷 품질 문제 → 홈 상품명 우선, 나머지는 모바일 상품명 우선
            String planName = categoryCodes.get(i).startsWith("M_TRB")
                    ? (ci.homePlanName() != null ? ci.homePlanName() : ci.mobilePlanName())
                    : (ci.mobilePlanName() != null ? ci.mobilePlanName() : ci.homePlanName());
            rawArgs.add(new Object[]{
                    consultIds.get(i),
                    rawTextGenerator.generate(categoryCodes.get(i), channels.get(i),
                            new java.util.Random(ThreadLocalRandom.current().nextLong()),
                            ci.name(), planName)
            });
        }
        jdbcTemplate.batchUpdate(
                "INSERT INTO consultation_raw_texts (consult_id, raw_text_json) VALUES (?, ?)",
                rawArgs
        );

        // ── 연관 테이블 Bulk INSERT (같은 트랜잭션) ───────────────────────
        insertClientReviews(consultIds, random);
        insertCustomerRiskLogs(consultIds, empIds, customerIds, categoryCodes, random);
        insertProductLogs(consultIds, customerIds, categoryCodes, consultCustomers, random);

        log.info("[SyntheticFactory] Step1 완료 — consultation_results: {}건, raw_texts: {}건 | " +
                        "consultId 범위: {} ~ {}",
                batchSize, batchSize,
                consultIds.get(0), consultIds.get(consultIds.size() - 1));

        return new BatchResult(consultIds, categoryCodes);
    }

    // ─────────────────────────────────────────────────────────
    //  Step 2: result_event_status, excellent_event_status REQUESTED (Step 1 커밋 후)
    // ─────────────────────────────────────────────────────────

    public void triggerAiExtraction(List<Long> consultIds, List<String> categoryCodes) {
        List<Object[]> args = new ArrayList<>(consultIds.size());
        for (int i = 0; i < consultIds.size(); i++) {
            args.add(new Object[]{consultIds.get(i), categoryCodes.get(i)});
        }

        jdbcTemplate.batchUpdate(
                """
                INSERT INTO result_event_status
                    (consult_id, category_code, status, retry_count, created_at, updated_at)
                VALUES (?, ?, 'REQUESTED', 0, NOW(), NOW())
                """,
                args
        );
    }
    public void triggerExcellentScoring(List<Long> consultIds) {
        List<Object[]> args = new ArrayList<>(consultIds.size());
        for (Long cid : consultIds) {
            args.add(new Object[]{cid});
        }

        jdbcTemplate.batchUpdate(
                """
                INSERT INTO excellent_event_status
                    (consult_id, status, retry_count, created_at, updated_at)
                VALUES (?, 'REQUESTED', 0, NOW(), NOW())
                """,
                args
        );
        log.info("[SyntheticFactory] {}건 채점 이벤트(excellent) 발행 완료", consultIds.size());
    }
    // ─────────────────────────────────────────────────────────
    //  Step 3: summary_event_status REQUESTED (Step 1 커밋 후)
    // ─────────────────────────────────────────────────────────

    public void triggerSummaryGeneration(List<Long> consultIds) {
        List<SummaryEventStatus> events = new ArrayList<>(consultIds.size());
        for (Long cid : consultIds) {
            events.add(SummaryEventStatus.builder().consultId(cid).build());
        }
        summaryEventStatusRepo.saveAll(events);
    }

    // ─────────────────────────────────────────────────────────
    //  내부 DTO
    // ─────────────────────────────────────────────────────────

    // ─────────────────────────────────────────────────────────
    //  연관 테이블 INSERT 헬퍼
    // ─────────────────────────────────────────────────────────

    /**
     * client_review — 고객 만족도 평가 (70% 확률로 생성).
     * score_1~5: 1~5점 랜덤, score_average: 평균.
     */
    private void insertClientReviews(List<Long> consultIds, ThreadLocalRandom random) {
        List<Object[]> args = new ArrayList<>();
        for (Long consultId : consultIds) {
            if (random.nextInt(100) >= 70) continue; // 30% 미응답

            int s1 = 1 + random.nextInt(5);
            int s2 = 1 + random.nextInt(5);
            int s3 = 1 + random.nextInt(5);
            int s4 = 1 + random.nextInt(5);
            int s5 = 1 + random.nextInt(5);
            double avg = Math.round((s1 + s2 + s3 + s4 + s5) / 5.0 * 10) / 10.0;
            args.add(new Object[]{consultId, s1, s2, s3, s4, s5, avg});
        }
        if (!args.isEmpty()) {
            jdbcTemplate.batchUpdate(
                    "INSERT INTO client_review (consult_id, score_1, score_2, score_3, score_4, score_5, score_average) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?)",
                    args
            );
        }
    }

    /**
     * customer_risk_logs — 위험 고객 감지 로그 (40% 확률로 생성).
     * CHN 카테고리는 CHURN 유형 우선, 나머지는 랜덤.
     */
    private void insertCustomerRiskLogs(List<Long> consultIds, List<Integer> empIds,
                                        List<Long> customerIds, List<String> categoryCodes,
                                        ThreadLocalRandom random) {
        List<Object[]> args = new ArrayList<>();
        for (int i = 0; i < consultIds.size(); i++) {
            if (random.nextInt(100) >= 40) continue; // 60% 해당 없음

            String typeCode;
            if (categoryCodes.get(i).contains("CHN")) {
                // CHN은 CHURN 60%, 나머지 40%
                typeCode = random.nextInt(100) < 60 ? "CHURN"
                        : RISK_TYPES.get(random.nextInt(RISK_TYPES.size()));
            } else {
                typeCode = RISK_TYPES.get(random.nextInt(RISK_TYPES.size()));
            }
            String levelCode = RISK_LEVELS.get(random.nextInt(RISK_LEVELS.size()));

            args.add(new Object[]{consultIds.get(i), empIds.get(i), customerIds.get(i), typeCode, levelCode});
        }
        if (!args.isEmpty()) {
            jdbcTemplate.batchUpdate(
                    "INSERT INTO customer_risk_logs (consult_id, emp_id, customer_id, type_code, level_code) " +
                    "VALUES (?, ?, ?, ?, ?)",
                    args
            );
        }
    }

    /**
     * consult_product_logs — 상담 중 처리된 상품 변경 이력.
     * 60% 확률로 1건, 20% 확률로 2건, 20% 0건.
     * contract_type에 따라 new/canceled 컬럼을 구분하여 삽입.
     * 고객의 실제 구독 코드(mobileCode/homeCode)를 우선 사용해 원문 내용과 정합성을 맞춘다.
     */
    private void insertProductLogs(List<Long> consultIds, List<Long> customerIds,
                                   List<String> categoryCodes,
                                   List<SyntheticPersonMatcher.CustomerInfo> consultCustomers,
                                   ThreadLocalRandom random) {
        // 코드가 있는 상품 유형만 후보로 포함 (없는 유형은 랜덤 선택 대상에서 제외)
        List<String> availableProductTypes = new ArrayList<>();
        if (cacheDummy.getMobileProductCodes() != null && !cacheDummy.getMobileProductCodes().isEmpty())
            availableProductTypes.add("mobile");
        if (cacheDummy.getHomeProductCodes() != null && !cacheDummy.getHomeProductCodes().isEmpty())
            availableProductTypes.add("home");
        if (cacheDummy.getAdditionalProductCodes() != null && !cacheDummy.getAdditionalProductCodes().isEmpty())
            availableProductTypes.add("additional");
        if (availableProductTypes.isEmpty()) return;

        List<Object[]> args = new ArrayList<>();
        String[] contractTypes = {"NEW", "CANCEL", "CHANGE", "RENEW"};

        for (int i = 0; i < consultIds.size(); i++) {
            int logCount;
            int roll = random.nextInt(100);
            if (roll < 20) continue;          // 20% 상품 변경 없음
            else if (roll < 80) logCount = 1; // 60% 1건
            else logCount = 2;                // 20% 2건

            SyntheticPersonMatcher.CustomerInfo ci = consultCustomers.get(i);

            for (int j = 0; j < logCount; j++) {
                // CHN은 RENEW/CANCEL 위주, 나머지는 랜덤
                String contractType;
                if (categoryCodes.get(i).contains("CHN")) {
                    contractType = random.nextBoolean() ? "RENEW" : "CANCEL";
                } else {
                    contractType = contractTypes[random.nextInt(contractTypes.length)];
                }
                String productType = availableProductTypes.get(random.nextInt(availableProductTypes.size()));

                // 고객의 실제 구독 코드 우선 사용 (없으면 CacheDummy 풀에서 랜덤 선택)
                String customerCode = "mobile".equals(productType) ? ci.mobileCode()
                        : "home".equals(productType) ? ci.homeCode() : null;
                String code1 = customerCode != null ? customerCode : pickProductCode(productType, random);
                // CHANGE의 신규 상품은 기존과 다른 코드로 선택
                String code2 = "CHANGE".equals(contractType)
                        ? pickDifferentCode(productType, code1, random) : null;

                args.add(buildProductLogArgs(
                        consultIds.get(i), customerIds.get(i),
                        contractType, productType, code1, code2));
            }
        }
        if (!args.isEmpty()) {
            jdbcTemplate.batchUpdate(
                    "INSERT INTO consult_product_logs " +
                    "(consult_id, customer_id, contract_type, product_type, " +
                    "new_product_home, new_product_mobile, new_product_service, " +
                    "canceled_product_home, canceled_product_mobile, canceled_product_service) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    args
            );
        }
    }

    /** product_type × contract_type 조합으로 10개 컬럼 Object[] 생성 */
    private Object[] buildProductLogArgs(long consultId, long customerId,
                                         String contractType, String productType,
                                         String code1, String code2) {
        // [new_home, new_mobile, new_service, cancel_home, cancel_mobile, cancel_service]
        String[] cols = new String[6];
        int newIdx    = switch (productType) { case "home" -> 0; case "mobile" -> 1; default -> 2; };
        int cancelIdx = newIdx + 3;

        boolean isNew    = "NEW".equals(contractType) || "RENEW".equals(contractType);
        boolean isCancel = "CANCEL".equals(contractType);
        boolean isChange = "CHANGE".equals(contractType);

        if (isNew)    cols[newIdx]    = code1;
        if (isCancel) cols[cancelIdx] = code1;
        if (isChange) { cols[newIdx] = code1; cols[cancelIdx] = code2; }

        return new Object[]{consultId, customerId, contractType, productType,
                cols[0], cols[1], cols[2], cols[3], cols[4], cols[5]};
    }

    private String pickProductCode(String productType, ThreadLocalRandom random) {
        List<String> pool = switch (productType) {
            case "home"       -> cacheDummy.getHomeProductCodes();
            case "mobile"     -> cacheDummy.getMobileProductCodes();
            default           -> cacheDummy.getAdditionalProductCodes();
        };
        return pool.get(random.nextInt(pool.size()));
    }

    private String pickDifferentCode(String productType, String exclude, ThreadLocalRandom random) {
        List<String> pool = switch (productType) {
            case "home"   -> cacheDummy.getHomeProductCodes();
            case "mobile" -> cacheDummy.getMobileProductCodes();
            default       -> cacheDummy.getAdditionalProductCodes();
        };
        if (pool.size() == 1) return pool.get(0); // 단일 코드면 그대로
        String code;
        do { code = pool.get(random.nextInt(pool.size())); } while (code.equals(exclude));
        return code;
    }

    public record BatchResult(List<Long> consultIds, List<String> categoryCodes) {
        public boolean isEmpty() { return consultIds.isEmpty(); }
    }

    // ─────────────────────────────────────────────────────────
    //  카테고리 분포 선택 (§2-1)
    // ─────────────────────────────────────────────────────────

    /** CHN 40% / TRB 20% / FEE 20% / 기타(ADD·ETC) 20% */
    private String pickCategoryCode(int roll) {
        List<String> pool;
        if      (roll < 40) pool = personMatcher.getChnCodes();
        else if (roll < 60) pool = personMatcher.getTrbCodes();
        else if (roll < 80) pool = personMatcher.getFeeCodes();
        else                pool = personMatcher.getOtherCodes();
        return pool.get(ThreadLocalRandom.current().nextInt(pool.size()));
    }

    // ─────────────────────────────────────────────────────────
    //  IAM 템플릿 선택
    // ─────────────────────────────────────────────────────────

    /**
     * targetDate의 업무 시간대(08:00 ~ 18:00) 내 랜덤 시각을 반환한다.
     * 과거 데이터 생성 시 created_at을 해당 날짜에 맞추기 위해 사용한다.
     */
    private LocalDateTime randomBusinessTime(LocalDate targetDate, ThreadLocalRandom random) {
        int hour   = 8 + random.nextInt(10);      // 08 ~ 17시
        int minute = random.nextInt(60);
        int second = random.nextInt(60);
        return targetDate.atTime(hour, minute, second);
    }

    /** categoryCode 접두사(M_FEE_01 → "FEE")로 IAM 세트 선택. 미매핑 시 ETC 폴백. */
    private String[] pickIamTemplate(String categoryCode, int randomIndex) {
        String key = "ETC";
        if (categoryCode != null) {
            String[] parts = categoryCode.split("_");
            if (parts.length >= 2) key = parts[1];
        }
        List<String[]> templates = IAM_TEMPLATES.getOrDefault(key, IAM_TEMPLATES.get("ETC"));
        return templates.get(randomIndex % templates.size());
    }
}

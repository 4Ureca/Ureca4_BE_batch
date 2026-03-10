package com.uplus.batch.domain.summary.values;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class ExtractKeywords {

  private ExtractKeywords() {}

  // === 위험 감지 ===
  private static final Set<String> RISK_KEYWORDS = Set.of(
      "risk_abuse", "risk_churn", "risk_fraud", "risk_phishing",
      "risk_repeat_voc", "risk_policy_abuse", "risk_compensation", "risk_voc",
      "폭언", "욕설", "갑질", "언어폭력", "폭언욕설",
      "해지위험", "이탈위험", "이탈방어", "해지방어", "해지예정", "해지징후",
      "사기", "사기의심", "부정사용",
      "피싱", "스미싱", "보이스피싱", "피싱해킹", "피싱피해",
      "반복민원", "고질민원", "블랙컨슈머", "상습민원",
      "정책악용", "제도악용", "혜택악용", "규정악용", "꼼수",
      "보상요구", "배상요구", "환불요구", "무리한보상", "과도한요구",
      "방통위", "소비자원"
  );

  // === 계약 / 처리 ===
  private static final Set<String> CONTRACT_KEYWORDS = Set.of(
      "번호이동", "번이", "번호이동해지", "타사이동", "타사이동해지", "통신사변경",
      "기기변경", "기변", "폰교체", "단말기변경",
      "재약정", "약정연장", "약정갱신",
      "신규가입", "신규개통", "첫가입",
      "명의변경", "명의이전", "소유자변경",
      "개통취소", "가입취소", "신청취소",
      "해지재약정", "약정만료해지", "만기해지", "계약만료해지",
      "기기약정", "단말기약정", "폰약정"
  );

  // === 설치 / 해지 ===
  private static final Set<String> INSTALL_KEYWORDS = Set.of(
      "이전설치", "이사설치", "설치변경", "주소변경",
      "신규설치", "첫설치",
      "해지", "해지접수", "서비스종료", "중도해지"
  );

  // === 요금 / 청구 ===
  private static final Set<String> BILLING_KEYWORDS = Set.of(
      "공시지원금", "단말기지원금",
      "위약금", "할인반환금", "해지비용", "해지위약금",
      "요금감면", "가격",
      "미납", "미결제", "요금밀림", "미납요금",
      "청구서", "고지서", "명세서", "요금내역서",
      "요금제", "요금상품", "요금종류",
      "요금납부", "요금결제", "청구요금", "청구금액", "납부금액",
      "결합할인", "가족결합", "묶음할인", "투게더", "참쉬운결합",
      "분할납부", "나눠내기", "분납", "분납신청", "나눠내기신청", "분할납부신청",
      "선택약정", "선약", "선택약정할인", "선택약정요금할인",
      "요금제변경", "플랜변경", "요금변경",
      "미납안내", "미납통보", "연체안내", "요금연체",
      "이용정지", "일시정지", "사용정지",
      "장기고객할인", "장기사용자할인",
      "자동이체", "이중청구", "소급적용", "일할계산"
  );

  // === 단말기 ===
  private static final Set<String> DEVICE_KEYWORDS = Set.of(
      "아이폰",
      "갤럭시", "갤럭시폰", "갤럭시탭", "삼성폰",
      "휴대폰", "핸드폰",
      "단말기할부", "휴대폰할부", "단말기할부금", "기기할부금",
      "단말기잔여할부", "기기잔여할부",
      "자급제", "자급제폰", "공기계",
      "선불폰", "선불요금제", "선불개통",
      "중고폰", "리퍼폰", "중고단말기"
  );

  // === 네트워크 / 장애 ===
  private static final Set<String> NETWORK_KEYWORDS = Set.of(
      "5g", "lte",
      "와이파이", "공유기", "라우터",
      "인터넷속도", "인터넷연결",
      "속도저하", "속도느림", "속도불량",
      "연결불가", "인터넷안됨",
      "와이파이불안정",
      "셋톱박스오류", "셋탑박스고장",
      "통화품질불량", "통화잡음",
      "데이터연결불가", "데이터안됨",
      "먹통"
  );

  // === 유심 ===
  private static final Set<String> USIM_KEYWORDS = Set.of(
      "유심", "나노유심", "물리유심",
      "유심정보", "유심등록", "유심카드"
  );

  // === IPTV / OTT ===
  private static final Set<String> OTT_KEYWORDS = Set.of(
      "iptv", "vod",
      "티빙", "티빙플러스",
      "웨이브", "웨이브플러스",
      "디즈니플러스", "디즈니",
      "넷플릭스", "넷플",
      "유튜브프리미엄",
      "아이들나라"
  );

  // === 인터넷 상품 ===
  private static final Set<String> INTERNET_KEYWORDS = Set.of(
      "기가인터넷", "기가슬림",
      "프리미엄안심500M", "프리미엄안심1G", "프리미엄안심보상1G",
      "백메가", "오백메가"
  );

  // === 요금제 ===
  private static final Set<String> PLAN_KEYWORDS = Set.of(
      "5G시그니처",
      "5G프리미어슈퍼", "5G프리미어플러스", "5G프리미어레귤러", "5G프리미어에센셜",
      "5G스탠다드", "5G심플플러스", "5G라이트플러스", "5G슬림플러스", "5G미니",
      "너겟", "너겟1GB", "너겟3GB", "너겟5GB", "너겟7GB", "너겟11GB", "너겟무제한",
      "유쓰", "5G유쓰스탠다드", "5G유쓰심플플러스",
      "5G청소년", "5G키즈", "5G시니어",
      "LTE슬림", "LTE프리미어", "LTE데이터33",
      "LTE청소년19", "LTE키즈22", "LTE시니어33",
      "현역병사", "현역병사데이터", "LTE표준"
  );

  // === 부가서비스 ===
  private static final Set<String> ADDON_KEYWORDS = Set.of(
      "폰케어", "유플러스폰케어", "스마트폰보험", "휴대폰보험",
      "로밍패스", "해외로밍", "데이터로밍",
      "로밍패스3GB", "로밍패스5GB", "로밍패스10GB", "로밍패스무제한",
      "데이터쉐어링",
      "뮤직벨링",
      "아이들나라서비스",
      "U+멤버십"
  );

  // === 결합상품 ===
  private static final Set<String> BUNDLE_KEYWORDS = Set.of(
      "U+투게더", "참쉬운가족결합", "가족무한사랑",
      "시니어가족결합", "헬로비전결합", "인터넷끼리결합"
  );

  // === 고객 등급 ===
  private static final Set<String> GRADE_KEYWORDS = Set.of(
      "vvip", "vip", "diamond",
      "브이브이아이피", "브이아이피", "다이아몬드", "우수고객"
  );

  // === 상담 채널 / 고객 유형 ===
  private static final Set<String> CHANNEL_KEYWORDS = Set.of(
      "전화상담", "채팅상담", "방문상담",
      "개인고객", "법인고객", "일반고객", "기업고객"
  );

  // === 기타 ===
  private static final Set<String> ETC_KEYWORDS = Set.of(
      "포인트적립", "포인트사용",
      "사은품", "경품",
      "알뜰폰", "알뜰요금제",
      "스마트홈", "맘카", "도어센서", "에어센서"
  );

  private static final Set<String> KEYWORD_DICT;

  static {
    Set<String> dict = new HashSet<>();
    dict.addAll(RISK_KEYWORDS);
    dict.addAll(CONTRACT_KEYWORDS);
    dict.addAll(INSTALL_KEYWORDS);
    dict.addAll(BILLING_KEYWORDS);
    dict.addAll(DEVICE_KEYWORDS);
    dict.addAll(NETWORK_KEYWORDS);
    dict.addAll(USIM_KEYWORDS);
    dict.addAll(OTT_KEYWORDS);
    dict.addAll(INTERNET_KEYWORDS);
    dict.addAll(PLAN_KEYWORDS);
    dict.addAll(ADDON_KEYWORDS);
    dict.addAll(BUNDLE_KEYWORDS);
    dict.addAll(GRADE_KEYWORDS);
    dict.addAll(CHANNEL_KEYWORDS);
    dict.addAll(ETC_KEYWORDS);
    KEYWORD_DICT = Collections.unmodifiableSet(dict);
  }

  public static boolean isKeyword(String token) {
    return KEYWORD_DICT.contains(token);
  }

  public static List<String> filter(List<String> tokens) {
    return tokens.stream()
        .filter(KEYWORD_DICT::contains)
        .distinct()
        .collect(Collectors.toList());
  }

  public static Set<String> getKeywordDict() {
    return KEYWORD_DICT;
  }
}
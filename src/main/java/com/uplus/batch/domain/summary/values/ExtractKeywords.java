package com.uplus.batch.domain.summary.values;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class ExtractKeywords {

  private ExtractKeywords() {}

  // =========================================================
  // 위험 감지
  // =========================================================
  private static final Set<String> RISK = Set.of(
      "폭언욕설","해지위험","사기의심","피싱해킹","반복민원","정책악용","보상요구","민원위험","타사언급"
  );

  // =========================================================
  // 계약 / 처리
  // =========================================================
  private static final Set<String> CONTRACT = Set.of(
      "번호이동","기기변경","재약정","신규가입","명의변경","개통취소","번호이동해지","약정만료해지",
      "해지재약정","자급제","선불폰","중고폰","가입","신규","교체","혜택","상품권","계약","약정"
  );

  // =========================================================
  // 설치 / 해지
  // =========================================================
  private static final Set<String> INSTALL = Set.of(
      "이전설치","신규설치","해지","해약","기간","설치","예약","모뎀","apn","단말","단말기","wi-fi"
  );

  // =========================================================
  // 요금 / 청구
  // =========================================================
  private static final Set<String> BILLING = Set.of(
      "공시지원금","지원금","할부","할부금","위약금","요금감면","미납요금","청구서","요금조회","요금제",
      "결합할인","분할납부","선택약정","요금납부","요금제변경","미납안내","이용정지","장기고객할인",
      "포인트적립","사은품","부담금","할인","보상"
  );

  // =========================================================
  // 단말기
  // =========================================================
  private static final Set<String> DEVICE = Set.of(
      "아이폰","갤럭시","갤럭시탭"
  );

  // =========================================================
  // 네트워크 / 장애
  // =========================================================
  private static final Set<String> NETWORK = Set.of(
      "네트워크","장애","5g","lte","와이파이","공유기","속도저하","연결불가","와이파이불안정",
      "셋톱박스오류","통화품질불량","데이터연결불가","인터넷장애","기사방문","원격점검", "원격"
  );

  // =========================================================
  // 유심
  // =========================================================
  private static final Set<String> USIM = Set.of("유심");

  // =========================================================
  // 요금제 상품
  // =========================================================
  private static final Set<String> PLAN = Set.of(
      "5G시그니처","5G프리미어슈퍼","5G프리미어플러스","5G프리미어레귤러","5G프리미어에센셜",
      "5G프리미어","5G스탠다드","5G심플플러스","5G라이트플러스","5G슬림플러스","5G미니",
      "너겟","유쓰","5G청소년","5G키즈","현역병사","요금제","서비스","부가서비스","모바일요금제",
      "사용량"
  );

  // =========================================================
  // 인터넷 상품
  // =========================================================
  private static final Set<String> INTERNET_PRODUCT = Set.of(
      "인터넷","홈상품","U+인터넷100M","U+인터넷500M","U+인터넷10G","프리미엄안심500M","프리미엄안심1G"
  );

  // =========================================================
  // IPTV / OTT
  // =========================================================
  private static final Set<String> IPTV_OTT = Set.of(
      "U+tv베이직","U+tv스탠다드","U+tv프리미엄","티빙","웨이브","디즈니플러스","넷플릭스","유튜브프리미엄","아이들나라"
  );

  // =========================================================
  // 부가서비스
  // =========================================================
  private static final Set<String> ADDON = Set.of(
      "U+폰케어","U+폰케어프리미엄","로밍패스","데이터쉐어링","U+멤버십","피싱해킹안심서비스","스팸전화알림"
  );

  // =========================================================
  // 결합
  // =========================================================
  private static final Set<String> BUNDLE = Set.of(
      "U+투게더","참쉬운가족결합","헬로비전결합"
  );

  // =========================================================
  // 스마트홈
  // =========================================================
  private static final Set<String> SMART_HOME = Set.of(
      "스마트홈","맘카"
  );

  // =========================================================
  // 고객 등급
  // =========================================================
  private static final Set<String> GRADE = Set.of(
      "vvip","vip","다이아몬드"
  );

  // =========================================================
  // 상담 채널 / 고객 유형
  // =========================================================
  private static final Set<String> CHANNEL_TYPE = Set.of(
      "전화상담","채팅상담","방문상담","개인고객","법인고객","불만","조치","품질","문제","도움","요청", "확인",
      "점검","필요","제공","정보","방문","이메일","채팅","장치","장비","주소","지원","파손","미납","안내","대응",
      "등록","출동","일정","접수"
  );

  // =========================================================
  // 브랜드
  // =========================================================
  private static final Set<String> BRAND = Set.of(
      "유플러스","헬로비전"
  );

  // =========================================================
  // 전체 dictionary
  // =========================================================
  private static final Set<String> KEYWORD_DICT;

  static {
    Set<String> dict = new HashSet<>();
    dict.addAll(RISK);
    dict.addAll(CONTRACT);
    dict.addAll(INSTALL);
    dict.addAll(BILLING);
    dict.addAll(DEVICE);
    dict.addAll(NETWORK);
    dict.addAll(USIM);
    dict.addAll(PLAN);
    dict.addAll(INTERNET_PRODUCT);
    dict.addAll(IPTV_OTT);
    dict.addAll(ADDON);
    dict.addAll(BUNDLE);
    dict.addAll(SMART_HOME);
    dict.addAll(GRADE);
    dict.addAll(CHANNEL_TYPE);
    dict.addAll(BRAND);
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
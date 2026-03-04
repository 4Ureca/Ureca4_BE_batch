package com.uplus.batch.jobs.daily_report.step.hourly_consult;

import lombok.*;

import java.io.Serializable;
import java.util.List;

/**
 * 시간대별 이슈 트렌드 집계 결과 DTO
 *
 * daily_report_snapshot에 저장되는 구조:
 * - timeSlotTrend[]: 슬롯별 카테고리·키워드 분석
 * - keywordSummary: 전체 키워드 분석 + 고객 유형별
 */
@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class HourlyConsultResult implements Serializable {

    private static final long serialVersionUID = 1L;

    private int totalConsultCount;
    private double avgDurationMinutes;
    private List<TimeSlotResult> timeSlotTrend;
    private KeywordSummary keywordSummary;

    // ── 시간대별 집계 ──

    @Getter
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class TimeSlotResult implements Serializable {
        private static final long serialVersionUID = 1L;

        private String slot;              // "09-12", "12-15", "15-18"
        private int consultCount;
        private double avgDuration;       // 분 단위
        private List<CategoryBreakdown> categoryBreakdown;
        private KeywordAnalysis keywordAnalysis;
    }

    @Getter
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CategoryBreakdown implements Serializable {
        private static final long serialVersionUID = 1L;

        private String code;              // FEE, DEV, TRB, CHN, ADD, ETC
        private String name;              // "요금/납부", "기기변경" 등
        private int count;
        private double rate;              // 해당 시간대 대비 비율(%)
    }

    // ── 키워드 분석 (슬롯별) ──

    @Getter
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class KeywordAnalysis implements Serializable {
        private static final long serialVersionUID = 1L;

        private List<TopKeyword> topKeywords;
        private List<NewKeyword> newKeywords;
    }

    @Getter
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class TopKeyword implements Serializable {
        private static final long serialVersionUID = 1L;

        private String keyword;
        private long count;
        private int rank;
        private double changeRate;        // 전일 같은 슬롯 대비 증감율(%)
    }

    @Getter
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class NewKeyword implements Serializable {
        private static final long serialVersionUID = 1L;

        private String keyword;
        private long count;
    }

    // ── 전체 키워드 분석 ──

    @Getter
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class KeywordSummary implements Serializable {
        private static final long serialVersionUID = 1L;

        private List<TopKeyword> topKeywords;
        private List<CustomerTypeKeyword> byCustomerType;
    }

    @Getter
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CustomerTypeKeyword implements Serializable {
        private static final long serialVersionUID = 1L;

        private String customerType;      // "VVIP", "VIP", "DIAMOND" 등
        private List<String> keywords;
    }
}

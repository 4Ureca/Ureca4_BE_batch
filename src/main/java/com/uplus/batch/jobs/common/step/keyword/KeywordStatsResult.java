package com.uplus.batch.jobs.common.step.keyword;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class KeywordStatsResult {

    private List<TopKeyword> topKeywords;
    private List<LongTermKeyword> longTermTopKeywords;
    private List<CustomerTypeKeyword> byCustomerType;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class TopKeyword {
        private String keyword;
        private long count;
        private int rank;
        private double changeRate;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class LongTermKeyword {
        private String keyword;
        private long count;
        private int rank;
        private int appearanceDays;
        private int totalDays;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CustomerTypeKeyword {
        private String customerType;
        private List<CustomerKeywordCount> keywords;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CustomerKeywordCount {
        private String keyword;
        private long count;
    }
}

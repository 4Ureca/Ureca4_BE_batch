package com.uplus.batch.jobs.weekly_report.step.performance;

import lombok.*;

import java.io.Serializable;
import java.util.List;

/**
 * 전체 상담 성과 집계 결과 DTO
 *
 * weekly_report_snapshot / monthly_report_snapshot에 저장되는 구조:
 * - 전체 요약: 총 상담수, 상담사 평균처리, 평균소요시간, 고객만족도
 * - 상담사별 성과: TOP10 순위
 */
@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PerformanceResult implements Serializable {

    private static final long serialVersionUID = 1L;

    private int totalConsultCount;
    private double avgConsultCountPerAgent;
    private double avgDurationMinutes;
    private double avgSatisfiedScore;
    private List<AgentPerformance> agentPerformance;

    @Getter
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class AgentPerformance implements Serializable {
        private static final long serialVersionUID = 1L;

        private long agentId;
        private String agentName;
        private int consultCount;
        private double avgDurationMinutes;
        private double avgSatisfiedScore;
        private Double qualityScore;  // 추후 구현 — 현재 null
    }
}

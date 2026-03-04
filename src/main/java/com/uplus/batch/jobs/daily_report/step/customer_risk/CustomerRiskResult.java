package com.uplus.batch.jobs.daily_report.step.customer_risk;

import lombok.*;

import java.io.Serializable;

/**
 * 고객 특이사항 집계 결과 DTO (daily 전용)
 *
 * - CustomerRiskTasklet에서 집계 후 생성
 * - ExecutionContext를 통해 DailySnapshotWriter로 전달
 * - 월별 리포트에는 customerRiskAnalysis 미포함 (daily에서만 알림)
 *
 */
@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CustomerRiskResult implements Serializable {

    private static final long serialVersionUID = 1L;

    // ─── customerRiskAnalysis 필드 (정의서 기준 7종) ───

    /** 사기 의심 상담 건수 (FRAUD) */
    private int fraudSuspect;

    /** 악성 민원 상담 건수 (ABUSE) */
    private int maliciousComplaint;

    /** 정책 악용 상담 건수 (POLICY) */
    private int policyAbuse;

    /** 과도한 보상 요구 건수 (COMP) */
    private int excessiveCompensation;

    /** 반복 민원 건수 (REPEAT) */
    private int repeatedComplaint;

    /** 피싱 피해 건수 (PHISHING) */
    private int phishingVictim;

    /** 해지 위험 건수 — riskFlags CHURN 태깅 기반 (AI/상담사 판단) */
    private int churnRisk;

    // ─── 편의 메서드 ───

    /** 특이사항 총 건수 (CHURN 포함 7종) */
    public int getTotalRiskCount() {
        return fraudSuspect + maliciousComplaint + policyAbuse
                + excessiveCompensation + repeatedComplaint + phishingVictim
                + churnRisk;
    }
}
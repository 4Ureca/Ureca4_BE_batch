package com.uplus.batch.jobs.customer_policy_dummy.tasklet;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class CustomerPolicyDummyTasklet implements Tasklet {

    private final JdbcTemplate jdbcTemplate;

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) {

        upsert(
            "ABUSE",
            "ABUSE_V1_BASE",
            "폭언 고객 기본 대응 가이드",
            "폭언 발생 시 단계별 대응 절차",
            1,
            "<h3>기본 대응 원칙</h3>\n" +
                "<ol>\n" +
                "<li>감정적 대응 금지</li>\n" +
                "<li>중립적 어조 유지</li>\n" +
                "<li>1차 경고 후 지속 시 상담 종료 가능</li>\n" +
                "</ol>\n" +
                "<p><strong>유의사항:</strong> 모든 발언은 상담 기록에 명확히 기재합니다.</p>",
            "[{\"question\":\"왜 이렇게 일처리를 못해\",\"answer\":\"불편을 드려 죄송합니다. 정확히 확인 후 안내드리겠습니다.\"}," +
                "{\"question\":\"말이 안 통하네\",\"answer\":\"다시 정리하여 정확히 설명드리겠습니다.\"}]"
        );

        upsert(
            "ABUSE",
            "ABUSE_V1_LEGAL",
            "협박성 발언 대응 지침",
            "법적 조치 가능성 안내 기준",
            2,
            "<h3>협박성 발언 발생 시</h3>\n" +
                "<ol>\n" +
                "<li>즉시 녹취 고지</li>\n" +
                "<li>관리자 보고</li>\n" +
                "<li>필요 시 법적 절차 안내</li>\n" +
                "</ol>\n" +
                "<p><strong>신변 위협</strong> 또는 방문 협박 발언 시 즉시 에스컬레이션합니다.</p>",
            "[{\"question\":\"내가 찾아간다\",\"answer\":\"해당 발언은 법적 문제로 이어질 수 있으며 통화는 녹음되고 있습니다.\"}]"
        );

        upsert(
            "FRAUD",
            "FRAUD_V1_VERIFY",
            "사기 의심 본인확인 강화",
            "명의 도용 의심 시 인증 강화",
            2,
            "<h3>추가 인증 절차</h3>\n" +
                "<ol>\n" +
                "<li>2단계 본인 인증 진행</li>\n" +
                "<li>기존 가입 정보 교차 확인</li>\n" +
                "<li>이상 패턴 기록</li>\n" +
                "</ol>\n" +
                "<p>의심 사례는 보안팀에 공유합니다.</p>",
            "[{\"question\":\"왜 확인을 또 해요?\",\"answer\":\"고객 정보 보호를 위한 필수 절차입니다.\"}]"
        );

        upsert(
            "FRAUD",
            "FRAUD_V1_REPORT",
            "사기 신고 안내",
            "금융기관 및 경찰 신고 절차 안내",
            1,
            "<h3>피해 발생 시 조치</h3>\n" +
                "<ul>\n" +
                "<li>즉시 금융기관 신고</li>\n" +
                "<li>경찰서 또는 사이버수사대 신고</li>\n" +
                "<li>비밀번호 변경 및 2차 인증 설정</li>\n" +
                "</ul>",
            "[{\"question\":\"돈이 빠져나갔어요.\",\"answer\":\"추가 피해 방지를 위해 즉시 금융기관과 경찰에 신고하시기 바랍니다.\"}]"
        );

        upsert(
            "POLICY",
            "POLICY_V1_LIMIT",
            "혜택 중복 제한 지침",
            "중복 혜택 요구 차단 기준",
            1,
            "<h3>중복 적용 제한 기준</h3>\n" +
                "<ol>\n" +
                "<li>동일 프로모션 중복 불가</li>\n" +
                "<li>약관 기준 우선 적용</li>\n" +
                "</ol>\n" +
                "<p>고객에게 약관 조항을 명확히 안내합니다.</p>",
            "[{\"question\":\"왜 또 할인 안 돼요?\",\"answer\":\"약관상 동일 혜택은 중복 적용이 어렵습니다.\"}]"
        );

        upsert(
            "COMP",
            "COMP_V1_RULE",
            "보상 기준 안내",
            "보상 한도 기준 안내",
            1,
            "<h3>보상 처리 원칙</h3>\n" +
                "<ol>\n" +
                "<li>내부 보상 기준 범위 내 처리</li>\n" +
                "<li>초과 금액은 관리자 승인 필요</li>\n" +
                "</ol>\n" +
                "<p>감정 대응 없이 기준 중심 설명을 유지합니다.</p>",
            "[{\"question\":\"50만원은 줘야죠.\",\"answer\":\"내부 기준 범위 내에서 지원 가능합니다.\"}]"
        );

        upsert(
            "REPEAT",
            "REPEAT_V1_BASE",
            "반복 민원 기본 대응",
            "동일 이슈 반복 접수 대응",
            0,
            "<h3>반복 접수 관리</h3>\n" +
                "<ol>\n" +
                "<li>기존 처리 이력 확인</li>\n" +
                "<li>동일 답변 유지</li>\n" +
                "<li>재설명 요청 시 핵심 요약 제공</li>\n" +
                "</ol>",
            "[{\"question\":\"이거 네 번째 전화야.\",\"answer\":\"이전 상담 내역을 모두 확인 후 정확히 안내드리겠습니다.\"}]"
        );

        upsert(
            "PHISHING",
            "PHISHING_V1_BASE",
            "피싱 피해 기본 대응",
            "피싱 피해 접수 및 보호 절차",
            2,
            "<h3>피싱 피해 대응 절차</h3>\n" +
                "<ol>\n" +
                "<li>즉시 서비스 차단</li>\n" +
                "<li>비밀번호 변경 안내</li>\n" +
                "<li>금융기관 신고 안내</li>\n" +
                "</ol>\n" +
                "<p><strong>긴급:</strong> 추가 피해 방지를 우선합니다.</p>",
            "[{\"question\":\"문자 링크 눌렀어요.\",\"answer\":\"즉시 비밀번호를 변경하시고 금융기관에 신고하시기 바랍니다.\"}]"
        );

        return RepeatStatus.FINISHED;
    }

    private void upsert(
        String typeCode,
        String policyCode,
        String policyTitle,
        String policySummary,
        int severity,
        String content,
        String scriptJson
    ) {
        jdbcTemplate.update(
            """
            INSERT INTO customer_policy
            (type_code, policy_code, policy_title, policy_summary, severity, content, script_json, display_start_at, display_end_at, is_active)
            VALUES (?, ?, ?, ?, ?, ?, CAST(? AS JSON), NULL, NULL, TRUE)
            ON DUPLICATE KEY UPDATE
                type_code = VALUES(type_code),
                policy_title = VALUES(policy_title),
                policy_summary = VALUES(policy_summary),
                severity = VALUES(severity),
                content = VALUES(content),
                script_json = VALUES(script_json),
                display_start_at = VALUES(display_start_at),
                display_end_at = VALUES(display_end_at),
                is_active = VALUES(is_active)
            """,
            typeCode,
            policyCode,
            policyTitle,
            policySummary,
            severity,
            content,
            scriptJson
        );
    }
}

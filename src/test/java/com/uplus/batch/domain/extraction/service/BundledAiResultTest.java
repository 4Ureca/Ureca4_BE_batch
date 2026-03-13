package com.uplus.batch.domain.extraction.service;

import com.uplus.batch.domain.extraction.dto.AiExtractionResponse;
import com.uplus.batch.domain.extraction.dto.BundledAiResult;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.*;

class BundledAiResultTest {

    @Test
    @DisplayName("isSuccess: rawSummary가 정상이면 true를 반환한다")
    void isSuccess_ValidSummary_ReturnsTrue() {
        BundledAiResult result = new BundledAiResult(
                0, 1L, "M_CHN_04",
                true, "요금 불만", true, false,
                List.of("할인 제안"),
                "고객이 요금 불만으로 해지 요청, 할인 제안 후 재약정 유도"
        );
        assertThat(result.isSuccess()).isTrue();
    }

    @Test
    @DisplayName("isSuccess: rawSummary가 null이면 false를 반환한다")
    void isSuccess_NullSummary_ReturnsFalse() {
        BundledAiResult result = new BundledAiResult(
                0, 1L, "M_CHN_04",
                false, null, false, false, List.of(), null
        );
        assertThat(result.isSuccess()).isFalse();
    }

    @Test
    @DisplayName("isSuccess: rawSummary가 빈 문자열이면 false를 반환한다")
    void isSuccess_BlankSummary_ReturnsFalse() {
        BundledAiResult result = new BundledAiResult(
                0, 1L, "M_CHN_04",
                false, null, false, false, List.of(), "   "
        );
        assertThat(result.isSuccess()).isFalse();
    }

    @Test
    @DisplayName("isSuccess: rawSummary에 'AI 분석 누락'이 포함되면 false를 반환한다")
    void isSuccess_FallbackMarker_ReturnsFalse() {
        BundledAiResult result = new BundledAiResult(
                0, 1L, "M_CHN_04",
                false, null, false, false, List.of(), "[AI 분석 누락]"
        );
        assertThat(result.isSuccess()).isFalse();
    }

    @Test
    @DisplayName("toAiExtractionResponse: 필드가 올바르게 변환된다")
    void toAiExtractionResponse_MapsFieldsCorrectly() {
        List<String> actions = List.of("할인 제안", "위약금 면제");
        BundledAiResult result = new BundledAiResult(
                0, 1L, "M_CHN_04",
                true, "요금 불만", true, false,
                actions,
                "고객이 해지 요청, 할인 제안 후 재약정"
        );

        AiExtractionResponse response = result.toAiExtractionResponse();

        assertThat(response.has_intent()).isTrue();
        assertThat(response.complaint_reason()).isEqualTo("요금 불만");
        assertThat(response.defense_attempted()).isTrue();
        assertThat(response.defense_success()).isFalse();
        assertThat(response.defense_actions()).containsExactlyElementsOf(actions);
        assertThat(response.raw_summary()).isEqualTo("고객이 해지 요청, 할인 제안 후 재약정");
    }

    @Test
    @DisplayName("toAiExtractionResponse: defenseActions가 null이면 빈 리스트로 변환된다")
    void toAiExtractionResponse_NullActions_ReturnsEmptyList() {
        BundledAiResult result = new BundledAiResult(
                0, 1L, "M_FEE_01",
                false, null, false, false,
                null,
                "고객 요금 문의 처리 완료"
        );

        AiExtractionResponse response = result.toAiExtractionResponse();

        assertThat(response.defense_actions()).isEmpty();
    }
}

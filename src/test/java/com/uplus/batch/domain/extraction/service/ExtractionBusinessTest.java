package com.uplus.batch.domain.extraction.service;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.BDDMockito.*;
import static org.assertj.core.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.uplus.batch.domain.extraction.dto.BundledAiResult;
import com.uplus.batch.domain.extraction.entity.EventStatus;
import com.uplus.batch.domain.extraction.entity.ResultEventStatus;
import com.uplus.batch.domain.extraction.repository.ConsultationExtractionRepository;
import com.uplus.batch.domain.extraction.repository.ConsultationRawTextRepository;
import com.uplus.batch.domain.extraction.repository.EventStatusRepository;
import com.uplus.batch.domain.extraction.schedular.ExtractionScheduler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;

@ExtendWith(MockitoExtension.class)
class ExtractionBusinessTest {

    @Mock private EventStatusRepository eventRepository;
    @Mock private ConsultationRawTextRepository rawTextRepository;
    @Mock private ConsultationExtractionRepository extractionRepository;
    @Mock private BundledGeminiExtractor bundledExtractor;

    @Spy private ObjectMapper objectMapper = new ObjectMapper();

    @InjectMocks
    private ExtractionScheduler extractionScheduler;

    private ResultEventStatus testTask;

    @BeforeEach
    void setUp() {
        testTask = ResultEventStatus.builder()
                .consultId(100L)
                .categoryCode("M_CHN_04")
                .build();
    }

    @Test
    @DisplayName("성공 케이스: BundledAiResult가 정상이면 COMPLETED로 저장된다")
    void saveExtractionResult_Success() {
        // Given
        BundledAiResult aiResult = new BundledAiResult(
                0, 100L, "M_CHN_04",
                true, "비싼 요금제", true, false,
                List.of("할인 제안"),
                "고객이 요금 불만으로 해지 요청, 할인 제안 후 재약정 유도",
                null, null, null, null, null
        );
        given(eventRepository.saveAndFlush(any())).willReturn(testTask);

        // When
        extractionScheduler.saveExtractionResult(testTask, aiResult);

        // Then
        assertThat(testTask.getStatus()).isEqualTo(EventStatus.COMPLETED);
        verify(extractionRepository, times(1)).save(any());
        verify(eventRepository, atLeastOnce()).saveAndFlush(testTask);
    }

    @Test
    @DisplayName("실패 케이스: aiResult가 null이면 FAILED로 처리된다")
    void saveExtractionResult_NullAiResult() {
        // Given
        given(eventRepository.saveAndFlush(any())).willReturn(testTask);

        // When
        extractionScheduler.saveExtractionResult(testTask, null);

        // Then
        assertThat(testTask.getStatus()).isEqualTo(EventStatus.FAILED);
        verify(extractionRepository, never()).save(any());
    }

    @Test
    @DisplayName("실패 케이스: rawSummary가 비어있으면 FAILED로 처리된다")
    void saveExtractionResult_EmptyRawSummary() {
        // Given
        BundledAiResult aiResult = new BundledAiResult(
                0, 100L, "M_CHN_04",
                false, null, false, false,
                List.of(), "",
                null, null, null, null, null
        );
        given(eventRepository.saveAndFlush(any())).willReturn(testTask);

        // When
        extractionScheduler.saveExtractionResult(testTask, aiResult);

        // Then
        assertThat(testTask.getStatus()).isEqualTo(EventStatus.FAILED);
        // isSuccess()=false → 첫 번째 guard에서 catch
        assertThat(testTask.getFailReason()).contains("AI 응답 없음 또는 raw_summary 비어있음");
    }
}
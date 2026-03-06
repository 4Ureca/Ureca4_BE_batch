package com.uplus.batch.domain.extraction.service;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.BDDMockito.*;
import static org.assertj.core.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.uplus.batch.domain.extraction.dto.AiExtractionResponse;
import com.uplus.batch.domain.extraction.entity.ConsultationRawText;
import com.uplus.batch.domain.extraction.entity.EventStatus;
import com.uplus.batch.domain.extraction.entity.ResultEventStatus;
import com.uplus.batch.domain.extraction.repository.ConsultationExtractionRepository;
import com.uplus.batch.domain.extraction.repository.ConsultationRawTextRepository;
import com.uplus.batch.domain.extraction.repository.EventStatusRepository;
import com.uplus.batch.domain.extraction.schedular.ExtractionScheduler;
import com.uplus.batch.domain.extraction.service.ConsultationExtractionManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Optional;

@ExtendWith(MockitoExtension.class)
class ExtractionBusinessTest {

    @Mock private EventStatusRepository eventRepository;
    @Mock private ConsultationRawTextRepository rawTextRepository;
    @Mock private ConsultationExtractionRepository extractionRepository;
    @Mock private ConsultationExtractionManager extractionManager;
    
    @Spy private ObjectMapper objectMapper = new ObjectMapper();

    @InjectMocks
    private ExtractionScheduler extractionScheduler;

    private ResultEventStatus testTask;

    @BeforeEach
    void setUp() {
        testTask = ResultEventStatus.builder()
                .consultId(100L)
                .categoryCode("CHN_RETENTION")
                .build();
    }

    @Test
    @DisplayName("성공 케이스: AI 추출 후 결과가 정상적으로 DB에 저장되어야 한다")
    void processIndividualTask_Success() throws Exception {
        // Given
        ConsultationRawText mockRaw = mock(ConsultationRawText.class);
        given(mockRaw.getRawTextJson()).willReturn("{\"text\": \"해지하고 싶어요\"}");
        given(rawTextRepository.findByConsultId(100L)).willReturn(Optional.of(mockRaw));

        AiExtractionResponse mockAiRes = new AiExtractionResponse(
                true, "비싼 요금제", true, false, List.of("할인 제안"), "고객이 요금 불만으로 해지 요청"
        );
        given(extractionManager.runExtraction(anyString(), anyString())).willReturn(mockAiRes);

        // When
        extractionScheduler.processIndividualTask(testTask);

        // Then
        assertThat(testTask.getStatus()).isEqualTo(EventStatus.COMPLETED);
        verify(extractionRepository, times(1)).save(any());
        verify(eventRepository, atLeastOnce()).saveAndFlush(testTask);
    }

    @Test
    @DisplayName("실패 케이스: 원본 데이터가 없는 경우 FAILED 상태로 업데이트 되어야 한다")
    void processIndividualTask_RawDataNotFound() {
        // Given
        given(rawTextRepository.findByConsultId(100L)).willReturn(Optional.empty());

        // When
        extractionScheduler.processIndividualTask(testTask);

        // Then
        assertThat(testTask.getStatus()).isEqualTo(EventStatus.FAILED);
        assertThat(testTask.getFailReason()).contains("원본 대화 데이터를 찾을 수 없습니다");
        assertThat(testTask.getRetryCount()).isEqualTo(1);
    }

    @Test
    @DisplayName("실패 케이스: AI 응답의 raw_summary가 비어있으면 예외가 발생하며 실패 처리된다")
    void processIndividualTask_EmptyAiResponse() {
        // 1. Given: 원본 데이터는 존재하지만 (null이 아니어야 함)
        ConsultationRawText mockRaw = mock(ConsultationRawText.class);
        given(mockRaw.getRawTextJson()).willReturn("{\"some\":\"json\"}"); // null이 아닌 값 설정
        given(rawTextRepository.findByConsultId(100L)).willReturn(Optional.of(mockRaw));

        // 2. Given: AI 응답 객체는 오지만 내용이 비어있는 상황 설정
        AiExtractionResponse emptyRes = new AiExtractionResponse(
                false, null, false, false, List.of(), "" // raw_summary가 빈 값
        );
        
        // any()를 사용하여 null 여부와 관계없이 매칭되도록 설정 (Stubbing Mismatch 방지)
        given(extractionManager.runExtraction(any(), any())).willReturn(emptyRes);

        // When
        extractionScheduler.processIndividualTask(testTask);

        // Then
        assertThat(testTask.getStatus()).isEqualTo(EventStatus.FAILED);
        assertThat(testTask.getFailReason()).contains("AI가 내용을 생성하지 못했습니다");
    }
}
package com.uplus.batch.domain.extraction.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.*;
import java.util.concurrent.Executor;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.uplus.batch.domain.extraction.dto.AiExtractionResponse;
import com.uplus.batch.domain.extraction.dto.QualityScoringResponse;
import com.uplus.batch.domain.extraction.entity.*;
import com.uplus.batch.domain.extraction.repository.*;
import com.uplus.batch.domain.extraction.service.ConsultationAnalysisManager.TaskPair;

@ExtendWith(MockitoExtension.class)
class ConsultationAnalysisManagerTest {

    @InjectMocks
    private ConsultationAnalysisManager manager;

    @Mock private GeminiExtractor geminiExtractor;
    @Mock private GeminiQualityScorer geminiQualityScorer;
    @Mock private ConsultationRawTextRepository rawTextRepository;
    @Mock private ConsultationExtractionRepository extractionRepository;
    @Mock private ConsultationEvaluationRepository evaluationRepository;
    @Mock private ResultEventStatusRepository resultEventRepository;
    @Mock private ExcellentEventStatusRepository excellentEventRepository;
    @Mock private ManualRepository manualRepository;
    @Mock private AnalysisCodeRepository analysisCodeRepository;

    @Spy private ObjectMapper objectMapper = new ObjectMapper();

    // 🥊 람다(Runnable::run) 대신 익명 클래스를 사용하여 Mockito Proxy 에러 해결!
    @Spy 
    private Executor geminiTaskExecutor = new Executor() {
        @Override
        public void execute(Runnable command) {
            command.run(); // 즉시 실행하여 동기식으로 테스트 진행
        }
    };

    @Test
    @DisplayName("성공: 모든 상담이 정상적으로 분석되고 저장된다")
    void processIntegratedBundledTasks_Success() throws Exception {
        ResultEventStatus summary = ResultEventStatus.builder().consultId(100L).categoryCode("M_CHN_01").build();
        ExcellentEventStatus scoring = ExcellentEventStatus.builder().consultId(100L).build();
        List<TaskPair> taskPairs = List.of(new TaskPair(summary, scoring));

        ConsultationRawText mockRaw = mock(ConsultationRawText.class);
        given(mockRaw.getConsultId()).willReturn(100L);
        given(mockRaw.getRawTextJson()).willReturn("{\"chat\": \"정상 상담\"}");
        given(rawTextRepository.findAllByConsultIdIn(anyList())).willReturn(List.of(mockRaw));
        
        given(manualRepository.findByCategoryCodeAndIsActiveTrue(anyString())).willReturn(Optional.of(mock(Manual.class)));
        given(analysisCodeRepository.findAllByClassificationIn(anyList())).willReturn(Collections.emptyList());

        given(geminiExtractor.extractBatch(anyList(), anyString(), anyMap()))
                .willReturn(List.of(mock(AiExtractionResponse.class)));
        given(geminiQualityScorer.evaluate(anyString(), anyString()))
                .willReturn(new QualityScoringResponse(90, "Good", true));

        manager.processIntegratedBundledTasks(taskPairs);

        assertThat(summary.getStatus()).isEqualTo(EventStatus.COMPLETED);
        assertThat(scoring.getStatus()).isEqualTo(EventStatus.COMPLETED);
        verify(extractionRepository, times(1)).save(any());
    }

    @Test
    @DisplayName("격리 성공: 1번은 성공하고 2번 채점만 실패했을 때, 전체가 FAILED 되지 않고 각각의 결과가 반영된다")
    void processIntegratedBundledTasks_IndividualIsolation() throws Exception {
        ResultEventStatus s1 = ResultEventStatus.builder().consultId(1L).categoryCode("CHN").build();
        ExcellentEventStatus e1 = ExcellentEventStatus.builder().consultId(1L).build();
        ResultEventStatus s2 = ResultEventStatus.builder().consultId(2L).categoryCode("CHN").build();
        ExcellentEventStatus e2 = ExcellentEventStatus.builder().consultId(2L).build();
        
        List<TaskPair> taskPairs = List.of(new TaskPair(s1, e1), new TaskPair(s2, e2));

        ConsultationRawText r1 = mock(ConsultationRawText.class);
        given(r1.getConsultId()).willReturn(1L);
        given(r1.getRawTextJson()).willReturn("{}");
        ConsultationRawText r2 = mock(ConsultationRawText.class);
        given(r2.getConsultId()).willReturn(2L);
        given(r2.getRawTextJson()).willReturn("{}");

        given(rawTextRepository.findAllByConsultIdIn(anyList())).willReturn(List.of(r1, r2));
        given(manualRepository.findByCategoryCodeAndIsActiveTrue(anyString())).willReturn(Optional.of(mock(Manual.class)));
        given(analysisCodeRepository.findAllByClassificationIn(anyList())).willReturn(Collections.emptyList());

        given(geminiExtractor.extractBatch(anyList(), anyString(), anyMap()))
                .willReturn(List.of(mock(AiExtractionResponse.class), mock(AiExtractionResponse.class)));

        given(geminiQualityScorer.evaluate(anyString(), anyString()))
                .willReturn(new QualityScoringResponse(90, "Good", true))
                .willThrow(new RuntimeException("Scoring API Error"));

        manager.processIntegratedBundledTasks(taskPairs);

        assertThat(s1.getStatus()).isEqualTo(EventStatus.COMPLETED);
        assertThat(e1.getStatus()).isEqualTo(EventStatus.COMPLETED);
        assertThat(s2.getStatus()).isEqualTo(EventStatus.FAILED);
        assertThat(e2.getStatus()).isEqualTo(EventStatus.FAILED);
    }
}
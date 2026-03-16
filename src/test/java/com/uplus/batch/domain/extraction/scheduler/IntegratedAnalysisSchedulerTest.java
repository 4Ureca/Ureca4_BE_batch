package com.uplus.batch.domain.extraction.scheduler;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.BDDMockito.*;
import static org.mockito.Mockito.*;

import java.util.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.Pageable;

import com.uplus.batch.domain.extraction.entity.*;
import com.uplus.batch.domain.extraction.repository.*;
import com.uplus.batch.domain.extraction.service.ConsultationAnalysisManager;

@ExtendWith(MockitoExtension.class)
class IntegratedAnalysisSchedulerTest {

    @InjectMocks
    private IntegratedAnalysisScheduler scheduler;

    @Mock private ResultEventStatusRepository resultEventRepository;
    @Mock private ExcellentEventStatusRepository excellentEventRepository;
    @Mock private ConsultationAnalysisManager analysisManager;

    @Test
    @DisplayName("성공: 요약/채점 짝꿍이 있는 데이터만 매니저에게 전달한다")
    void executeIntegratedAnalysis_Success() {
        // [Given] 🥊 consultId 300L로 짝을 맞춤
        ResultEventStatus summary = ResultEventStatus.builder().consultId(300L).categoryCode("OTB").build();
        ExcellentEventStatus scoring = ExcellentEventStatus.builder().consultId(300L).build();

        given(resultEventRepository.findReadyToProcessPairs(any(Pageable.class))).willReturn(List.of(summary));
        given(excellentEventRepository.findByConsultId(300L)).willReturn(Optional.of(scoring));

        // [When]
        scheduler.executeIntegratedAnalysis();

        // [Then]
        verify(analysisManager, times(1)).processIntegratedBundledTasks(anyList());
    }

    @Test
    @DisplayName("실패: 채점 데이터가 없는 미아 데이터는 매니저에게 전달하지 않는다")
    void executeIntegratedAnalysis_NoPair_Skip() {
        // [Given] 🥊 요약은 있는데 채점이 없음
        ResultEventStatus summary = ResultEventStatus.builder().consultId(400L).categoryCode("CHN").build();

        given(resultEventRepository.findReadyToProcessPairs(any(Pageable.class))).willReturn(List.of(summary));
        given(excellentEventRepository.findByConsultId(400L)).willReturn(Optional.empty());

        // [When]
        scheduler.executeIntegratedAnalysis();

        // [Then] 🥊 짝이 안 맞으므로 매니저 호출 안 함
        verify(analysisManager, never()).processIntegratedBundledTasks(anyList());
    }
}
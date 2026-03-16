package com.uplus.batch.domain.extraction.scheduler;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.time.LocalDateTime;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.uplus.batch.domain.extraction.entity.EventStatus;
import com.uplus.batch.domain.extraction.entity.ResultEventStatus;
import com.uplus.batch.domain.extraction.repository.ExcellentEventStatusRepository;
import com.uplus.batch.domain.extraction.repository.ResultEventStatusRepository;

@ExtendWith(MockitoExtension.class)
class IntegratedRetrySchedulerTest {

    @InjectMocks
    private IntegratedRetryScheduler retryScheduler;

    @Mock private ResultEventStatusRepository resultEventRepository;
    @Mock private ExcellentEventStatusRepository excellentEventRepository;

    @Test
    @DisplayName("성공: 좀비 데이터 정리 및 실패 건 재시도 로직이 호출된다")
    void retryAndRecoverTasks_Success() {
        // [Given] 실패한 요약 데이터 1건 시뮬레이션
        ResultEventStatus failedTask = spy(ResultEventStatus.builder().consultId(500L).categoryCode("CHN").build());
        failedTask.fail("에러");
        
        when(resultEventRepository.findByStatusAndRetryCountLessThan(eq(EventStatus.FAILED), anyInt()))
                .thenReturn(List.of(failedTask));

        // [When]
        retryScheduler.retryAndRecoverTasks();

        // [Then] 🥊
        verify(resultEventRepository, times(1)).cleanupStaleProcessingTasks(any(LocalDateTime.class));
        verify(excellentEventRepository, times(1)).cleanupStaleProcessingTasks(any(LocalDateTime.class));
        verify(failedTask, times(1)).retry(); // 상태가 REQUESTED로 돌아갔는지 확인
    }
}
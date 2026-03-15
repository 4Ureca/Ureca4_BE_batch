package com.uplus.batch.historical;

import com.uplus.batch.synthetic.SyntheticConsultationFactory;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.time.LocalDate;
import java.util.List;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.BDDMockito.*;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class HistoricalBatchServiceTest {

    @Mock private HistoricalBatchProperties properties;
    @Mock private HistoricalBatchRepository checkpointRepo;
    @Mock private SyntheticConsultationFactory consultationFactory;

    @InjectMocks
    private HistoricalBatchService batchService;

    /** 공통 stub — 각 테스트에서 필요한 것만 override 가능 */
    void setUpDefault() {
        given(properties.getStartDate()).willReturn(LocalDate.of(2026, 1, 1));
        given(properties.getEndDate()).willReturn(LocalDate.of(2026, 1, 3));
        given(properties.getDailyCount()).willReturn(10);
        given(properties.getChunkSize()).willReturn(5);
    }

    @Test
    @DisplayName("isRunning: 초기 상태는 false여야 한다")
    void isRunning_InitiallyFalse() {
        assertThat(batchService.isRunning()).isFalse();
    }

    @Test
    @DisplayName("runBatch: pending 날짜가 없으면 성공 0일로 완료된다")
    void runBatch_NoPendingDates() {
        // Given
        setUpDefault();
        given(checkpointRepo.findPendingOrFailedDates()).willReturn(List.of());

        // When
        String result = batchService.runBatch();

        // Then
        assertThat(result).contains("성공: 0일");
        assertThat(batchService.isRunning()).isFalse();
        // initDate는 startDate~endDate 3일치 호출 (1/1, 1/2, 1/3)
        verify(checkpointRepo, times(3)).initDate(any(), eq(10));
    }

    @Test
    @DisplayName("runBatch: 날짜 처리 성공 시 COMPLETED 마킹이 호출된다")
    void runBatch_SuccessfulDate() {
        // Given: dailyCount=5, chunkSize=10 → 루프 1회 (5건이 한번에 처리)
        given(properties.getStartDate()).willReturn(LocalDate.of(2026, 1, 1));
        given(properties.getEndDate()).willReturn(LocalDate.of(2026, 1, 1));
        given(properties.getDailyCount()).willReturn(5);
        given(properties.getChunkSize()).willReturn(10);

        LocalDate targetDate = LocalDate.of(2026, 1, 1);
        given(checkpointRepo.findPendingOrFailedDates()).willReturn(List.of(targetDate));

        SyntheticConsultationFactory.BatchResult mockResult =
                new SyntheticConsultationFactory.BatchResult(
                        List.of(1L, 2L, 3L, 4L, 5L),
                        List.of("M_CHN_04", "M_FEE_01", "M_TRB_01", "M_CHN_01", "M_FEE_02"));
        given(consultationFactory.executeStep1WithDate(anyInt(), eq(targetDate))).willReturn(mockResult);

        // When
        String result = batchService.runBatch();

        // Then
        assertThat(result).contains("성공: 1일", "실패: 0일");
        verify(checkpointRepo).markInProgress(targetDate);
        verify(checkpointRepo).markCompleted(targetDate);
        verify(consultationFactory, times(1)).triggerAiExtraction(any(), any());
        verify(consultationFactory, times(1)).triggerSummaryGeneration(any());
    }

    @Test
    @DisplayName("runBatch: 날짜 처리 중 예외 발생 시 FAILED 마킹이 호출된다")
    void runBatch_FailedDate() {
        // Given
        setUpDefault();
        LocalDate targetDate = LocalDate.of(2026, 1, 1);
        given(checkpointRepo.findPendingOrFailedDates()).willReturn(List.of(targetDate));
        given(consultationFactory.executeStep1WithDate(anyInt(), eq(targetDate)))
                .willThrow(new RuntimeException("DB 연결 오류"));

        // When
        String result = batchService.runBatch();

        // Then
        assertThat(result).contains("성공: 0일", "실패: 1일");
        verify(checkpointRepo).markInProgress(targetDate);
        verify(checkpointRepo).markFailed(eq(targetDate), contains("DB 연결 오류"));
        verify(checkpointRepo, never()).markCompleted(any());
    }

    @Test
    @DisplayName("runBatch: BatchResult가 empty면 triggerAiExtraction을 호출하지 않는다")
    void runBatch_EmptyBatchResult_SkipsTriggers() {
        // Given
        setUpDefault();
        LocalDate targetDate = LocalDate.of(2026, 1, 1);
        given(checkpointRepo.findPendingOrFailedDates()).willReturn(List.of(targetDate));

        SyntheticConsultationFactory.BatchResult emptyResult =
                new SyntheticConsultationFactory.BatchResult(List.of(), List.of());
        given(consultationFactory.executeStep1WithDate(anyInt(), eq(targetDate))).willReturn(emptyResult);

        // When
        batchService.runBatch();

        // Then
        verify(consultationFactory, never()).triggerAiExtraction(any(), any());
        verify(consultationFactory, never()).triggerSummaryGeneration(any());
        // 데이터 없어도 날짜 자체는 완료 처리
        verify(checkpointRepo).markCompleted(targetDate);
    }

    @Test
    @DisplayName("runBatch: 이미 실행 중일 때 재진입하면 '이미 실행 중' 메시지를 반환한다")
    void runBatch_ReentrantCallReturnsAlreadyRunning() throws InterruptedException {
        // Given: 첫 번째 실행을 지연시켜 running=true 상태를 만든다
        setUpDefault();
        LocalDate targetDate = LocalDate.of(2026, 1, 1);
        given(checkpointRepo.findPendingOrFailedDates()).willReturn(List.of(targetDate));
        given(consultationFactory.executeStep1WithDate(anyInt(), any()))
                .willAnswer(inv -> {
                    Thread.sleep(200);
                    return new SyntheticConsultationFactory.BatchResult(List.of(), List.of());
                });

        Thread t = new Thread(() -> batchService.runBatch());
        t.start();
        Thread.sleep(50); // 첫 실행이 시작될 시간 확보

        // When: 두 번째 동시 호출
        String result = batchService.runBatch();

        // Then
        assertThat(result).contains("이미 실행 중");
        t.join();
    }
}

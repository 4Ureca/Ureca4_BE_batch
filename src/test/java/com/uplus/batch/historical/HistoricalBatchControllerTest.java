package com.uplus.batch.historical;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.BDDMockito.*;

@ExtendWith(MockitoExtension.class)
class HistoricalBatchControllerTest {

    @Mock private HistoricalBatchService batchService;
    @Mock private HistoricalBatchProperties properties;
    @Mock private HistoricalBatchRepository checkpointRepo;

    @InjectMocks
    private HistoricalBatchController controller;

    @Test
    @DisplayName("POST /run: 이미 실행 중이면 409를 반환한다")
    void runBatch_AlreadyRunning_Returns409() {
        // Given
        given(properties.getDailyCount()).willReturn(10);
        given(batchService.isRunning()).willReturn(true);

        // When
        ResponseEntity<Map<String, Object>> response = controller.runBatch(null, null, false);

        // Then
        assertThat(response.getStatusCode().value()).isEqualTo(409);
        assertThat(response.getBody().get("status")).isEqualTo("already_running");
    }

    @Test
    @DisplayName("POST /run: 정상 조건이면 202 Accepted를 반환하고 백그라운드 스레드를 시작한다")
    void runBatch_Success_Returns202() {
        // Given
        given(batchService.isRunning()).willReturn(false);
        given(properties.getStartDate()).willReturn(LocalDate.of(2026, 1, 1));
        given(properties.getEndDate()).willReturn(LocalDate.of(2026, 3, 24));
        given(properties.getDailyCount()).willReturn(10);
        given(properties.getChunkSize()).willReturn(100);

        // When
        ResponseEntity<Map<String, Object>> response = controller.runBatch(null, null, false);

        // Then
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(response.getBody().get("status")).isEqualTo("started");
        assertThat(response.getBody()).containsKey("config");
    }

    @Test
    @DisplayName("GET /status: 날짜별 현황을 summary + details로 반환한다")
    void getStatus_ReturnsSummaryAndDetails() {
        // Given
        List<Map<String, Object>> rows = List.of(
                Map.of("target_date", "2026-01-01", "status", "COMPLETED"),
                Map.of("target_date", "2026-01-02", "status", "FAILED"),
                Map.of("target_date", "2026-01-03", "status", "IN_PROGRESS"),
                Map.of("target_date", "2026-01-04", "status", "PENDING")
        );
        given(checkpointRepo.findAllStatus()).willReturn(rows);
        given(batchService.isRunning()).willReturn(true);

        // When
        ResponseEntity<Map<String, Object>> response = controller.getStatus();

        // Then
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        Map<String, Object> body = response.getBody();
        assertThat(body.get("running")).isEqualTo(true);

        @SuppressWarnings("unchecked")
        Map<String, Object> summary = (Map<String, Object>) body.get("summary");
        assertThat(summary.get("total")).isEqualTo(4L);
        assertThat(summary.get("completed")).isEqualTo(1L);
        assertThat(summary.get("failed")).isEqualTo(1L);
        assertThat(summary.get("inProgress")).isEqualTo(1L);
        assertThat(summary.get("pending")).isEqualTo(1L);
    }

    @Test
    @DisplayName("GET /health: running 상태를 반환한다")
    void health_ReturnsRunning() {
        // Given
        given(batchService.isRunning()).willReturn(false);

        // When
        ResponseEntity<Map<String, Object>> response = controller.health();

        // Then
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody().get("running")).isEqualTo(false);
    }
}

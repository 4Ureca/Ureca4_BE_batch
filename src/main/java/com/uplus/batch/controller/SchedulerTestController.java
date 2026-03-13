package com.uplus.batch.controller;

import com.uplus.batch.schedular.DailyReportScheduler;
import com.uplus.batch.schedular.WeeklyReportScheduler;
import com.uplus.batch.schedular.MonthlyReportScheduler;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * 스케줄러 수동 테스트용 컨트롤러
 *
 * - 시간 미입력 → 즉시 실행
 * - 시간 입력   → 해당 시간에 예약 후 실행
 *
 * ⚠️ 개발/테스트 전용 — 운영 배포 시 비활성화 권장
 */
@Slf4j
@Tag(name = "scheduler-test", description = "스케줄러 수동/예약 실행 (테스트용)")
@RestController
@RequestMapping("/api/scheduler")
@RequiredArgsConstructor
public class SchedulerTestController {

  private final DailyReportScheduler dailyReportScheduler;
  private final WeeklyReportScheduler weeklyReportScheduler;
  private final MonthlyReportScheduler monthlyReportScheduler;
  private final TaskScheduler taskScheduler;

  @Operation(summary = "일별 스케줄러 실행",
      description = "date 미입력 시 DB 내 모든 요약문 날짜 대상 전체 실행. date 입력 시 해당 날짜만 실행. count로 실행 건수 제한 가능")
  @PostMapping("/daily")
  public ResponseEntity<String> triggerDaily(
      @Parameter(description = "집계 대상 날짜 (예: 2026-03-12). 미입력 시 전체 실행")
      @RequestParam(required = false) String date,
      @Parameter(description = "전체 실행 시 앞에서 N건만 실행 (예: 10). 미입력 시 전체")
      @RequestParam(required = false) Integer count,
      @Parameter(description = "예약 시간 (예: 2026-03-12T14:30:00). 미입력 시 즉시 실행")
      @RequestParam(required = false) String reserveAt) {

    if (reserveAt == null || reserveAt.isBlank()) {
      if (date != null && !date.isBlank()) {
        dailyReportScheduler.runDailyBatch(date);
      } else {
        dailyReportScheduler.runAllDates(count);
      }
      String label = date != null ? date : (count != null ? "ALL(limit=" + count + ")" : "ALL");
      return ResponseEntity.ok("일별 스케줄러 즉시 실행 완료 (date=" + label + ")");
    }

    LocalDateTime scheduled = LocalDateTime.parse(reserveAt);
    taskScheduler.schedule(
        () -> {
          if (date != null && !date.isBlank()) {
            dailyReportScheduler.runDailyBatch(date);
          } else {
            dailyReportScheduler.runAllDates(count);
          }
        },
        scheduled.atZone(ZoneId.of("Asia/Seoul")).toInstant());

    log.info("[SchedulerTest] 일별 배치 예약 — date={}, count={}, reserveAt={}", date, count, reserveAt);
    return ResponseEntity.ok("일별 스케줄러 예약 완료 → " + formatTime(scheduled));
  }

  @Operation(summary = "주별 스케줄러 실행",
      description = "date 미입력 시 DB 내 모든 요약문 날짜 대상 전체 실행. date 입력 시 해당 주만 실행. count로 실행 건수 제한 가능")
  @PostMapping("/weekly")
  public ResponseEntity<String> triggerWeekly(
      @Parameter(description = "집계 기준 날짜 (해당 주 계산용, 예: 2026-03-12). 미입력 시 전체 실행")
      @RequestParam(required = false) String date,
      @Parameter(description = "전체 실행 시 앞에서 N건(주)만 실행 (예: 5). 미입력 시 전체")
      @RequestParam(required = false) Integer count,
      @Parameter(description = "예약 시간 (예: 2026-03-12T14:30:00). 미입력 시 즉시 실행")
      @RequestParam(required = false) String reserveAt) {

    if (reserveAt == null || reserveAt.isBlank()) {
      if (date != null && !date.isBlank()) {
        weeklyReportScheduler.runWeeklyBatch(date);
      } else {
        weeklyReportScheduler.runAllDates(count);
      }
      String label = date != null ? date : (count != null ? "ALL(limit=" + count + ")" : "ALL");
      return ResponseEntity.ok("주별 스케줄러 즉시 실행 완료 (date=" + label + ")");
    }

    LocalDateTime scheduled = LocalDateTime.parse(reserveAt);
    taskScheduler.schedule(
        () -> {
          if (date != null && !date.isBlank()) {
            weeklyReportScheduler.runWeeklyBatch(date);
          } else {
            weeklyReportScheduler.runAllDates(count);
          }
        },
        scheduled.atZone(ZoneId.of("Asia/Seoul")).toInstant());

    log.info("[SchedulerTest] 주별 배치 예약 — date={}, count={}, reserveAt={}", date, count, reserveAt);
    return ResponseEntity.ok("주별 스케줄러 예약 완료 → " + formatTime(scheduled));
  }

  @Operation(summary = "월별 스케줄러 실행",
      description = "date 미입력 시 DB 내 모든 요약문 날짜 대상 전체 실행. date 입력 시 해당 월만 실행. count로 실행 건수 제한 가능")
  @PostMapping("/monthly")
  public ResponseEntity<String> triggerMonthly(
      @Parameter(description = "집계 기준 날짜 (해당 월 계산용, 예: 2026-03-12). 미입력 시 전체 실행")
      @RequestParam(required = false) String date,
      @Parameter(description = "전체 실행 시 앞에서 N건(월)만 실행 (예: 3). 미입력 시 전체")
      @RequestParam(required = false) Integer count,
      @Parameter(description = "예약 시간 (예: 2026-03-12T14:30:00). 미입력 시 즉시 실행")
      @RequestParam(required = false) String reserveAt) {

    if (reserveAt == null || reserveAt.isBlank()) {
      if (date != null && !date.isBlank()) {
        monthlyReportScheduler.runMonthlyBatch(date);
      } else {
        monthlyReportScheduler.runAllDates(count);
      }
      String label = date != null ? date : (count != null ? "ALL(limit=" + count + ")" : "ALL");
      return ResponseEntity.ok("월별 스케줄러 즉시 실행 완료 (date=" + label + ")");
    }

    LocalDateTime scheduled = LocalDateTime.parse(reserveAt);
    taskScheduler.schedule(
        () -> {
          if (date != null && !date.isBlank()) {
            monthlyReportScheduler.runMonthlyBatch(date);
          } else {
            monthlyReportScheduler.runAllDates(count);
          }
        },
        scheduled.atZone(ZoneId.of("Asia/Seoul")).toInstant());

    log.info("[SchedulerTest] 월별 배치 예약 — date={}, count={}, reserveAt={}", date, count, reserveAt);
    return ResponseEntity.ok("월별 스케줄러 예약 완료 → " + formatTime(scheduled));
  }

  private String formatTime(LocalDateTime dt) {
    return dt.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
  }
}

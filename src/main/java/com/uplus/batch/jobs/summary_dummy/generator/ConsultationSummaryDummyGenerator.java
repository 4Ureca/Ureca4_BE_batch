package com.uplus.batch.jobs.summary_dummy.generator;

import com.uplus.batch.common.dummy.dto.CacheDummy;
import com.uplus.batch.domain.summary.entity.ConsultationSummary;
import com.uplus.batch.jobs.summary_dummy.dto.ConsultationResultRow;
import java.util.ArrayList;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Period;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

@Component
@RequiredArgsConstructor
public class ConsultationSummaryDummyGenerator {

  private static final List<String> RISK_LEVELS = List.of("LOW", "MEDIUM", "HIGH");

  private final ThreadLocalRandom random = ThreadLocalRandom.current();
  private final CacheDummy cache;

  public ConsultationSummary generate(ConsultationResultRow row) {

    ConsultationSummary doc = new ConsultationSummary();

    doc.setConsultId(row.getConsultId());
    doc.setConsultedAt(row.getCreatedAt());
    doc.setChannel(row.getChannel());
    doc.setDurationSec(row.getDurationSec());

    doc.setAgent(
        ConsultationSummary.Agent.builder()
            .id(row.getEmpId())
            .name(row.getAgentName())
            .build()
    );

    doc.setCustomer(
        ConsultationSummary.Customer.builder()
            .id(row.getCustomerId())
            .name(row.getCustomerName())
            .type(row.getCustomerType())
            .phone(row.getCustomerPhone())
            .grade(row.getGradeCode())
            .ageGroup(calculateAgeGroup(row.getBirthDate()))
            .gender(mapGender(row.getGender()))
            .satisfiedScore(randomSatisfiedScore())
            .build()
    );

    doc.setCategory(
        ConsultationSummary.Category.builder()
            .code(row.getCategoryCode())
            .large(row.getCategoryLarge())
            .medium(row.getCategoryMedium())
            .small(row.getCategorySmall())
            .build()
    );

    doc.setIam(
        ConsultationSummary.Iam.builder()
            .issue(row.getIamIssue())
            .action(row.getIamAction())
            .memo(row.getIamMemo())
            .matchKeyword(List.of("요금", "할인", "키워드"))
            .matchRates(random.nextDouble(0.3, 0.95))
            .build()
    );

    doc.setRiskFlags(randomRiskFlags());
    doc.setSummary(randomSummary());
    doc.setCancellation(randomCancellation());
    doc.setResultProducts(randomResultProducts());
    doc.setCreatedAt(LocalDateTime.now());

    return doc;
  }

  private ConsultationSummary.Summary randomSummary() {

    boolean completed = random.nextInt(100) < 90;

    return ConsultationSummary.Summary.builder()
        .status(completed ? "COMPLETED" : "FAILED")
        .content(completed ? "상담 정상 처리 완료" : null)
        .keywords(List.of("상담", "처리"))
        .build();
  }

  private List<ConsultationSummary.RiskFlag> randomRiskFlags() {

    if (random.nextInt(100) < 2 &&
        cache.getRiskTypes() != null &&
        !cache.getRiskTypes().isEmpty()) {

      List<String> codes = new ArrayList<>(cache.getRiskTypes().keySet());
      String selected = codes.get(random.nextInt(codes.size()));
      String level = RISK_LEVELS.get(random.nextInt(RISK_LEVELS.size()));

      return List.of(
          ConsultationSummary.RiskFlag.builder()
              .riskType(selected)
              .riskLevel(level)
              .build()
      );
    }

    return null;
  }

  private ConsultationSummary.Cancellation randomCancellation() {

    boolean intent = random.nextInt(100) < 10;

    List<String> defense = null;

    if (intent &&
        cache.getDefenseActions() != null &&
        !cache.getDefenseActions().isEmpty()) {

      defense = List.of(
          cache.getDefenseActions()
              .get(random.nextInt(cache.getDefenseActions().size()))
      );
    }

    return ConsultationSummary.Cancellation.builder()
        .intent(intent)
        .defenseAttempted(intent && random.nextBoolean())
        .defenseSuccess(intent && random.nextInt(100) < 50)
        .defenseActions(defense)
        .complaintReasons(intent ? randomComplaintReason() : null)
        .build();
  }

  private static final List<String> COMPLAINT_REASONS = List.of(
      "요금 불만", "서비스 품질 불만", "경쟁사 이동", "약정 만료", "단말기 불만", "부가서비스 불만"
  );

  private String randomComplaintReason() {
    return COMPLAINT_REASONS.get(random.nextInt(COMPLAINT_REASONS.size()));
  }

  private String calculateAgeGroup(LocalDate birthDate) {

    if (birthDate == null) return null;

    int age = Period.between(birthDate, LocalDate.now()).getYears();
    int group = (age / 10) * 10;

    return group + "대";
  }

  /** customers.gender('M'/'F'/null) → '남성'/'여성'. null이면 랜덤 폴백. */
  private String mapGender(String raw) {
    if ("M".equalsIgnoreCase(raw)) return "남성";
    if ("F".equalsIgnoreCase(raw)) return "여성";
    return random.nextBoolean() ? "남성" : "여성";
  }

  private Double randomSatisfiedScore() {

    if (random.nextInt(100) < 60) return null;

    double score = random.nextDouble(0.0, 5.0);
    return Math.round(score * 10) / 10.0;
  }

  private List<ConsultationSummary.ResultProducts> randomResultProducts() {

    int count = randomCount();
    if (count == 0) return null;

    List<ConsultationSummary.ResultProducts> list = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      list.add(buildSingleResultProduct());
    }
    return list;
  }

  private int randomCount() {

    int percent = random.nextInt(100);

    if (percent < 70) return random.nextInt(3);
    else return random.nextInt(2) + 3;
  }

  private ConsultationSummary.ResultProducts buildSingleResultProduct() {
    String changeType = randomChangeType();
    return switch (changeType) {
      case "NEW"    -> buildNewProducts(changeType);
      case "CANCEL" -> buildCancelProducts(changeType);
      case "CHANGE" -> buildConversionProducts(changeType);
      case "RENEW"  -> buildRecommitmentProducts(changeType);
      default -> null;
    };
  }

  private String randomChangeType() {

    List<String> types = cache.getContractTypes();

    if (types == null || types.isEmpty()) {
      throw new IllegalStateException("contractTypes not initialized");
    }

    return types.get(random.nextInt(types.size()));
  }

  private ConsultationSummary.ResultProducts buildNewProducts(String type) {
    return ConsultationSummary.ResultProducts.builder()
        .changeType(type)
        .subscribed(List.of(randomAnyProduct()))
        .build();
  }

  private ConsultationSummary.ResultProducts buildCancelProducts(String type) {
    return ConsultationSummary.ResultProducts.builder()
        .changeType(type)
        .canceled(List.of(randomAnyProduct()))
        .build();
  }

  private ConsultationSummary.ResultProducts buildConversionProducts(String type) {

    String oldProduct = randomAnyProduct();
    String newProduct;

    do {
      newProduct = randomAnyProduct();
    } while (newProduct.equals(oldProduct));

    ConsultationSummary.ResultProducts.Conversion conv =
        ConsultationSummary.ResultProducts.Conversion.builder()
            .canceled(oldProduct)
            .subscribed(newProduct)
            .build();

    return ConsultationSummary.ResultProducts.builder()
        .changeType(type)
        .conversion(List.of(conv))
        .build();
  }

  private ConsultationSummary.ResultProducts buildRecommitmentProducts(String type) {
    return ConsultationSummary.ResultProducts.builder()
        .changeType(type)
        .recommitment(List.of(randomAnyProduct()))
        .build();
  }

  private String randomAnyProduct() {

    List<String> pool = new ArrayList<>();

    if (cache.getHomeProductCodes() != null)
      pool.addAll(cache.getHomeProductCodes());
    if (cache.getMobileProductCodes() != null)
      pool.addAll(cache.getMobileProductCodes());
    if (cache.getAdditionalProductCodes() != null)
      pool.addAll(cache.getAdditionalProductCodes());

    if (pool.isEmpty()) throw new IllegalStateException("Product codes not initialized");

    return pool.get(random.nextInt(pool.size()));
  }
}
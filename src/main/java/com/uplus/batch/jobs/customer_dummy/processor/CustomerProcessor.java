package com.uplus.batch.jobs.customer_dummy.processor;

import com.uplus.batch.domain.customer.dto.CustomerRow;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.UUID;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;
import net.datafaker.Faker;

@Component
@StepScope
public class CustomerProcessor implements ItemProcessor<Integer, CustomerRow> {

  private final Random random = new Random();
  private final Faker faker = new Faker(new Locale("ko"));
  private static final List<String> EMAIL_DOMAINS = List.of(
      "naver.com",
      "google.com",
      "hanmail.net",
      "kakao.com"
  );

  @Override
  public CustomerRow process(Integer item) {

    String customerType = randomCustomerType();
    String name = generateName(customerType);

    String gender = null;
    if ("개인".equals(customerType)) {
      gender = random.nextBoolean() ? "M" : "F";
    }

    return CustomerRow.builder()
        .identificationNum(UUID.randomUUID().toString())
        .name(name)
        .customerType(customerType)
        .gender(gender)
        .birthDate(randomBirthDate())
        .gradeCode(randomGrade())
        .preferredContact(randomContact())
        .email(generateRandomEmail())
        .phone("010" + (10000000 + random.nextInt(90000000)))
        .createdAt(randomCreatedAt())
        .build();
  }

  private String randomCustomerType() {
    return random.nextInt(100) < 90 ? "개인" : "법인";
  }

  private String generateName(String customerType) {
    if ("법인".equals(customerType)) {
      return faker.company().name();
    }
    return randomKoreanName();
  }

  private String randomKoreanName() {
    String last = faker.name().lastName();
    String first = faker.name().firstName();
    return last + first;
  }

  private String generateRandomEmail() {
    String localPart = UUID.randomUUID()
        .toString()
        .replace("-", "")
        .substring(0, 12);

    String domain = EMAIL_DOMAINS.get(
        random.nextInt(EMAIL_DOMAINS.size())
    );

    return localPart + "@" + domain;
  }

  private String randomGrade() {
    int r = random.nextInt(100);
    if (r < 70) return "DIAMOND";
    if (r < 95) return "VIP";
    return "VVIP";
  }

  private String randomContact() {
    int r = random.nextInt(100);
    if (r < 80)
      return "CALL";
    return "CHATTING";
  }

  private LocalDate randomBirthDate() {
    int minYear = 1960;
    int maxYear = 2005;
    double mean = 1985;
    double stdDev = 10;

    int year;
    do {
      year = (int) Math.round(mean + random.nextGaussian() * stdDev);
    } while (year < minYear || year > maxYear);

    return LocalDate.of(year,
        random.nextInt(12) + 1,
        random.nextInt(28) + 1);
  }

  private LocalDateTime randomCreatedAt() {
    LocalDate start = LocalDate.of(2020, 1, 1);
    long days = ChronoUnit.DAYS.between(start, LocalDate.of(2025, 12, 31));
    double bias = Math.pow(random.nextDouble(), 0.5);
    long offset = (long) (days * bias);
    return start.plusDays(offset).atStartOfDay();
  }
}
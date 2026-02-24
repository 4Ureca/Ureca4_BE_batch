package com.uplus.batch.domain.customer.dto;

import java.time.LocalDate;
import java.time.LocalDateTime;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class CustomerRow {

  private String identificationNum;
  private String name;
  private String customerType;
  private String gender;
  private LocalDate birthDate;
  private String gradeCode;
  private String preferredContact;
  private String email;
  private String phone;
  private LocalDateTime createdAt;
}
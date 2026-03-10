package com.uplus.batch.domain.summary.dto;

public record ConsultProductLogSyncRow(
    Long consultId,
    String contractType,
    String productType,
    String newProductHome,
    String newProductMobile,
    String newProductService,
    String canceledProductHome,
    String canceledProductMobile,
    String canceledProductService
) {
}
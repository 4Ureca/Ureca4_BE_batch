package com.uplus.batch.domain.extraction.dto;

public record QualityScoringResponse(
	    int score,
	    String evaluation_reason,
	    boolean is_candidate
	) {}
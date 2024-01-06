package com.ybkim.AptPrice.domain.MyHomePrice.dao;

import jakarta.validation.constraints.NotBlank;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

@AllArgsConstructor
@Getter
@ToString
public class MyHomePriceFilterCondition {

  @NotBlank
  private String searchSidoCd;       //시도
  private String searchGugunCd;      //시군구
  private String searchDongCd;       //읍면동
  @NotBlank
  private String contractDate;       //시작 계약일자
  @NotBlank
  private String contractDateTo;     //종료 계약일자
  private String searchArea;         //면적
  private String searchAreaValue;   //시작 면적
  private String searchAreaValueTo;  //종료 면적
  private String searchFromAmount;   //시작 금액
  private String searchToAmnount;    //종료 금액

  private int startRec;           //시작레코드
  private int endRec;              //종료레코드

  public MyHomePriceFilterCondition(String contractDate, String contractDateTo, String searchSidoCd
      , String searchGugunCd, String searchDongCd, String searchArea, String searchAreaValue, String searchAreaValueTo, String searchFromAmount, String searchToAmnount) {
    this.contractDate = contractDate;
    this.contractDateTo = contractDateTo;
    this.searchSidoCd = searchSidoCd;
    this.searchGugunCd = searchGugunCd;
    this.searchDongCd = searchDongCd;
    this.searchArea = searchArea;
    this.searchAreaValue = searchAreaValue;
    this.searchAreaValueTo = searchAreaValueTo;
    this.searchFromAmount = searchFromAmount;
    this.searchToAmnount = searchToAmnount;
  }
}

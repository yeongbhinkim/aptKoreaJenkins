package com.ybkim.AptPrice.domain.common.paging;


import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter @Setter
@ToString
public class FindCriteria extends PageCriteria{

  private String searchSidoCd;       //시도
  private String searchGugunCd;      //시군구
  private String searchDongCd;       //읍면동
  private String contractDate;       //시작 계약일자
  private String contractDateTo;     //종료 계약일자
  private String searchArea;         //면적
  private String searchAreaValue;    //시작 면적
  private String searchAreaValueTo;  //종료 면적
  private String searchFromAmount;   //시작 금액
  private String searchToAmnount;    //종료 금액

  public FindCriteria(RecordCriteria rc, int pageCount) {
    super(rc, pageCount);
  }
}

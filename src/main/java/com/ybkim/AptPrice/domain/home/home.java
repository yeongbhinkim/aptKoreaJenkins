package com.ybkim.AptPrice.domain.home;

import lombok.Data;

@Data
public class home {

  private String CITY_NM;	  //VARCHAR2(30 BYTE)	시도 이름
  private String CITY_CODE;	//VARCHAR2(30 BYTE)	시도 코드
  private String COUNTY_NM;	//VARCHAR2(30 BYTE)	시군구 이름
  private String COUNTY_CODE;	//VARCHAR2(30 BYTE)	시군구 코드
  private String DISTRICTS_NM;	//VARCHAR2(30 BYTE)	읍면동_이름
  private String DISTRICTS_CODE;	//VARCHAR2(30 BYTE)	읍면동_코드

}
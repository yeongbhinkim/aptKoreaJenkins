package com.ybkim.AptPrice.domain.AptScheduler;

import lombok.Data;

@Data
public class CountyCode {
    private String county_code;	  //VARCHAR2(30 BYTE)	시도 코드

    private String city;	  //시군

    private String county;


}

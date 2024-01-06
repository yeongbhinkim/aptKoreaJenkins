package com.ybkim.AptPrice.domain.home.svc;


import com.ybkim.AptPrice.domain.home.home;

import java.util.List;

public interface homeSVC {

  /**
   * 시도 조회
   *
   * @return
   */
  List<home> regionCity();

  List<home> regionCounty(String CITY_CODE);

  List<home> regionDistricts(String COUNTY_CODE);

}

package com.ybkim.AptPrice.domain.home.dao;


import com.ybkim.AptPrice.domain.home.home;

import java.util.List;

public interface homeDAO {

  /**
   * 시도 조회
   * @return
   */
  List<home> selectRegionCity();

  /**
   * 시군구 조회
   * @return
   */
  List<home> selectRegionCounty(String CITY_CODE);

  /**
   * 읍면동 조회
   * @return
   */
  List<home> selectRegionDistricts(String COUNTY_CODE);

}

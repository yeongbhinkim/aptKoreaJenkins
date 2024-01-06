package com.ybkim.AptPrice.domain.AptScheduler.dao;


import com.ybkim.AptPrice.domain.AptScheduler.AptApiDb;
import com.ybkim.AptPrice.domain.AptScheduler.CountyCode;

import java.util.List;

public interface AptApiDbDAO {


  /**
   * API -> DB insert
   *
   * @return
   */
  void insertApiDb(AptApiDb aptApiDb);

  /**
   * 시군구 조회
   * @return
   */
  List<CountyCode> apiSelectRegionCounty();

  CountyCode selectCity(String city);
  CountyCode selectCounty(String county);
}

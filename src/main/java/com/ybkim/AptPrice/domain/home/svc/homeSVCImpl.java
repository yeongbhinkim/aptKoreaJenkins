package com.ybkim.AptPrice.domain.home.svc;


import com.ybkim.AptPrice.domain.home.dao.homeDAO;
import com.ybkim.AptPrice.domain.home.home;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
@Transactional
public class homeSVCImpl implements homeSVC {

  private final homeDAO homeDAO;

  /**
   * APT 조건 조회
   *
   * @return
   */
  @Override
  public List<home> regionCity() {
    return homeDAO.selectRegionCity();
  }

  @Override
  public List<home> regionCounty(String CITY_CODE) {
//    log.info("/regionCounty 요청됨 {} ", CITY_CODE);
    return homeDAO.selectRegionCounty(CITY_CODE);

  }

  @Override
  public List<home> regionDistricts(String COUNTY_CODE) {
//    log.info("/regionDistricts 요청됨 {} ", COUNTY_CODE);
    return homeDAO.selectRegionDistricts(COUNTY_CODE);
  }


}

package com.ybkim.AptPrice.domain.AptScheduler.svc;


import com.ybkim.AptPrice.domain.AptScheduler.AptApiDb;
import com.ybkim.AptPrice.domain.AptScheduler.CountyCode;
import com.ybkim.AptPrice.domain.AptScheduler.dao.AptApiDbDAO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
@Transactional
public class AptApiDbSVCImpl implements AptApiDbSVC {

  private final AptApiDbDAO AptApiDbDAO;

  /**
   * API -> DB insert
   *
   * @param aptApiDb
   * @return
   */
  @Override
  public void ApiDb(AptApiDb aptApiDb) {
//    log.info("/regionCounty 요청됨 {} ", CITY_CODE);
    AptApiDbDAO.insertApiDb(aptApiDb);

  }

  /**
   * 시군구 조회
   *
   * @return
   */
  @Override
  public List<CountyCode> apiRegionCounty() {
    return AptApiDbDAO.apiSelectRegionCounty();
  }

  @Override
  public CountyCode selectCity(String city) {

    return AptApiDbDAO.selectCity(city);
  }
  @Override
  public CountyCode selectCounty(String county) {

    return AptApiDbDAO.selectCounty(county);
  }


}

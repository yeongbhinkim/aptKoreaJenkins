package com.ybkim.AptPrice.domain.MyHomePrice.svc;


import com.ybkim.AptPrice.domain.MyHomePrice.MyHomePrice;
import com.ybkim.AptPrice.domain.MyHomePrice.dao.MyHomePriceDAO;
import com.ybkim.AptPrice.domain.MyHomePrice.dao.MyHomePriceFilterCondition;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
@Transactional
public class MyHomePriceSVCImpl implements MyHomePriceSVC {

  private final MyHomePriceDAO MyHomePriceDAO;

  private String CODE = "F0101";

  /**
   * APT 조건 조회
   *
   * @param myHomePriceFilterCondition
   * @return
   */
  @Override
  public List<MyHomePrice> MyHomePriceList(MyHomePriceFilterCondition myHomePriceFilterCondition) {
    return MyHomePriceDAO.selectMyHomePriceList(myHomePriceFilterCondition);
  }

  /**
   * 전체 건수
   *
   * @return
   */
  @Override
  public int totalCount(MyHomePriceFilterCondition myHomePriceFilterCondition) {
    return MyHomePriceDAO.totalCount(myHomePriceFilterCondition);
  }

  /**
   * APT 상세조회 폼
   *
   * @param apt_id
   * @return
   */
  @Override
  public MyHomePrice MyHomePriceDetailForm(Long apt_id) {
    return MyHomePriceDAO.selectMyHomePriceDetailForm(apt_id);
  }

  /**
   * APT 상세조회 리스트
   *
   * @param apt_id
   * @return
   */
  @Override
  public List<MyHomePrice> MyHomePriceDetail(Long apt_id) {
    return MyHomePriceDAO.selectMyHomePriceDetail(apt_id);
  }

  /**
   * APT 상세조회 ScatterChart
   *
   * @param apt_id
   * @return
   */
  @Override
  public List<MyHomePrice> MyHomePriceScatterChart(Long apt_id) {
    return MyHomePriceDAO.selectMyHomePriceScatterChart(apt_id);
  }

}

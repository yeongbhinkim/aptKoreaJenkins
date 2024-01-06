package com.ybkim.AptPrice.domain.MyHomePrice.svc;


import com.ybkim.AptPrice.domain.MyHomePrice.MyHomePrice;
import com.ybkim.AptPrice.domain.MyHomePrice.dao.MyHomePriceFilterCondition;

import java.util.List;

public interface MyHomePriceSVC {

  /**
   * APT 조건 조회
   *
   * @param myHomePriceFilterCondition
   * @return
   */
  List<MyHomePrice> MyHomePriceList(MyHomePriceFilterCondition myHomePriceFilterCondition);

  /**
   * 전체건수
   *
   * @param myHomePriceFilterCondition
   * @return
   */
  int totalCount(MyHomePriceFilterCondition myHomePriceFilterCondition);


  /**
   * APT 상세조회 폼
   *
   * @param apt_id
   * @return
   */
  MyHomePrice MyHomePriceDetailForm(Long apt_id);

  /**
   * APT 상세조회 리스트
   *
   * @param apt_id
   * @return
   */
  List<MyHomePrice> MyHomePriceDetail(Long apt_id);

  /**
   * APT 상세조회 ScatterChart
   *
   * @param apt_id
   * @return
   */
  List<MyHomePrice> MyHomePriceScatterChart(Long apt_id);
}

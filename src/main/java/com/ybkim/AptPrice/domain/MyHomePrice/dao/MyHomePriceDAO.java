package com.ybkim.AptPrice.domain.MyHomePrice.dao;


import com.ybkim.AptPrice.domain.MyHomePrice.MyHomePrice;

import java.util.List;

public interface MyHomePriceDAO {

  /**
   * APT 조건 조회
   *
   * @param myHomePriceFilterCondition
   * @return
   */
  List<MyHomePrice> selectMyHomePriceList(MyHomePriceFilterCondition myHomePriceFilterCondition);

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
  MyHomePrice selectMyHomePriceDetailForm(Long apt_id);

  /**
   * APT 상세조회 리스트
   *
   * @param apt_id
   * @return
   */
  List<MyHomePrice> selectMyHomePriceDetail(Long apt_id);

  /**
   * APT 상세조회 ScatterChart
   *
   * @param apt_id
   * @return
   */
  List<MyHomePrice> selectMyHomePriceScatterChart(Long apt_id);
}

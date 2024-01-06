package com.ybkim.AptPrice.web;


import com.ybkim.AptPrice.domain.MyHomePrice.MyHomePrice;
import com.ybkim.AptPrice.domain.MyHomePrice.dao.MyHomePriceFilterCondition;
import com.ybkim.AptPrice.domain.MyHomePrice.svc.MyHomePriceSVC;
import com.ybkim.AptPrice.domain.common.paging.FindCriteria;
import com.ybkim.AptPrice.domain.home.home;
import com.ybkim.AptPrice.domain.home.svc.homeSVC;
import com.ybkim.AptPrice.web.form.MyHomePrice.MyHomePriceForm;
import com.ybkim.AptPrice.web.form.MyHomePrice.MyHomePriceListForm;
import com.ybkim.AptPrice.web.form.MyHomePrice.MyHomePriceScatterChart;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Slf4j
@Controller
@RequestMapping("/MyHomePrice")
@RequiredArgsConstructor
public class MyHomePriceController {
  private final MyHomePriceSVC MyHomePriceSVC;
  private final homeSVC homeSVC;
  private static final Logger logger = LogManager.getLogger(MyHomePriceController.class);

  @Value("${external.api.molit.kakaoServiceKey}")
  private String kakaoServiceKey;

  @Autowired
  @Qualifier("fc5") //동일한 타입의 객체가 여러개있을때 빈이름을 명시적으로 지정해서 주입받을때
  private FindCriteria fc;

  /**
   * 조건 조회 리스트
   *
   * @param reqPage
   * @param contractDate
   * @param contractDateTo
   * @param searchSidoCd
   * @param searchGugunCd
   * @param searchDongCd
   * @param searchArea
   * @param searchAreaValue
   * @param searchAreaValueTo
   * @param searchFromAmount
   * @param searchToAmnount
   * @param model
   * @return
   */
  @GetMapping({
      "/list/{reqPage}/{searchSidoCd}/{searchGugunCd}/{searchDongCd}/{contractDate}/{contractDateTo}/////",
      "/list/{reqPage}/{searchSidoCd}/{searchGugunCd}/{searchDongCd}/{contractDate}/{contractDateTo}/{searchArea}////",
      "/list/{reqPage}/{searchSidoCd}/{searchGugunCd}/{searchDongCd}/{contractDate}/{contractDateTo}/{searchArea}///{searchFromAmount}/{searchToAmnount}",
      "/list/{reqPage}/{searchSidoCd}/{searchGugunCd}/{searchDongCd}/{contractDate}/{contractDateTo}/{searchArea}/{searchAreaValue}/{searchAreaValueTo}//",
      "/list/{reqPage}/{searchSidoCd}/{searchGugunCd}/{searchDongCd}/{contractDate}/{contractDateTo}/{searchArea}/{searchAreaValue}/{searchAreaValueTo}/{searchFromAmount}/{searchToAmnount}"})
  public String listAndReqPage(
      @PathVariable(required = false) Optional<Integer> reqPage,            //요청페이지, 요청없으면 1
      @PathVariable(required = false) Optional<String> searchSidoCd,        //시도
      @PathVariable(required = false) Optional<String> searchGugunCd,       //시군구
      @PathVariable(required = false) Optional<String> searchDongCd,        //읍면동
      @PathVariable(required = false) Optional<String> contractDate,        //시작 계약일자
      @PathVariable(required = false) Optional<String> contractDateTo,      //종료 계약일자
      @PathVariable(required = false) Optional<String> searchArea,          //면적
      @PathVariable(required = false) Optional<String> searchAreaValue,     //면적
      @PathVariable(required = false) Optional<String> searchAreaValueTo,   //면적
      @PathVariable(required = false) Optional<String> searchFromAmount,    //시작 금액
      @PathVariable(required = false) Optional<String> searchToAmnount,     //종료 금액
      Model model) {
    List<home> regionCity = homeSVC.regionCity();
    model.addAttribute("regionCity", regionCity);
//    log.info("/list 요청됨 {},{},{},{},{},{},{},{},{}", reqPage, contractDate, contractDateTo
//        , searchSidoCd, searchGugunCd, searchDongCd, searchArea, searchFromAmount, searchToAmnount);
    logger.info("/list 요청됨 {},{},{},{},{},{},{},{},{}", reqPage
        , searchSidoCd, searchGugunCd, searchDongCd, contractDate, contractDateTo
        , searchArea, searchFromAmount, searchToAmnount);

    //FindCriteria 값 설정
    fc.getRc().setReqPage(reqPage.orElse(1));                     //요청페이지, 요청없으면 1
    fc.setSearchSidoCd(searchSidoCd.orElse(""));                  //시도
    fc.setSearchGugunCd(searchGugunCd.orElse(""));                //시군구
    fc.setSearchDongCd(searchDongCd.orElse(""));                  //읍면동
    fc.setContractDate(contractDate.orElse(""));                  //시작 계약일자
    fc.setContractDateTo(contractDateTo.orElse(""));              //종료 계약일자
    fc.setSearchArea(searchArea.orElse(""));                      //면적
    fc.setSearchAreaValue(searchAreaValue.orElse("0"));                //시작 면적
    fc.setSearchAreaValueTo(searchAreaValueTo.orElse("10000"));          //종료 면적
    fc.setSearchFromAmount(searchFromAmount.orElse("0"));         //시작 금액
    fc.setSearchToAmnount(searchToAmnount.orElse("100000000"));   //종료 금액

    //myHomePriceFilterCondition 값 설정
    MyHomePriceFilterCondition myHomePriceFilterCondition = new MyHomePriceFilterCondition(
        searchSidoCd.orElse(""), searchGugunCd.orElse(""), searchDongCd.orElse(""), contractDate.orElse(""), contractDateTo.orElse(""),
        searchArea.orElse(""), searchAreaValue.orElse("0"), searchAreaValueTo.orElse("10000"), searchFromAmount.orElse("0"),
        searchToAmnount.orElse("100000000"), fc.getRc().getStartRec(), fc.getRc().getEndRec());

    //FindCriteria 전체건수 값 설정
    fc.setTotalRec(MyHomePriceSVC.totalCount(myHomePriceFilterCondition));

    //전체조회
    List<MyHomePrice> list = MyHomePriceSVC.MyHomePriceList(myHomePriceFilterCondition);
    List<MyHomePriceListForm> partOfList = new ArrayList<>();
//    log.info("myHomePriceFilterCondition = {} ", list);
    for (MyHomePrice MyHomePrice : list) {
      MyHomePriceListForm myHomePriceListForm = new MyHomePriceListForm();
      BeanUtils.copyProperties(MyHomePrice, myHomePriceListForm);
      partOfList.add(myHomePriceListForm);
    }

//    log.info("myHomePriceFilterCondition = {}", myHomePriceFilterCondition);
//    log.info("partOfList = {} ", partOfList);
//    log.info("fc = {} ", fc);
    //조회 리스트
    model.addAttribute("list", partOfList);
    //페이징
    model.addAttribute("fc", fc);

    return "mainView";
  }


  /**
   * 상세조회
   *
   * @param apt_id
   * @param model
   * @return
   */
  @GetMapping({"/detail/{apt_id}"})
  public String detail(
      @PathVariable(required = false) Long apt_id,             //PK_시퀀스
      Model model) {

    List<home> regionCity = homeSVC.regionCity();
    model.addAttribute("regionCity", regionCity);
    model.addAttribute("kakaoServiceKey", kakaoServiceKey);
    //상세정보 Form
    MyHomePrice myHomePrice = MyHomePriceSVC.MyHomePriceDetailForm(apt_id);

    MyHomePriceListForm myHomePriceListForm = new MyHomePriceListForm();

    BeanUtils.copyProperties(myHomePrice, myHomePriceListForm);
//    log.info("myHomePriceListForm = {} ", myHomePriceListForm);
    model.addAttribute("myHomePriceListForm", myHomePriceListForm);

    //상세정보 리스트
    List<MyHomePrice> list = MyHomePriceSVC.MyHomePriceDetail(apt_id);
    List<MyHomePriceForm> partOfList = new ArrayList<>();

    //점차트 데이터
    List<MyHomePrice> chart = MyHomePriceSVC.MyHomePriceScatterChart(apt_id);
    List<MyHomePriceScatterChart> partOfChart = new ArrayList<>();

    for (MyHomePrice MyHomePrice : list) {
      //상세정보 리스트
      MyHomePriceForm MyHomePriceForm = new MyHomePriceForm();
      BeanUtils.copyProperties(MyHomePrice, MyHomePriceForm);
      partOfList.add(MyHomePriceForm);
    }
    for (MyHomePrice MyHomePrice : chart) {
      //점차트 데이터
      MyHomePriceScatterChart MyHomePriceScatterChart = new MyHomePriceScatterChart();
      BeanUtils.copyProperties(MyHomePrice, MyHomePriceScatterChart);
      partOfChart.add(MyHomePriceScatterChart);
    }

//    log.info("partOfChart@@ = {} ", partOfChart);
//    log.info("partOfList = {} ", partOfList);
    model.addAttribute("list", partOfList);
    model.addAttribute("chart", partOfChart);

    return "contentView";
  }


}

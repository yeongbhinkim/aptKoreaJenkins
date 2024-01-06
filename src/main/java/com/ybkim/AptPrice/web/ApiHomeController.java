package com.ybkim.AptPrice.web;


import com.ybkim.AptPrice.domain.home.home;
import com.ybkim.AptPrice.domain.home.svc.homeSVC;
import com.ybkim.AptPrice.web.api.ApiResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.List;
import java.util.Optional;


@Slf4j
@Controller
@RequiredArgsConstructor
public class ApiHomeController {

  private final homeSVC homeSVC;

  @ResponseBody
  @GetMapping({"/regionCounty/",
               "/regionCounty/{CITY_CODE}"})
  public ApiResult<List<home>> regionCounty(@PathVariable Optional<String> CITY_CODE) {
    List<home> HomeForm = homeSVC.regionCounty(CITY_CODE.orElse(""));
    ApiResult<List<home>> result = new ApiResult<>("00", "success", HomeForm);
//    log.info("/regionCounty 요청됨 {} ", result);
    return result;
  }

  @ResponseBody
  @GetMapping({"/regionDistricts/",
               "/regionDistricts/{COUNTY_CODE}"})
  public ApiResult<List<home>> regionDistricts(@PathVariable Optional<String> COUNTY_CODE) {
    List<home> HomeForm = homeSVC.regionDistricts(COUNTY_CODE.orElse(""));
    ApiResult<List<home>> result = new ApiResult<>("00", "success", HomeForm);
//    log.info("/regionDistricts 요청됨 {} ", result);
    return result;
  }

}
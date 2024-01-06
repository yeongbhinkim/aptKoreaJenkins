package com.ybkim.AptPrice.web;


import com.ybkim.AptPrice.domain.home.home;
import com.ybkim.AptPrice.domain.home.svc.homeSVC;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;

import java.util.List;

@Slf4j
@Controller
@RequiredArgsConstructor
public class HomeController {

  private final homeSVC homeSVC;

  @RequestMapping({"/"})
  public String home(Model model) {
    List<home> regionCity = homeSVC.regionCity();
    model.addAttribute("regionCity", regionCity);
//    log.info("regionCity={}", regionCity);

    return "homeView";
  }

}







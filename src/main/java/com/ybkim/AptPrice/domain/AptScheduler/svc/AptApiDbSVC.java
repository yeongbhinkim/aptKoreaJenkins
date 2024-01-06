package com.ybkim.AptPrice.domain.AptScheduler.svc;


import com.ybkim.AptPrice.domain.AptScheduler.AptApiDb;
import com.ybkim.AptPrice.domain.AptScheduler.CountyCode;

import java.util.List;

public interface AptApiDbSVC {


    /**
     * API -> DB insert
     *
     * @param aptApiDb
     * @return
     */
    void ApiDb(AptApiDb aptApiDb);

    List<CountyCode> apiRegionCounty();

    CountyCode selectCity(String city);
    CountyCode selectCounty(String county);
}

package com.ybkim.AptPrice.domain.MyHomePrice.dao;


import com.ybkim.AptPrice.domain.MyHomePrice.MyHomePrice;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@Repository
@RequiredArgsConstructor

public class MyHomePriceDAOImpl implements MyHomePriceDAO {

    private final JdbcTemplate jdbcTemplate;

    /**
     * APT 조건조회
     *
     * @param myHomePriceFilterCondition
     * @return
     */
//    @Override
//    public List<MyHomePrice> selectMyHomePriceList(MyHomePriceFilterCondition myHomePriceFilterCondition) {
//        StringBuffer sql = new StringBuffer();
//        log.info("myHomePriceFilterCondition = {}", myHomePriceFilterCondition);
//        sql.append(" SELECT B.* ");
//        sql.append("       ,GET_TRANSACTIONCOUNT_LIST(B.CITY, B.STREET, B.BON_BUN, B.BU_BUN, B.DAN_GI_MYEONG, B.SQUARE_METER, B.LAYER, B.CONSTRUCTION_DATE, B.CITY_CODE) AS TRANSACTIONCOUNTLIST ");
//        sql.append("         FROM ( ");
//        sql.append("                 SELECT A.* ,ROW_NUMBER() OVER (ORDER BY FULL_CONTRACT_DATE DESC) AS NO ");
//        sql.append("                 FROM( ");
//        sql.append("                         SELECT ");
//        sql.append("                         ROW_NUMBER() OVER (PARTITION BY A.CITY, A.STREET, A.BON_BUN, A.BU_BUN, A.DAN_GI_MYEONG, A.SQUARE_METER, A.LAYER ORDER BY A.FULL_CONTRACT_DATE DESC) AS RN, ");
//        sql.append("                         A.* ");
//        sql.append("                                 FROM APT A ");
//        sql.append("                         WHERE 1 = 1 ");
//        sql.append("                         AND CITY_CODE = ? ");
////        sql.append("                         AND COUNTY_CODE = ? ");
////        sql.append("                         AND DISTRICTS_CODE = ? ");
//
//        if (!myHomePriceFilterCondition.getSearchGugunCd().equals("false")) {
//            sql.append(" AND COUNTY_CODE = ? ");
//        }
//
//        if (!myHomePriceFilterCondition.getSearchDongCd().equals("false")) {
//            sql.append(" AND DISTRICTS_CODE = ? ");
//        }
//
//        sql.append("                         AND FULL_CONTRACT_DATE BETWEEN ? AND ? ");
////        sql.append("                         AND CITY LIKE CONCAT('%', ?, '%', ?, '%') ");
//        sql.append("                         AND SQUARE_METER BETWEEN ? AND ? ");
//        sql.append("                         AND AMOUNT BETWEEN ? AND ? ");
//        sql.append("     ) A ");
//        sql.append("         WHERE A.RN = 1 ");
//        sql.append(" ) B ");
//        sql.append("         WHERE B.NO BETWEEN ? AND ? ");
//        sql.append("         ORDER BY B.CITY, B.LAYER ");
//
//
//        List<MyHomePrice> list = jdbcTemplate.query(sql.toString(),
//                new BeanPropertyRowMapper<>(MyHomePrice.class),
//                myHomePriceFilterCondition.getSearchSidoCd(),
//                myHomePriceFilterCondition.getSearchGugunCd(),
//                myHomePriceFilterCondition.getSearchDongCd(),
//                myHomePriceFilterCondition.getContractDate(),
//                myHomePriceFilterCondition.getContractDateTo(),
//                myHomePriceFilterCondition.getSearchAreaValue(),
//                myHomePriceFilterCondition.getSearchAreaValueTo(),
//                myHomePriceFilterCondition.getSearchFromAmount(),
//                myHomePriceFilterCondition.getSearchToAmnount(),
//                myHomePriceFilterCondition.getStartRec(),
//                myHomePriceFilterCondition.getEndRec()
//        );
//
//        log.info("list123 = {}", list);
//
//        return list;
//    }
    @Override
    public List<MyHomePrice> selectMyHomePriceList(MyHomePriceFilterCondition myHomePriceFilterCondition) {
        StringBuffer sql = new StringBuffer();
        log.info("myHomePriceFilterCondition = {}", myHomePriceFilterCondition);
        sql.append(" SELECT B.* ");
        sql.append("       ,GET_TRANSACTIONCOUNT_LIST(B.CITY, B.STREET, B.BON_BUN, B.BU_BUN, B.DAN_GI_MYEONG, B.SQUARE_METER, B.LAYER, B.CONSTRUCTION_DATE, B.CITY_CODE) AS TRANSACTIONCOUNTLIST ");
        sql.append("         FROM ( ");
        sql.append("                 SELECT A.* ,ROW_NUMBER() OVER (ORDER BY FULL_CONTRACT_DATE DESC) AS NO ");
        sql.append("                 FROM( ");
        sql.append("                         SELECT ");
        sql.append("                         ROW_NUMBER() OVER (PARTITION BY A.CITY, A.STREET, A.BON_BUN, A.BU_BUN, A.DAN_GI_MYEONG, A.SQUARE_METER, A.LAYER ORDER BY A.FULL_CONTRACT_DATE DESC) AS RN, ");
        sql.append("                         A.* ");
        sql.append("                                 FROM APT A ");
        sql.append("                         WHERE 1 = 1 ");
        sql.append("                         AND CITY_CODE = ? ");

        List<Object> params = new ArrayList<>();
        params.add(myHomePriceFilterCondition.getSearchSidoCd());

        if (!myHomePriceFilterCondition.getSearchGugunCd().equals("false")) {
            sql.append(" AND COUNTY_CODE = ? ");
            params.add(myHomePriceFilterCondition.getSearchGugunCd());
        }

        if (!myHomePriceFilterCondition.getSearchDongCd().equals("false")) {
            sql.append(" AND DISTRICTS_CODE = ? ");
            params.add(myHomePriceFilterCondition.getSearchDongCd());
        }

        sql.append("                         AND FULL_CONTRACT_DATE BETWEEN ? AND ? ");
        params.add(myHomePriceFilterCondition.getContractDate());
        params.add(myHomePriceFilterCondition.getContractDateTo());

        sql.append("                         AND SQUARE_METER BETWEEN ? AND ? ");
        params.add(myHomePriceFilterCondition.getSearchAreaValue());
        params.add(myHomePriceFilterCondition.getSearchAreaValueTo());

        sql.append("                         AND AMOUNT BETWEEN ? AND ? ");
        params.add(myHomePriceFilterCondition.getSearchFromAmount());
        params.add(myHomePriceFilterCondition.getSearchToAmnount());

        sql.append("     ) A ");
        sql.append("         WHERE A.RN = 1 ");
        sql.append(" ) B ");
        sql.append("         WHERE B.NO BETWEEN ? AND ? ");
        sql.append("         ORDER BY B.CITY, B.LAYER ");

        params.add(myHomePriceFilterCondition.getStartRec());
        params.add(myHomePriceFilterCondition.getEndRec());

        List<MyHomePrice> list = jdbcTemplate.query(sql.toString(),
                new BeanPropertyRowMapper<>(MyHomePrice.class),
                params.toArray());

        log.info("list123 = {}", list);

        return list;
    }


    /**
     * 전체건수
     *
     * @return
     */
    @Override
    public int totalCount(MyHomePriceFilterCondition myHomePriceFilterCondition) {
        StringBuffer sql = new StringBuffer();
        log.info("myHomePriceFilterCondition123 = {}", myHomePriceFilterCondition);

        sql.append(" SELECT count(*) as COUNT FROM( ");
        sql.append("   SELECT ");
        sql.append("   ROW_NUMBER() OVER (PARTITION BY A.CITY ,A.STREET ,A.BON_BUN ,A.BU_BUN ,A.DAN_GI_MYEONG ,A.SQUARE_METER ,A.LAYER ,A.CONSTRUCTION_DATE ORDER BY A.CONTRACT_DATE DESC) AS RN  ");
        sql.append("   FROM APT A ");
        sql.append("   WHERE 1 = 1  ");
        sql.append("   AND CITY_CODE = ? ");

        List<Object> params = new ArrayList<>();
        params.add(myHomePriceFilterCondition.getSearchSidoCd());

        if (!myHomePriceFilterCondition.getSearchGugunCd().equals("false")) {
            sql.append(" AND COUNTY_CODE = ? ");
            params.add(myHomePriceFilterCondition.getSearchGugunCd());
        }

        if (!myHomePriceFilterCondition.getSearchDongCd().equals("false")) {
            sql.append(" AND DISTRICTS_CODE = ? ");
            params.add(myHomePriceFilterCondition.getSearchDongCd());
        }

        sql.append("   AND FULL_CONTRACT_DATE BETWEEN ? AND ?  ");
        params.add(myHomePriceFilterCondition.getContractDate());
        params.add(myHomePriceFilterCondition.getContractDateTo());

        sql.append("   AND SQUARE_METER  BETWEEN ? AND ?  ");
        params.add(myHomePriceFilterCondition.getSearchAreaValue());
        params.add(myHomePriceFilterCondition.getSearchAreaValueTo());

        sql.append("   AND AMOUNT BETWEEN ? AND ?  ");
        params.add(myHomePriceFilterCondition.getSearchFromAmount());
        params.add(myHomePriceFilterCondition.getSearchToAmnount());

        sql.append(" ) A ");
        sql.append(" WHERE A.RN= 1 ");

        Integer cnt = jdbcTemplate.queryForObject(
                sql.toString(), Integer.class,
                params.toArray()
        );

        log.info("cnt1 = {}", cnt);

        return cnt;
    }


    /**
     * APT 상세조회 폼
     *
     * @param apt_id
     * @return
     */
    @Override
    public MyHomePrice selectMyHomePriceDetailForm(Long apt_id) {

        StringBuffer sql = new StringBuffer();

        sql.append(" SELECT * FROM APT ");
        sql.append(" WHERE APT_ID = ? ");

//    MyHomePrice MyHomePriceItem = null;
//    try {
//      MyHomePriceItem = jdbcTemplate.queryForObject(
//          sql.toString(),
//          new BeanPropertyRowMapper<>(MyHomePrice.class),
//          myHomePriceFilterCondition.getApt_id()
//      );
//    } catch (Exception e) { //1건을 못찾으면
//      MyHomePriceItem = null;
//    }

        MyHomePrice MyHomePriceItem = jdbcTemplate.queryForObject(
                sql.toString(),
                new BeanPropertyRowMapper<>(MyHomePrice.class),
                apt_id
        );

//    log.info("MyHomePriceItem = {}", MyHomePriceItem);

        return MyHomePriceItem;
    }

    /**
     * APT 상세조회 리스트
     *
     * @param apt_id
     * @return
     */
    @Override
    public List<MyHomePrice> selectMyHomePriceDetail(Long apt_id) {
        StringBuffer sql = new StringBuffer();

        sql.append(" SELECT A.* ");
        sql.append(" FROM APT A, (SELECT * ");
        sql.append("              FROM APT B ");
        sql.append("              WHERE APT_ID = ? ) B ");
        sql.append(" WHERE 1 = 1 ");
        sql.append(" AND A.CITY = B.CITY  ");
        sql.append(" AND A.STREET = B.STREET  ");
        sql.append(" AND A.BON_BUN = B.BON_BUN  ");
        sql.append(" AND A.BU_BUN = B.BU_BUN  ");
        sql.append(" AND A.DAN_GI_MYEONG = B.DAN_GI_MYEONG  ");
        sql.append(" AND A.SQUARE_METER = B.SQUARE_METER  ");
        sql.append(" AND A.LAYER = B.LAYER  ");
        sql.append(" AND A.CONSTRUCTION_DATE = B.CONSTRUCTION_DATE  ");
        sql.append(" ORDER BY FULL_CONTRACT_DATE DESC ");

        List<MyHomePrice> detaillist = jdbcTemplate.query(sql.toString(),
                new BeanPropertyRowMapper<>(MyHomePrice.class),
                apt_id
        );

//    log.info("detaillist = {}", detaillist);

        return detaillist;
    }

    /**
     * APT 상세조회 ScatterChart
     *
     * @param apt_id
     * @return
     */
    @Override
    public List<MyHomePrice> selectMyHomePriceScatterChart(Long apt_id) {
        StringBuffer sql = new StringBuffer();
//    sql.append(" SELECT CONCAT(A.CONTRACT_DATE, LPAD(A.CONTRACT_DAY, 2, '0')) as x, TRIM(REPLACE(A.AMOUNT, ',', '')) as y ");
        sql.append(" SELECT REPLACE(A.FULL_CONTRACT_DATE,'-','') as x, A.AMOUNT as y ");
        sql.append(" FROM APT A, (SELECT * ");
        sql.append("              FROM APT B ");
        sql.append("              WHERE APT_ID = ? ) B ");
        sql.append(" WHERE 1 = 1 ");
        sql.append(" AND A.CITY = B.CITY ");
        sql.append(" AND A.STREET = B.STREET ");
        sql.append(" AND A.BON_BUN = B.BON_BUN ");
        sql.append(" AND A.BU_BUN = B.BU_BUN ");
        sql.append(" AND A.DAN_GI_MYEONG = B.DAN_GI_MYEONG ");
        sql.append(" AND A.SQUARE_METER = B.SQUARE_METER ");
        sql.append(" AND A.LAYER = B.LAYER ");
        sql.append(" AND A.CONSTRUCTION_DATE = B.CONSTRUCTION_DATE ");

        List<MyHomePrice> ScatterChart = jdbcTemplate.query(sql.toString(),
                new BeanPropertyRowMapper<>(MyHomePrice.class),
                apt_id
        );

//    log.info("ScatterChart = {}", ScatterChart);

        return ScatterChart;
    }


}
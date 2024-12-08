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
        sql.append(" select b.* ");
        sql.append("       ,get_transactioncount_list(b.city, b.street, b.bon_bun, b.bu_bun, b.dan_gi_myeong, b.square_meter, b.layer, b.construction_date, b.city_code) as transactioncountlist ");
        sql.append("         from ( ");
        sql.append("                 select a.* ,row_number() over (order by full_contract_date desc) as no ");
        sql.append("                 from( ");
        sql.append("                         select ");
        sql.append("                         row_number() over (partition by a.city, a.street, a.bon_bun, a.bu_bun, a.dan_gi_myeong, a.square_meter, a.layer order by a.full_contract_date desc) as rn, ");
        sql.append("                         a.* ");
        sql.append("                                 from apt a ");
        sql.append("                         where 1 = 1 ");
        sql.append("                         and city_code = ? ");

        List<Object> params = new ArrayList<>();
        params.add(myHomePriceFilterCondition.getSearchSidoCd());

        if (!myHomePriceFilterCondition.getSearchGugunCd().equals("false")) {
            sql.append(" and county_code = ? ");
            params.add(myHomePriceFilterCondition.getSearchGugunCd());
        }

        if (!myHomePriceFilterCondition.getSearchDongCd().equals("false")) {
            sql.append(" and districts_code = ? ");
            params.add(myHomePriceFilterCondition.getSearchDongCd());
        }

        sql.append("                         and full_contract_date between ? and ? ");
        params.add(myHomePriceFilterCondition.getContractDate());
        params.add(myHomePriceFilterCondition.getContractDateTo());

        sql.append("                         and square_meter between ? and ? ");
        params.add(myHomePriceFilterCondition.getSearchAreaValue());
        params.add(myHomePriceFilterCondition.getSearchAreaValueTo());

        sql.append("                         and amount between ? and ? ");
        params.add(myHomePriceFilterCondition.getSearchFromAmount());
        params.add(myHomePriceFilterCondition.getSearchToAmnount());

        sql.append("     ) a ");
        sql.append("         where a.rn = 1 ");
        sql.append(" ) b ");
        sql.append("         where b.no between ? and ? ");
        sql.append("         order by b.city, b.layer ");

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

        sql.append(" select count(*) as count from( ");
        sql.append("   select ");
        sql.append("   row_number() over (partition by a.city ,a.street ,a.bon_bun ,a.bu_bun ,a.dan_gi_myeong ,a.square_meter ,a.layer ,a.construction_date order by a.contract_date desc) as rn  ");
        sql.append("   from apt a ");
        sql.append("   where 1 = 1  ");
        sql.append("   and city_code = ? ");

        List<Object> params = new ArrayList<>();
        params.add(myHomePriceFilterCondition.getSearchSidoCd());

        if (!myHomePriceFilterCondition.getSearchGugunCd().equals("false")) {
            sql.append(" and county_code = ? ");
            params.add(myHomePriceFilterCondition.getSearchGugunCd());
        }

        if (!myHomePriceFilterCondition.getSearchDongCd().equals("false")) {
            sql.append(" and districts_code = ? ");
            params.add(myHomePriceFilterCondition.getSearchDongCd());
        }

        sql.append("   and full_contract_date between ? and ?  ");
        params.add(myHomePriceFilterCondition.getContractDate());
        params.add(myHomePriceFilterCondition.getContractDateTo());

        sql.append("   and square_meter  between ? and ?  ");
        params.add(myHomePriceFilterCondition.getSearchAreaValue());
        params.add(myHomePriceFilterCondition.getSearchAreaValueTo());

        sql.append("   and amount between ? and ?  ");
        params.add(myHomePriceFilterCondition.getSearchFromAmount());
        params.add(myHomePriceFilterCondition.getSearchToAmnount());

        sql.append(" ) a ");
        sql.append(" where a.rn= 1 ");

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

        sql.append(" select * from apt ");
        sql.append(" where apt_id = ? ");

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

        sql.append(" select a.* ");
        sql.append(" from apt a, (select * ");
        sql.append("              from apt b ");
        sql.append("              where apt_id = ? ) b ");
        sql.append(" where 1 = 1 ");
        sql.append(" and a.city = b.city  ");
        sql.append(" and a.street = b.street  ");
        sql.append(" and a.bon_bun = b.bon_bun  ");
        sql.append(" and a.bu_bun = b.bu_bun  ");
        sql.append(" and a.dan_gi_myeong = b.dan_gi_myeong  ");
        sql.append(" and a.square_meter = b.square_meter  ");
        sql.append(" and a.layer = b.layer  ");
        sql.append(" and a.construction_date = b.construction_date  ");
        sql.append(" order by full_contract_date desc ");

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
        sql.append(" select replace(a.full_contract_date,'-','') as x, a.amount as y ");
        sql.append(" from apt a, (select * ");
        sql.append("              from apt b ");
        sql.append("              where apt_id = ? ) b ");
        sql.append(" where 1 = 1 ");
        sql.append(" and a.city = b.city ");
        sql.append(" and a.street = b.street ");
        sql.append(" and a.bon_bun = b.bon_bun ");
        sql.append(" and a.bu_bun = b.bu_bun ");
        sql.append(" and a.dan_gi_myeong = b.dan_gi_myeong ");
        sql.append(" and a.square_meter = b.square_meter ");
        sql.append(" and a.layer = b.layer ");
        sql.append(" and a.construction_date = b.construction_date ");

        List<MyHomePrice> ScatterChart = jdbcTemplate.query(sql.toString(),
                new BeanPropertyRowMapper<>(MyHomePrice.class),
                apt_id
        );

//    log.info("ScatterChart = {}", ScatterChart);

        return ScatterChart;
    }


}
package com.ybkim.AptPrice.domain.AptScheduler.dao;


import com.ybkim.AptPrice.domain.AptScheduler.AptApiDb;
import com.ybkim.AptPrice.domain.AptScheduler.CountyCode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.List;

@Slf4j
@Repository
@RequiredArgsConstructor

public class AptApiDbDAOImpl implements AptApiDbDAO {

    private final JdbcTemplate jdbcTemplate;

    /**
     * API -> DB insert
     *
     * @return
     */

    @Override
    public void insertApiDb(AptApiDb aptApiDb) {
        StringBuilder sql = new StringBuilder();

        sql.append("  insert into apt ( ");
        sql.append("          city, street, bon_bun, bu_bun, dan_gi_myeong, square_meter, contract_date, ");
        sql.append("          contract_day, amount, layer, construction_date, road_name, reason_cancellation_date, ");
        sql.append("          registration_creation, transaction_type, location_agency, full_contract_date, ");
        sql.append("          city_code, county_code, districts_code ");
        sql.append("  ) ");
        sql.append("  select ");
        sql.append("  new.city, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ");
        sql.append("  rct.city_code, rc.county_code, rd.districts_code ");
        sql.append("  from (select ? as city) as new ");
        sql.append("  left join region_city rct on substring_index(new.city, ' ', 1) = rct.city_nm ");
        sql.append("  left join region_county rc on substring(substring_index(substring_index(new.city, ' ', 2), ' ', -1), 1, 2) = substring(rc.county_nm, 1, 2) ");
        sql.append("  and substring(rc.county_code, 1, 2) = substring(rct.city_code, 1, 2) ");
        sql.append("  left join region_districts rd on substring_index(new.city, ' ', -1) like concat('%', substring_index(rd.districts_nm, ' ', -1), '%') ");
        sql.append("  and substring(rd.districts_code, 1, 3) = substring(rc.county_code, 1, 3) ");
        sql.append("  on duplicate key update ");
        sql.append("  city = values(city), ");
        sql.append("          street = values(street), ");
        sql.append("          bon_bun = values(bon_bun), ");
        sql.append("          bu_bun = values(bu_bun), ");
        sql.append("          dan_gi_myeong = values(dan_gi_myeong), ");
        sql.append("          square_meter = values(square_meter), ");
        sql.append("          contract_date = values(contract_date), ");
        sql.append("          contract_day = values(contract_day), ");
        sql.append("          amount = values(amount), ");
        sql.append("          layer = values(layer), ");
        sql.append("          construction_date = values(construction_date), ");
        sql.append("          road_name = values(road_name), ");
        sql.append("          reason_cancellation_date = values(reason_cancellation_date), ");
        sql.append("          registration_creation = values(registration_creation), ");
        sql.append("          transaction_type = values(transaction_type), ");
        sql.append("          location_agency = values(location_agency), ");
        sql.append("          full_contract_date = values(full_contract_date) ");

        jdbcTemplate.update(
                sql.toString(),
//                aptApiDb.getCity(),
                aptApiDb.getStreet(),
                aptApiDb.getBon_bun(),
                aptApiDb.getBu_bun(),
                aptApiDb.getDan_gi_myeong(),
                aptApiDb.getSquare_meter(),
                aptApiDb.getContract_date(),
                aptApiDb.getContract_day(),
                aptApiDb.getAmount(),
                aptApiDb.getLayer(),
                aptApiDb.getConstruction_date(),
                aptApiDb.getRoad_name(),
                aptApiDb.getReason_cancellation_date(),
                aptApiDb.getRegistration_creation(), //등기일자 추가
                aptApiDb.getTransaction_type(),
                aptApiDb.getLocation_agency(),
                aptApiDb.getFullContractDate(), // 'FULL_CONTRACT_DATE' 필드를 위한 getter 추가
                aptApiDb.getCity()
        );

    }


    /**
     * 시군구 조회
     *
     * @return
     */
    @Override
    public List<CountyCode> apiSelectRegionCounty() {
        StringBuffer sql = new StringBuffer();

        sql.append(" select county_code from region_county ");

        List<CountyCode> list = jdbcTemplate.query(sql.toString(),
                new BeanPropertyRowMapper<>(CountyCode.class)
        );
        return list;
    }

    /**
     * 시군구 조회
     *
     * @return
     */
    @Override
    public CountyCode selectCity(String city) {
        StringBuffer sql = new StringBuffer();
        String cityCodePrefix = city.substring(0, 2);

        sql.append(" select city_nm as city from region_city where city_code like concat(? ,'%') ");

        CountyCode cityNm = jdbcTemplate.queryForObject(sql.toString(),
                new BeanPropertyRowMapper<>(CountyCode.class), cityCodePrefix
        );
        return cityNm;
    }

    /**
     * 시군구 조회
     *
     * @return
     */
    @Override
    public CountyCode selectCounty(String county) {
        StringBuffer sql = new StringBuffer();

        // 첫 번째 쿼리
        sql.append("select county_nm as county from region_county where county_code = ?");
        try {
            CountyCode countyMm = jdbcTemplate.queryForObject(sql.toString(),
                    new BeanPropertyRowMapper<>(CountyCode.class), county);
            return countyMm;
        } catch (EmptyResultDataAccessException e) {
            // 첫 번째 쿼리 결과가 없을 경우 두 번째 쿼리 실행
//            String countyCodePrefix = county.substring(0, 5);
//            sql = new StringBuffer();  // 쿼리 문자열 초기화
//            sql.append("SELECT CITY_NM as county FROM region_city WHERE CITY_CODE LIKE CONCAT(?, '%')");
//            try {
//                return jdbcTemplate.queryForObject(sql.toString(),
//                        new BeanPropertyRowMapper<>(CountyCode.class), countyCodePrefix);
//            } catch (EmptyResultDataAccessException ex) {
//                // 두 번째 쿼리 결과도 없을 경우 null 반환
//                return null;
//            }
            return null;
        }
    }

    @Override
    public String startDate() {
        // SQL 쿼리 작성
        StringBuffer sql = new StringBuffer();
        sql.append(" select full_contract_date + interval 1 day as fullcontractdate ");
        sql.append(" from apt ");
        sql.append(" order by full_contract_date desc ");
        sql.append(" limit 1 ");

        // 쿼리 실행 및 결과 매핑
        try {
            // 단일 값을 반환하는 queryForObject 사용
            return jdbcTemplate.queryForObject(
                    sql.toString(),
                    String.class // FullContractDate가 String 타입으로 반환되도록 변경
            );
        } catch (EmptyResultDataAccessException e) {
            // 결과가 없는 경우 null 반환
            return null;
        }
    }



}
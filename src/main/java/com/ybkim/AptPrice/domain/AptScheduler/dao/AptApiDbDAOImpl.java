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
//    System.out.println("aptApiDb = " + aptApiDb);
        StringBuilder sql = new StringBuilder();
//        sql.append(" INSERT INTO apt ( ");
//        sql.append("     apt_id, city, street, bon_bun, bu_bun, dan_gi_myeong, ");
//        sql.append("     square_meter, contract_date, contract_day, amount, ");
//        sql.append("     layer, construction_date, road_name, reason_cancellation_date, ");
//        sql.append("     transaction_type, location_agency, full_contract_date ");
//        sql.append(" ) VALUES ( ");
//        sql.append("     NULL, ?, ?, ?, ?, ?, ");  // 'apt_id'는 AUTO_INCREMENT이므로 NULL을 사용합니다
//        sql.append("     ?, ?, ?, ?, ");
//        sql.append("     ?, ?, ?, ?, ");
//        sql.append("     ?, ?, ? ");  // 'full_contract_date' 필드 추가
//        sql.append(" ) ON DUPLICATE KEY UPDATE ");
//        sql.append("     city = VALUES(city), street = VALUES(street), bon_bun = VALUES(bon_bun), ");
//        sql.append("     bu_bun = VALUES(bu_bun), dan_gi_myeong = VALUES(dan_gi_myeong), ");
//        sql.append("     square_meter = VALUES(square_meter), contract_date = VALUES(contract_date), ");
//        sql.append("     contract_day = VALUES(contract_day), amount = VALUES(amount), ");
//        sql.append("     layer = VALUES(layer), construction_date = VALUES(construction_date), ");
//        sql.append("     road_name = VALUES(road_name), reason_cancellation_date = VALUES(reason_cancellation_date), ");
//        sql.append("     transaction_type = VALUES(transaction_type), location_agency = VALUES(location_agency), ");
//        sql.append("     full_contract_date = VALUES(full_contract_date)");


//        sql.append("  INSERT INTO apt_test ( ");
        sql.append("  INSERT INTO apt ( ");
        sql.append("          CITY, STREET, BON_BUN, BU_BUN, DAN_GI_MYEONG, SQUARE_METER, CONTRACT_DATE, ");
        sql.append("          CONTRACT_DAY, AMOUNT, LAYER, CONSTRUCTION_DATE, ROAD_NAME, REASON_CANCELLATION_DATE, ");
        sql.append("          REGISTRATION_CREATION, TRANSACTION_TYPE, LOCATION_AGENCY, FULL_CONTRACT_DATE, ");
        sql.append("          CITY_CODE, COUNTY_CODE, DISTRICTS_CODE ");
        sql.append("  ) ");
        sql.append("  SELECT ");
        sql.append("  NEW.city, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ");
        sql.append("  rct.CITY_CODE, rc.COUNTY_CODE, rd.DISTRICTS_CODE ");
        sql.append("  FROM (SELECT ? AS city) AS NEW ");
        sql.append("  LEFT JOIN region_city rct ON SUBSTRING_INDEX(NEW.city, ' ', 1) = rct.CITY_NM ");
        sql.append("  LEFT JOIN region_county rc ON SUBSTRING(SUBSTRING_INDEX(SUBSTRING_INDEX(NEW.city, ' ', 2), ' ', -1), 1, 2) = SUBSTRING(rc.COUNTY_NM, 1, 2) ");
        sql.append("  AND SUBSTRING(rc.COUNTY_CODE, 1, 2) = SUBSTRING(rct.CITY_CODE, 1, 2) ");
        sql.append("  LEFT JOIN region_districts rd ON SUBSTRING_INDEX(NEW.city, ' ', -1) LIKE CONCAT('%', SUBSTRING_INDEX(rd.DISTRICTS_NM, ' ', -1), '%') ");
        sql.append("  AND SUBSTRING(rd.DISTRICTS_CODE, 1, 3) = SUBSTRING(rc.COUNTY_CODE, 1, 3) ");
        sql.append("  ON DUPLICATE KEY UPDATE ");
        sql.append("  CITY = VALUES(CITY), ");
        sql.append("          STREET = VALUES(STREET), ");
        sql.append("          BON_BUN = VALUES(BON_BUN), ");
        sql.append("          BU_BUN = VALUES(BU_BUN), ");
        sql.append("          DAN_GI_MYEONG = VALUES(DAN_GI_MYEONG), ");
        sql.append("          SQUARE_METER = VALUES(SQUARE_METER), ");
        sql.append("          CONTRACT_DATE = VALUES(CONTRACT_DATE), ");
        sql.append("          CONTRACT_DAY = VALUES(CONTRACT_DAY), ");
        sql.append("          AMOUNT = VALUES(AMOUNT), ");
        sql.append("          LAYER = VALUES(LAYER), ");
        sql.append("          CONSTRUCTION_DATE = VALUES(CONSTRUCTION_DATE), ");
        sql.append("          ROAD_NAME = VALUES(ROAD_NAME), ");
        sql.append("          REASON_CANCELLATION_DATE = VALUES(REASON_CANCELLATION_DATE), ");
        sql.append("          REGISTRATION_CREATION = VALUES(REGISTRATION_CREATION), ");
        sql.append("          TRANSACTION_TYPE = VALUES(TRANSACTION_TYPE), ");
        sql.append("          LOCATION_AGENCY = VALUES(LOCATION_AGENCY), ");
        sql.append("          FULL_CONTRACT_DATE = VALUES(FULL_CONTRACT_DATE) ");

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


//  @Override
//  public void insertApiDb(AptApiDb  aptApiDb) {
//    StringBuffer sql = new StringBuffer();
//    System.out.println("aptApiDb = " + aptApiDb);
//    sql.append(" INSERT INTO apt ( ");
//    sql.append("     apt_id, city, street, bon_bun, bu_bun, dan_gi_myeong,  ");
//    sql.append("     square_meter, contract_date, contract_day, amount, ");
//    sql.append("     layer, construction_date, road_name, reason_cancellation_date, ");
//    sql.append("     transaction_type, location_agency ");
//    sql.append(" ) VALUES ( ");
//    sql.append("     NEXTVAL(apt_id_seq), ?, ?, ?, ?, ?, ");
//    sql.append("     ?, ?, ?, ?, ");
//    sql.append("     ?, ?, ?, ?, ");
//    sql.append("     ?, ? ");
//    sql.append(" ) ON DUPLICATE KEY UPDATE ");
//    sql.append("     city = VALUES(city), street = VALUES(street), bon_bun = VALUES(bon_bun), ");
//    sql.append("     bu_bun = VALUES(bu_bun), dan_gi_myeong = VALUES(dan_gi_myeong), ");
//    sql.append("     square_meter = VALUES(square_meter), contract_date = VALUES(contract_date), ");
//    sql.append("     contract_day = VALUES(contract_day), amount = VALUES(amount), ");
//    sql.append("     layer = VALUES(layer), construction_date = VALUES(construction_date), ");
//    sql.append("     road_name = VALUES(road_name), reason_cancellation_date = VALUES(reason_cancellation_date), ");
//    sql.append("     transaction_type = VALUES(transaction_type), location_agency = VALUES(location_agency);");
//
//
//
//
////    sql.append(" INSERT INTO apt ( ");
////    sql.append("     apt_id, city, street, bon_bun, bu_bun, dan_gi_myeong,  ");
////    sql.append("     square_meter, contract_date, contract_day, amount, ");
////    sql.append("     layer, construction_date, road_name, reason_cancellation_date, ");
////    sql.append("     transaction_type, location_agency ");
////    sql.append(" ) VALUES ( ");
////    sql.append("     NEXT VALUE FOR apt_id_seq, ?, ?, ?, ?, ?, ");
////    sql.append("     ?, ?, ?, ?, ");
////    sql.append("     ?, ?, ?, ?, ");
////    sql.append("     ?, ? ");
////    sql.append(" ) ");
//
//    //배치 처리 : 여러건의 갱신작업을 한꺼번에 처리하므로 단건처리할때보다 성능이 좋다.
//    jdbcTemplate.batchUpdate(sql.toString(), new BatchPreparedStatementSetter() {
//      @Override
//      public void setValues(PreparedStatement ps, int i) throws SQLException {
//        ps.setString(1, aptApiDb.getCity());
//        ps.setString(2, aptApiDb.getStreet());
//        ps.setString(3, aptApiDb.getBon_bun());
//        ps.setString(4, aptApiDb.getBu_bun());
//        ps.setString(5, aptApiDb.getDan_gi_myeong());
//        ps.setString(6, aptApiDb.getSquare_meter());
//        ps.setString(7, aptApiDb.getContract_date());
//        ps.setString(8, aptApiDb.getContract_day());
//        ps.setString(9, aptApiDb.getAmount());
//        ps.setString(10, aptApiDb.getLayer());
//        ps.setString(11, aptApiDb.getConstruction_date());
//        ps.setString(12, aptApiDb.getRoad_name());
//        ps.setString(13, aptApiDb.getReason_cancellation_date());
//        ps.setString(14, aptApiDb.getTransaction_type());
//        ps.setString(15, aptApiDb.getLocation_agency());
//      }
//
//      //배치처리할 건수
//      @Override
//      public int getBatchSize() {
////        return aptApiDb.size();
//        return 1;
//      }
//
//    });
//
//  }

    /**
     * 시군구 조회
     *
     * @return
     */
    @Override
    public List<CountyCode> apiSelectRegionCounty() {
        StringBuffer sql = new StringBuffer();

        sql.append(" SELECT COUNTY_CODE FROM REGION_COUNTY ");

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

        sql.append(" SELECT CITY_NM as city FROM region_city WHERE CITY_CODE LIKE CONCAT(? ,'%') ");

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
        sql.append("SELECT COUNTY_NM as county FROM region_county WHERE COUNTY_CODE = ?");
        try {
            CountyCode countyMm = jdbcTemplate.queryForObject(sql.toString(),
                    new BeanPropertyRowMapper<>(CountyCode.class), county);
            return countyMm;
        } catch (EmptyResultDataAccessException e) {
            // 첫 번째 쿼리 결과가 없을 경우 두 번째 쿼리 실행
            String countyCodePrefix = county.substring(0, 5);
            sql = new StringBuffer();  // 쿼리 문자열 초기화
            sql.append("SELECT COUNTY_NM as county FROM region_county WHERE COUNTY_CODE LIKE CONCAT(?, '%')");
            try {
                return jdbcTemplate.queryForObject(sql.toString(),
                        new BeanPropertyRowMapper<>(CountyCode.class), countyCodePrefix);
            } catch (EmptyResultDataAccessException ex) {
                // 두 번째 쿼리 결과도 없을 경우 null 반환
                return null;
            }
        }
    }


}
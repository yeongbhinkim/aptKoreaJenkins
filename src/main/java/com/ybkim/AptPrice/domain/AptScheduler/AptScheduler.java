package com.ybkim.AptPrice.domain.AptScheduler;

import com.ybkim.AptPrice.domain.AptScheduler.svc.AptApiDbSVC;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

@Configuration
@EnableScheduling
public class AptScheduler {

    @Autowired
    AptApiDbSVC aptApiDbSVC;

    @Value("${external.api.molit.serviceKey}")
    private String serviceKey;

//    @Scheduled(cron = "0 57 20 * * ?")
    @Scheduled(cron = "0 34 23 * * ?")
    public void downloadCsvFile() {
        System.out.println("CSV 다운로드 작업 시작");
        List<AptApiDb> transactionsList = new ArrayList<>();

        // Trust manager setup to ignore certificate validation (unsafe for production)
        TrustManager[] trustAllCerts = new TrustManager[]{
                new X509TrustManager() {
                    public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                        return null;
                    }

                    public void checkClientTrusted(
                            java.security.cert.X509Certificate[] certs, String authType) {
                    }

                    public void checkServerTrusted(
                            java.security.cert.X509Certificate[] certs, String authType) {
                    }
                }
        };

        try {
            // Install the all-trusting trust manager
            SSLContext sc = SSLContext.getInstance("SSL");
            sc.init(null, trustAllCerts, new java.security.SecureRandom());
            HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            // 요청할 URL
            URL url = new URL("https://rtdown.molit.go.kr/rtms/rqs/rtAptTrCSV.do");
            HttpURLConnection con = (HttpURLConnection) url.openConnection();

            // HTTP POST 메서드 설정
            con.setRequestMethod("POST");

            // 헤더 설정
//            con.setRequestProperty("Accept-Language", "ko,en;q=0.9,ko-KR;q=0.8,en-US;q=0.7");
//            con.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");

            // POST 요청에 필요한 파라미터 설정
            // POST 요청에 필요한 파라미터 설정
            String urlParameters = "cal_url=" + URLEncoder.encode("/sym/cmm/EgovNormalCalPopup.do", StandardCharsets.UTF_8.name()) +
                    "&totCnt=" + URLEncoder.encode("0", StandardCharsets.UTF_8.name()) +
                    "&exlTotCnt=" + URLEncoder.encode("0", StandardCharsets.UTF_8.name()) +
                    "&captchaCheck=" + URLEncoder.encode("true", StandardCharsets.UTF_8.name()) +
                    "&sidoNm=" + URLEncoder.encode("전체", StandardCharsets.UTF_8.name()) +
                    "&sigunguNm=" + URLEncoder.encode("전체", StandardCharsets.UTF_8.name()) +
                    "&umdNm=" + URLEncoder.encode("전체", StandardCharsets.UTF_8.name()) +
                    "&areaNm=" + URLEncoder.encode("전체", StandardCharsets.UTF_8.name()) +
                    "&loadNm=" + URLEncoder.encode("도로", StandardCharsets.UTF_8.name()) +
                    "&searchFromDt=" + URLEncoder.encode("20231101", StandardCharsets.UTF_8.name()) +
                    "&searchToDt=" + URLEncoder.encode("20231130", StandardCharsets.UTF_8.name()) +
                    "&fileType=" + URLEncoder.encode("CSV", StandardCharsets.UTF_8.name()) +
                    "&searchRtGubunCd=" + URLEncoder.encode("AT", StandardCharsets.UTF_8.name()) +
                    "&gubunRadio2=" + URLEncoder.encode("1", StandardCharsets.UTF_8.name()) +
                    "&searchSidoCd=" + URLEncoder.encode("ALL", StandardCharsets.UTF_8.name()) +
                    "&searchGugunCd=" + URLEncoder.encode("ALL", StandardCharsets.UTF_8.name()) +
                    "&searchDongCd=" + URLEncoder.encode("ALL", StandardCharsets.UTF_8.name()) +
                    "&searchChosung=" + URLEncoder.encode("ALL", StandardCharsets.UTF_8.name()) +
                    "&searchLoad=" + URLEncoder.encode("ALL", StandardCharsets.UTF_8.name()) +
                    "&searchDanjiCd=" + URLEncoder.encode("ALL", StandardCharsets.UTF_8.name()) +
                    "&bungiSn=" + URLEncoder.encode("ALL", StandardCharsets.UTF_8.name()) +
                    "&searchArea=" + URLEncoder.encode("ALL", StandardCharsets.UTF_8.name()) +
                    "&searchGb=" + URLEncoder.encode("2", StandardCharsets.UTF_8.name());

            // POST 요청을 위한 설정
            con.setDoOutput(true);
            try (DataOutputStream wr = new DataOutputStream(con.getOutputStream())) {
                wr.writeBytes(urlParameters);
                wr.flush();
            }

            // 응답 코드 가져오기
            int responseCode = con.getResponseCode();
            System.out.println("Response Code : " + responseCode);

            // 응답 받기
            try (BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream(), "EUC-KR"))) {
                String inputLine;
                StringBuilder response = new StringBuilder();
                int lineCount = 0;

                while ((inputLine = in.readLine()) != null) {
                    if (lineCount++ >= 16) {
                        response.append(inputLine).append("\n");
                    }
                }

//                System.out.println("response = " + response);
                // 받은 응답을 CSV 파서로 파싱
//                String[] records = response.toString().split("\"");
//                System.out.println("records = " + records);
//                for (String record : records) {
                // 레코드가 큰따옴표로 시작한다면, 첫 번째 큰따옴표를 제거합니다.
//                    String cleanedRecord = record.startsWith("\"") ? record.substring(1) : record;
//                    cleanedRecord = cleanedRecord.endsWith("\"") ? cleanedRecord.substring(0, cleanedRecord.length() - 1) : cleanedRecord;
                // CSV 파서를 사용하여 레코드 파싱
//                    CSVParser parser = CSVParser.parse(cleanedRecord, CSVFormat.DEFAULT
                CSVParser parser = CSVParser.parse(response.toString(), CSVFormat.DEFAULT
                        .withHeader("city", "street", "bon_bun", "bu_bun", "dan_gi_myeong", "square_meter",
                                "contract_date", "contract_day", "amount", "layer", "construction_date",
                                "road_name", "reason_cancellation_date", "registration_creation", "transaction_type",
                                "location_agency")
                        .withSkipHeaderRecord()
                        .withDelimiter(',')
                        .withQuote('"')); // 필드 값이 큰따옴표로 감싸져 있으므로 인용 부호 설정

                for (CSVRecord csvRecord : parser) {
                    AptApiDb transaction = new AptApiDb();
//                    System.out.println("csvRecord = " + csvRecord);
                    transaction.setCity(csvRecord.get("city"));
                    transaction.setStreet(csvRecord.get("street"));
                    transaction.setBon_bun(csvRecord.get("bon_bun"));
                    transaction.setBu_bun(csvRecord.get("bu_bun"));
                    transaction.setDan_gi_myeong(csvRecord.get("dan_gi_myeong"));
                    transaction.setSquare_meter(Float.parseFloat(csvRecord.get("square_meter")));
                    transaction.setContract_date(csvRecord.get("contract_date"));
                    transaction.setContract_day(csvRecord.get("contract_day"));
                    String amountStr = csvRecord.get("amount").replace(",", "");
                    transaction.setAmount(Integer.parseInt(amountStr));
//                    transaction.setAmount(Integer.parseInt(csvRecord.get("amount")));
                    transaction.setLayer(Integer.parseInt(csvRecord.get("layer")));
                    transaction.setConstruction_date(csvRecord.get("construction_date"));
                    transaction.setRoad_name(csvRecord.get("road_name"));
                    transaction.setReason_cancellation_date(csvRecord.get("reason_cancellation_date"));
                    transaction.setRegistration_creation(csvRecord.get("registration_creation"));
                    transaction.setTransaction_type(csvRecord.get("transaction_type"));
                    transaction.setLocation_agency(csvRecord.get("location_agency"));
                    String contractDay = csvRecord.get("contract_day");
                    // 숫자로 변환한 후, 두 자리 형식으로 문자열 포맷팅
                    contractDay = String.format("%02d", Integer.parseInt(contractDay));
                    transaction.setFullContractDate(csvRecord.get("contract_date") + contractDay);
//                    System.out.println("transaction = " + transaction);
                    transactionsList.add(transaction);
                }
            }
//            }
//            System.out.println("transactionsList " + transactionsList);
            System.out.println("Transactions count: " + transactionsList.size());
            // 리스트에 저장된 데이터 확인
            for (AptApiDb transaction : transactionsList) {
                //여기에 인설트 넣으면 될듯?
                aptApiDbSVC.ApiDb(transaction);
                System.out.println("transaction = " + transaction);
            }
            System.out.println("==========완료==========");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


//    @Scheduled(cron = "0 10 20 * * ?")

    //    @Scheduled(cron = "0 0 0 1 * ?")
//    @Scheduled(cron = "0 34 23 * * ?")
    public void myScheduledMethod() throws IOException, ParseException, ParserConfigurationException, SAXException {

//        LocalDate startDate = LocalDate.of(2006, 1, 1);
        LocalDate currentDate = LocalDate.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMM");

//        for (LocalDate date = startDate; date.isBefore(currentDate.plusMonths(1)); date = date.plusMonths(1)) {
        String formattedDate = currentDate.format(formatter);
        System.out.println("formattedDate = " + formattedDate);


        List<CountyCode> apiRegionCounty = aptApiDbSVC.apiRegionCounty();

        for (int i = 0; i < apiRegionCounty.size(); i++) {
            CountyCode county = apiRegionCounty.get(i);
            System.out.println("county.getCounty_code() = " + county.getCounty_code());

            String countyCode = county.getCounty_code().substring(0, 5);

            StringBuilder urlBuilder = new StringBuilder("http://openapi.molit.go.kr/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcAptTradeDev"); /*URL*/
            urlBuilder.append("?" + URLEncoder.encode("serviceKey", "UTF-8") + serviceKey); /*Service Key*/
//            urlBuilder.append("&" + URLEncoder.encode("pageNo", "UTF-8") + "=" + URLEncoder.encode("1", "UTF-8")); /*페이지번호*/
//            urlBuilder.append("&" + URLEncoder.encode("numOfRows", "UTF-8") + "=" + URLEncoder.encode("1000000", "UTF-8")); /*한 페이지 결과 수*/
//            urlBuilder.append("&" + URLEncoder.encode("LAWD_CD", "UTF-8") + "=" + URLEncoder.encode("11110", "UTF-8")); /*지역코드*/
            urlBuilder.append("&" + URLEncoder.encode("LAWD_CD", "UTF-8") + "=" + URLEncoder.encode(countyCode, "UTF-8")); /*지역코드*/
            urlBuilder.append("&" + URLEncoder.encode("DEAL_YMD", "UTF-8") + "=" + URLEncoder.encode("202311", "UTF-8")); /*계약월*/
//            urlBuilder.append("&" + URLEncoder.encode("DEAL_YMD", "UTF-8") + "=" + URLEncoder.encode(formattedDate, "UTF-8")); /*계약월*/
            URL url = new URL(urlBuilder.toString());
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.setRequestProperty("Content-type", "application/json");
            System.out.println("Response code: " + conn.getResponseCode());
            BufferedReader rd;

            if (conn.getResponseCode() >= 200 && conn.getResponseCode() <= 300) {
                rd = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            } else {
                rd = new BufferedReader(new InputStreamReader(conn.getErrorStream()));
            }
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = rd.readLine()) != null) {
                sb.append(line);
            }
            rd.close();
            conn.disconnect();

            String xmlData = sb.toString();
            System.out.println("xmlData = " + xmlData);

            // DocumentBuilderFactory 인스턴스 생성
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();

            // String 형태의 XML 데이터를 InputStream으로 변환
            ByteArrayInputStream input = new ByteArrayInputStream(
                    xmlData.getBytes(StandardCharsets.UTF_8));

            // XML 데이터 파싱
            Document doc = builder.parse(input);
            doc.getDocumentElement().normalize();

            // "item" 태그로 요소들을 가져옴
            NodeList nList = doc.getElementsByTagName("item");
//            List<AptApiDb> aptApiDbs = new ArrayList<>();

            for (int temp = 0; temp < nList.getLength(); temp++) {
                Node node = nList.item(temp);

                if (node.getNodeType() == Node.ELEMENT_NODE) {
                    Element element = (Element) node;


                    // 원하는 태그의 데이터 추출
//                    String 거래유형 = element.getElementsByTagName("거래유형").item(0).getTextContent();
//                    String 거래금액 = element.getElementsByTagName("거래금액").item(0).getTextContent();
//                    String 건축년도 = element.getElementsByTagName("건축년도").item(0).getTextContent();
//                    String 년 = element.getElementsByTagName("년").item(0).getTextContent();
//                    String 도로명 = element.getElementsByTagName("도로명").item(0).getTextContent();
//                    String 도로명건물본번호코드 = element.getElementsByTagName("도로명건물본번호코드").item(0).getTextContent();
//                    String 도로명건물부번호코드 = element.getElementsByTagName("도로명건물부번호코드").item(0).getTextContent();
//                    String 도로명시군구코드 = element.getElementsByTagName("도로명시군구코드").item(0).getTextContent();
//                    String 도로명일련번호코드 = element.getElementsByTagName("도로명일련번호코드").item(0).getTextContent();
//                    String 도로명지상지하코드 = element.getElementsByTagName("도로명지상지하코드").item(0).getTextContent();
//                    String 도로명코드 = element.getElementsByTagName("도로명코드").item(0).getTextContent();
//                    String 법정동 = element.getElementsByTagName("법정동").item(0).getTextContent();
//                    String 법정동본번코드 = element.getElementsByTagName("법정동본번코드").item(0).getTextContent();
//                    String 법정동부번코드 = element.getElementsByTagName("법정동부번코드").item(0).getTextContent();
//                    String 법정동시군구코드 = element.getElementsByTagName("법정동시군구코드").item(0).getTextContent();
//                    String 법정동읍면동코드 = element.getElementsByTagName("법정동읍면동코드").item(0).getTextContent();
//                    String 법정동지번코드 = element.getElementsByTagName("법정동지번코드").item(0).getTextContent();
//                    String 아파트 = element.getElementsByTagName("아파트").item(0).getTextContent();
//                    String 월 = element.getElementsByTagName("월").item(0).getTextContent();
//                    String 일 = element.getElementsByTagName("일").item(0).getTextContent();
//                    String 일련번호 = element.getElementsByTagName("일련번호").item(0).getTextContent();
//                    String 전용면적 = element.getElementsByTagName("전용면적").item(0).getTextContent();
//                    String 중개사소재지 = element.getElementsByTagName("중개사소재지").item(0).getTextContent();
//                    String 지번 = element.getElementsByTagName("지번").item(0).getTextContent();
//                    String 지역코드 = element.getElementsByTagName("지역코드").item(0).getTextContent();
//                    String 층 = element.getElementsByTagName("층").item(0).getTextContent();
//                    String 해제사유발생일 = element.getElementsByTagName("해제사유발생일").item(0).getTextContent();
//                    String 해제여부 = element.getElementsByTagName("해제여부").item(0).getTextContent();
//                    String 등기일자 = element.getElementsByTagName("등기일자").item(0).getTextContent();

                    String 거래유형 = element.getElementsByTagName("거래유형").item(0) != null ? element.getElementsByTagName("거래유형").item(0).getTextContent() : "";
                    String 거래금액 = element.getElementsByTagName("거래금액").item(0) != null ? element.getElementsByTagName("거래금액").item(0).getTextContent() : "";
                    String 건축년도 = element.getElementsByTagName("건축년도").item(0) != null ? element.getElementsByTagName("건축년도").item(0).getTextContent() : "";
                    String 년 = element.getElementsByTagName("년").item(0) != null ? element.getElementsByTagName("년").item(0).getTextContent() : "";
                    String 도로명 = element.getElementsByTagName("도로명").item(0) != null ? element.getElementsByTagName("도로명").item(0).getTextContent() : "";
                    String 법정동 = element.getElementsByTagName("법정동").item(0) != null ? element.getElementsByTagName("법정동").item(0).getTextContent() : "";
                    String 법정동본번코드 = element.getElementsByTagName("법정동본번코드").item(0) != null ? element.getElementsByTagName("법정동본번코드").item(0).getTextContent() : "";
                    String 법정동부번코드 = element.getElementsByTagName("법정동부번코드").item(0) != null ? element.getElementsByTagName("법정동부번코드").item(0).getTextContent() : "";
//                    String 법정동시군구코드 = element.getElementsByTagName("법정동시군구코드").item(0) != null ? element.getElementsByTagName("법정동시군구코드").item(0).getTextContent() : "";
                    String 아파트 = element.getElementsByTagName("아파트").item(0) != null ? element.getElementsByTagName("아파트").item(0).getTextContent() : "";
                    String 월 = element.getElementsByTagName("월").item(0) != null ? element.getElementsByTagName("월").item(0).getTextContent() : "";
                    String 일 = element.getElementsByTagName("일").item(0) != null ? element.getElementsByTagName("일").item(0).getTextContent() : "";
                    String 전용면적 = element.getElementsByTagName("전용면적").item(0) != null ? element.getElementsByTagName("전용면적").item(0).getTextContent() : "";
                    String 중개사소재지 = element.getElementsByTagName("중개사소재지").item(0) != null ? element.getElementsByTagName("중개사소재지").item(0).getTextContent() : "";
                    String 지번 = element.getElementsByTagName("지번").item(0) != null ? element.getElementsByTagName("지번").item(0).getTextContent() : "";
                    String 층 = element.getElementsByTagName("층").item(0) != null ? element.getElementsByTagName("층").item(0).getTextContent() : "";
                    String 해제사유발생일 = element.getElementsByTagName("해제사유발생일").item(0) != null ? element.getElementsByTagName("해제사유발생일").item(0).getTextContent() : "";
                    String 등기일자 = element.getElementsByTagName("등기일자").item(0) != null ? element.getElementsByTagName("등기일자").item(0).getTextContent() : "";
                    String 법정동읍면동코드 = element.getElementsByTagName("법정동읍면동코드").item(0) != null ? element.getElementsByTagName("법정동읍면동코드").item(0).getTextContent() : "";


                    String countyDistrictsNm = countyCode + 법정동읍면동코드;
                    System.out.println("countyDistrictsNm = " + countyDistrictsNm);
                    String countyDistrictsNmTest = aptApiDbSVC.selectCounty(countyDistrictsNm) != null
                            ? aptApiDbSVC.selectCounty(countyDistrictsNm).getCounty()
                            : null;

                    System.out.println("countyDistrictsNmTest = " + countyDistrictsNmTest);

                    String city;

                    if (법정동.replace(" ", "").equals(countyDistrictsNmTest)) {
                        System.out.println("1");
                        city = aptApiDbSVC.selectCity(countyDistrictsNm).getCity() + 법정동;
                        System.out.println("1 = " + city);
                    } else {
                        System.out.println("2");
                        city = aptApiDbSVC.selectCity(countyDistrictsNm).getCity() + " " + aptApiDbSVC.selectCounty(countyDistrictsNm).getCounty() + 법정동;
                        System.out.println("2 = " + city);
                    }

                    System.out.println("city = " + city);

                    if (월.length() == 1) {
                        월 = "0" + 월;
                    }

                    String contract_date = 년 + 월;
//                        System.out.println("contract_date = " + contract_date);

                    AptApiDb aptApiDb = new AptApiDb();
                    aptApiDb.setCity(city);
                    aptApiDb.setStreet(지번);
                    aptApiDb.setBon_bun(법정동본번코드);
                    aptApiDb.setBu_bun(법정동부번코드);
                    aptApiDb.setDan_gi_myeong(아파트);
                    aptApiDb.setSquare_meter(Float.parseFloat(전용면적));
                    aptApiDb.setContract_date(contract_date);
                    aptApiDb.setContract_day(일);
                    aptApiDb.setAmount(Integer.parseInt(거래금액.replace(",", "").trim()));
                    aptApiDb.setLayer(Integer.parseInt(층));
                    aptApiDb.setConstruction_date(건축년도);
                    aptApiDb.setRoad_name(도로명);
                    aptApiDb.setReason_cancellation_date(해제사유발생일);
                    aptApiDb.setTransaction_type(거래유형);
                    aptApiDb.setLocation_agency(중개사소재지);
                    aptApiDb.setRegistration_creation(등기일자);

                    // 기존 코드
                    String year = contract_date.substring(0, 4);
                    String month = contract_date.substring(4, 6);

                    // day를 int로 변환
                    int dayInt = Integer.parseInt(일);

                    // FULL_CONTRACT_DATE 설정
                    aptApiDb.setFullContractDate(year + "-" + month + "-" + String.format("%02d", dayInt));


                    // 추출된 데이터 출력 또는 처리
                    System.out.println("aptApiDb: " + aptApiDb);
                    // 추가적인 데이터 처리
                    //여기에 인설트 머지문 생성
                    try {
                        aptApiDbSVC.ApiDb(aptApiDb);

                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }


            System.out.println("======================완료===================");
        }
//        }
    }
}

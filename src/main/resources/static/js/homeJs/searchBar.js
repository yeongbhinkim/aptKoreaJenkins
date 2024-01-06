'use strict';
var now = new Date();
// 페이지 로드 시 이전 검색 조건 복원 및 시군구, 읍면동 데이터 재조회
document.addEventListener('DOMContentLoaded', async function () {
    if (localStorage.getItem('searchConditions')) {
        const conditions = JSON.parse(localStorage.getItem('searchConditions'));

        $contractDate.value = conditions.contractDate;
        $contractDateTo.value = conditions.contractDateTo;
        $searchSidoCd.value = conditions.searchSidoCd;
        $searchArea.value = conditions.searchArea;
        $searchFromAmount.value = conditions.searchFromAmount;
        $searchToAmnount.value = conditions.searchToAmount;

        // 시도 및 시군구 데이터 재조회
        try {
            if (conditions.searchSidoCd && conditions.searchSidoCd !== 'ALL') {
                await sidoCd(conditions.searchSidoCd);
                $searchGugunCd.value = conditions.searchGugunCd;

                if (conditions.searchGugunCd && conditions.searchGugunCd !== 'ALL') {
                    await gugunCd(conditions.searchGugunCd);
                    $searchDongCd.value = conditions.searchDongCd;
                }
            }
        } catch (error) {
            console.error('Error loading data:', error);
        }
    }
});

//계약일자 오늘날짜
document.getElementById('contractDate').value = new Date(now.setMonth(now.getMonth() - 1)).toISOString().substring(0, 10);
document.getElementById('contractDateTo').value = new Date().toISOString().substring(0, 10);

// 주소구분 라디오 이벤트
// const $gubunR1 = document.getElementById('gubunR1');
// $gubunR1.addEventListener('click', fn_select_check)
const $searchDongCd = document.getElementById("searchDongCd");
// const $searchChosung = document.getElementById("searchChosung");
const $searchLoad = document.getElementById("searchLoad");
// $searchDongCd.style.cssText = "display: block; ";
// $searchChosung.style.cssText = "display: none; ";
// $searchLoad.style.cssText = "display: none; ";

function fn_select_check(e) {
    if (e == 1) {
        $searchDongCd.style.cssText = "display: block; ";
        // $searchChosung.style.cssText = "display: none; ";
        // $searchLoad.style.cssText = "display: none; ";
    } else if (e == 2) {
        $searchDongCd.style.cssText = "display: none; ";
        // $searchChosung.style.cssText = "display: block; ";
        // $searchLoad.style.cssText = "display: block; ";
    }
}

// 시도 셀렉트 박스 변경 이벤트
const $searchSidoCd = document.getElementById('searchSidoCd');
$searchSidoCd.addEventListener('change', function(e) {
    sidoCd(e.target.value);
});

// 시군구 셀렉트 박스 변경 이벤트
const $searchGugunCd = document.getElementById("searchGugunCd");
$searchGugunCd.addEventListener('change', function(e) {
    gugunCd(e.target.value);
});



// 시도 데이터를 불러오는 함수
function sidoCd(sidoCd) {
    return new Promise((resolve, reject) => {
        const url = '/regionCounty/' + sidoCd;
        fetch(url, {
            method: 'GET',
        })
            .then(response => {
                if (!response.ok) {
                    throw new Error('Network response was not ok');
                }
                return response.json();
            })
            .then(data => {
                cbSidoCd(data); // 데이터 처리 함수 호출
                resolve();
            })
            .catch(error => {
                console.error('Fetch error:', error);
                reject(error);
            });
    });
}

// 시군구 데이터를 불러오는 함수
function gugunCd(gugunCd) {
    return new Promise((resolve, reject) => {
        const url = '/regionDistricts/' + gugunCd;
        fetch(url, {
            method: 'GET',
        })
            .then(response => {
                if (!response.ok) {
                    throw new Error('Network response was not ok');
                }
                return response.json();
            })
            .then(data => {
                cbGugunCd(data); // 데이터 처리 함수 호출
                resolve();
            })
            .catch(error => {
                console.error('Fetch error:', error);
                reject(error);
            });
    });
}

//시도 -> 시군구 데이터 셋
function cbSidoCd(res) {
    $searchGugunCd.add = res.data;
    $searchGugunCd.innerHTML = "";
    $searchDongCd.innerHTML = "";

    var opt2 = document.createElement("option");
    opt2.value = "ALL";
    opt2.innerHTML = "전체";
    $searchGugunCd.appendChild(opt2);

    var opt3 = document.createElement("option");
    opt3.value = "ALL";
    opt3.innerHTML = "전체";
    $searchDongCd.appendChild(opt3);

    for (var i = 0; i < res.data.length; i++) {
        var opt = document.createElement("option");
        opt.value = res.data[i].county_CODE;
        opt.innerHTML = res.data[i].county_NM;
        $searchGugunCd.appendChild(opt);
    }
}

//시군구 -> 읍면동 데이터 셋
function cbGugunCd(res) {
    $searchDongCd.add = res.data;
    $searchDongCd.innerHTML = "";

    var opt3 = document.createElement("option");
    opt3.value = "ALL";
    opt3.innerHTML = "전체";
    $searchDongCd.appendChild(opt3);

    for (var i = 0; i < res.data.length; i++) {
        var opt = document.createElement("option");
        opt.value = res.data[i].districts_CODE;
        opt.innerHTML = res.data[i].districts_NM;
        $searchDongCd.appendChild(opt);
    }
}

//검색
const $searchBtn = document.getElementById('searchBtn');                    //검색버튼
const $contractDate = document.getElementById('contractDate');              //시작 계약일자
const $contractDateTo = document.getElementById('contractDateTo');          //종료 계약일자
// const $searchSidoCd = document.getElementById('searchSidoCd');              //시도
// const $searchGugunCd = document.getElementById('searchGugunCd');            //시군구
// const $searchDongCd = document.getElementById('searchDongCd');              //읍면동
const $searchArea = document.getElementById('searchArea');                  //면적
const $searchFromAmount = document.getElementById('searchFromAmount');      //시작 금액
const $searchToAmnount = document.getElementById('searchToAmnount');        //종료 금액
var $searchAreaValue = "";  //시작 면적
var $searchAreaValueTo = "";  //종료 면적
//검색 버튼 클릭시
$searchBtn?.addEventListener('click', search_f);


function search_f(e) {
    //날짜 필수값 체크
    if ($contractDate.value == "" || $contractDateTo.value == "") {
        alert('계약일자를 입력하세요');
        if ($contractDate.value == "") {
            $contractDate.focus();
            $contractDate.select(); //커서이동
        }
        if ($contractDateTo.value == "") {
            $contractDateTo.focus();
            $contractDateTo.select(); //커서이동
        }
        return false;
    }

    //면적 값
    if ($searchArea.value == 1) {
        $searchAreaValue = 0;
        $searchAreaValueTo = 60;
    } else if ($searchArea.value == 2) {
        $searchAreaValue = 60;
        $searchAreaValueTo = 85;
    } else if ($searchArea.value == 3) {
        $searchAreaValue = 85;
        $searchAreaValueTo = 102;
    } else if ($searchArea.value == 4) {
        $searchAreaValue = 102;
        $searchAreaValueTo = 135;
    } else if ($searchArea.value == 5) {
        $searchAreaValue = 135;
        $searchAreaValueTo = 10000;
    } else if ($searchArea.value == 0) {
        $searchAreaValue = 0;
        $searchAreaValueTo = 10000;
    }

    const searchSidoCdText = $searchSidoCd.options[$searchSidoCd.selectedIndex].value == 'ALL' ? false : $searchSidoCd.options[$searchSidoCd.selectedIndex].value;
    const searchGugunCdText = $searchGugunCd.options[$searchGugunCd.selectedIndex].value == 'ALL' ? false : $searchGugunCd.options[$searchGugunCd.selectedIndex].value;
    const searchDongCdText = $searchDongCd.options[$searchDongCd.selectedIndex].value == 'ALL' ? false : $searchDongCd.options[$searchDongCd.selectedIndex].value;

    if (searchSidoCdText == false) {
        alert('시도를 선택하세요');
        $searchSidoCd.focus();
        return;
    }



    // 현재 검색 조건 로컬 스토리지에 저장
    const searchConditions = {
        contractDate: $contractDate.value,
        contractDateTo: $contractDateTo.value,
        searchSidoCd: $searchSidoCd.value,
        searchGugunCd: $searchGugunCd.value,
        searchDongCd: $searchDongCd.value,
        searchArea: $searchArea.value,
        searchFromAmount: $searchFromAmount.value,
        searchToAmount: $searchToAmnount.value
    };

    localStorage.setItem('searchConditions', JSON.stringify(searchConditions));


    const url = `/MyHomePrice/list/1/${searchSidoCdText}/${searchGugunCdText}/${searchDongCdText}/${$contractDate.value}/${$contractDateTo.value}/${$searchArea.value}/${$searchAreaValue}/${$searchAreaValueTo}/${$searchFromAmount.value}/${$searchToAmnount.value}`;


    //조건 검색
    // const url = `/MyHomePrice/list/1/${$contractDate.value}/${$contractDateTo.value}/${$searchSidoCd.options[$searchSidoCd.selectedIndex].text}/${$searchGugunCd.options[$searchGugunCd.selectedIndex].text}/${$searchDongCd.options[$searchDongCd.selectedIndex].text}/${$searchArea.value}/${$searchAreaValue}/${$searchAreaValueTo}/${$searchFromAmount.value}/${$searchToAmnount.value}`;
    location.href = url;
}



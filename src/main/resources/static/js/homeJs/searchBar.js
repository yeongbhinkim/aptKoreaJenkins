'use strict';

var now = new Date();

// 페이지 로드 시 이전 검색 조건 복원 및 시군구, 읍면동 데이터 재조회
document.addEventListener('DOMContentLoaded', async function () {
    const storedConditions = localStorage.getItem('searchConditions');
    if (storedConditions) {
        const conditions = JSON.parse(storedConditions);

        // 값이 유효한지 확인하고 DOM 요소에 할당
        if (conditions.contractDate) $contractDate.value = conditions.contractDate;
        if (conditions.contractDateTo) $contractDateTo.value = conditions.contractDateTo;
        if (conditions.searchSidoCd) $searchSidoCd.value = conditions.searchSidoCd;
        if (conditions.searchArea) $searchArea.value = conditions.searchArea;
        if (conditions.searchFromAmount) $searchFromAmount.value = conditions.searchFromAmount;
        if (conditions.searchToAmount) $searchToAmnount.value = conditions.searchToAmount;

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

// 계약일자 설정
$contractDate.value = new Date(now.setMonth(now.getMonth() - 1)).toISOString().substring(0, 10);
$contractDateTo.value = new Date().toISOString().substring(0, 10);

// 시도 셀렉트 박스 변경 이벤트
$searchSidoCd.addEventListener('change', function (e) {
    sidoCd(e.target.value);
});

// 시군구 셀렉트 박스 변경 이벤트
$searchGugunCd.addEventListener('change', function (e) {
    gugunCd(e.target.value);
});

// 시도 데이터를 불러오는 함수
function sidoCd(sidoCd) {
    return fetchData(`/regionCounty/${sidoCd}`, cbSidoCd);
}

// 시군구 데이터를 불러오는 함수
function gugunCd(gugunCd) {
    return fetchData(`/regionDistricts/${gugunCd}`, cbGugunCd);
}

// 공통 fetch 함수
function fetchData(url, callback) {
    return fetch(url, { method: 'GET' })
        .then(response => {
            if (!response.ok) {
                throw new Error('Network response was not ok');
            }
            return response.json();
        })
        .then(data => callback(data))
        .catch(error => {
            console.error('Fetch error:', error);
        });
}

// 시도 -> 시군구 데이터 처리
function cbSidoCd(res) {
    populateSelectBox($searchGugunCd, res.data, 'county_CODE', 'county_NM');
    populateSelectBox($searchDongCd, [], 'ALL', '전체');
}

// 시군구 -> 읍면동 데이터 처리
function cbGugunCd(res) {
    populateSelectBox($searchDongCd, res.data, 'districts_CODE', 'districts_NM');
}

// 선택 박스에 데이터 추가
function populateSelectBox(selectElement, data, valueKey, textKey) {
    selectElement.innerHTML = '';

    // 기본 '전체' 옵션 추가
    const defaultOption = document.createElement('option');
    defaultOption.value = 'ALL';
    defaultOption.innerHTML = '전체';
    selectElement.appendChild(defaultOption);

    // 동적으로 옵션 추가
    data.forEach(item => {
        const option = document.createElement('option');
        option.value = item[valueKey];
        option.innerHTML = item[textKey];
        selectElement.appendChild(option);
    });
}

// 검색 버튼 클릭 이벤트
$searchBtn?.addEventListener('click', search_f);

function search_f(e) {
    e.preventDefault();

    // 날짜 필수값 체크
    if (!$contractDate.value || !$contractDateTo.value) {
        alert('계약일자를 입력하세요');
        !$contractDate.value ? $contractDate.focus() : $contractDateTo.focus();
        return;
    }

    // 면적 값 설정
    const areaRanges = {
        "1": [0, 60],
        "2": [60, 85],
        "3": [85, 102],
        "4": [102, 135],
        "5": [135, 10000],
        "0": [0, 10000]
    };
    const [$searchAreaValue, $searchAreaValueTo] = areaRanges[$searchArea.value] || [0, 10000];

    const searchSidoCdText = getSelectedValue($searchSidoCd);
    const searchGugunCdText = getSelectedValue($searchGugunCd);
    const searchDongCdText = getSelectedValue($searchDongCd);

    if (!searchSidoCdText) {
        alert('시도를 선택하세요');
        $searchSidoCd.focus();
        return;
    }

    // 검색 조건을 로컬 스토리지에 저장
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
    location.href = url;
}

// 선택된 값 반환 함수
function getSelectedValue(selectElement) {
    return selectElement?.value === 'ALL' ? null : selectElement.value;
}

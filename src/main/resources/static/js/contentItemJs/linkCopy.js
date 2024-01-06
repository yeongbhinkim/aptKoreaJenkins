'use strict';

//현재 페이지 url 링크 복사
const $copyLink = document.querySelector('.link-btn');
$copyLink.addEventListener('click', evt => {
  AptUrl.style.display='block';
  AptUrl.select();
  document.execCommand('copy');
  AptUrl.style.display='none';
  alert('현재 링크가 복사 되었습니다.');
});
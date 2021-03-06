## 1. 가상의 데이터 인프라 구축해보지 (No Coding)
- raw_data : 어떤 데이터들이 복사될 수 있을지?
- analytics: 어떤 써머리 테이블들을 만들 수 있을지?

#### _스타벅스의 데이터 인프라를 구축해보자._
- raw_data
  - 고객의 결제 데이터
    - 결제 시간
    - 결제 방식(대면, 앱)
    - 결제 금액
    - 선택 메뉴 (List)
    - 고객이 원하는 메뉴중 재고가 없는것들 (List)

  - 서빙 정보
    - 서빙(주문) 시작 시간
    - 서빙 방식 (매장/드라이브쓰루)
    - 서빙 직원
    - 음료 제작 시간
    - 서빙 완료 시간
    
  - 메뉴 정보
    - 메뉴 이름
    - 메뉴 타입 (음료, 푸드, 베이커리, 굿즈)
    - 메뉴 재료 (List)
    - 금액
  
  - 매장내 환경적 요소
    - 혼잡도
    - 온도
    - 조명 밝기
    - 소음 수준
    - 메뉴 디스플레이 순서 및 위치
    
  - 매장외 환경적 요소
    - 날씨
    - 온도
    - 습도
    - 휴일 여부
    - 교통 혼잡도
    
  - 이벤트 정보
    - 이벤트 타입 (메뉴 할인, 굿즈 한정판매)
    - 이벤트 진행 날짜


- analytics
  - 시간대별 판매량은 얼마나 되는지
  - 어떤 메뉴가 얼마나 팔렸는지
  - 성별/나이대별 잘 팔린 메뉴는 무엇인지
  - 날씨별/시간대별 많이 팔린 메뉴는 무엇인지
  - 메뉴 타입별 같이 잘 판매된 상품은 무엇인지
  - 결제 방식별, 서빙 방식별 선호되는 음료는 무엇인지
  - 재료별 일일 사용량, 가장 많이 사용되는 재료는 무엇인지
  - 음료의 주문후 완성되는데 얼마나 걸리는지, 어떤 음료가 완성되는데 오래걸리는지
  - 고객이 결제 후 음료를 받는데 까지 얼마나 걸렸는지, 시간대별 음료 서빙 속도
  - 혼잡한 시간에는 음료의 서빙 속도가 얼마나 되는지
  - 일별 굿즈 한정판매 / 음료판매


## 2. SQL이나  Python으로 Monthly Active User 세보기
|raw_data.user_session_channel|raw_data.session_timestamp|
|---|---|
|CREATE TABLE raw_data.user_session_channel (<br />userid integer ,<br /> sessionid varchar(32),<br />channel varchar(32),<br />Primary key (userid, sessionid)<br />);|CREATE TABLE raw_data.session_timestamp (<br />sessionid varchar(32) primary key,<br />ts timestamp<br />);|
- 앞서 주어진 두개의 테이블 (session_timestamp, user_session_channel)을 바탕으로 월별마다 액티브한 사용자들의 수를 카운트한다
- 여기서 중요한 점은 세션의 수를 세는 것이 아니라 사용자의 수를 카운트한다는 점이다.
- 결과는 예를 들면 아래와 같은 식이 되어야 한다:
```
2019-05: 400
2019-06: 500
2019-07: 600
```

[SQL 작성 완료](monthly-report.sql)

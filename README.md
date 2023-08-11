# RecruitRadar

- 기간: 2023.08.04 ~ 2023.09.xx

## Team
| **김민석** | **서대원** | **유하준** | **정희원** |
|:---:|:---:|:---:|:---:|
| <img src="https://github.com/kmus1232.png" width='400' /> | <img src="https://github.com/DaewonSeo.png" width='400' /> | <img src="https://github.com/HaJunYoo.png" width='400' /> | <img src="https://github.com/heewoneha.png" width='400' /> |

## Rules

- 각자의 이름으로 된 브랜치를 만들어서 코드를 올려주세요

  - cd {platform_folder}
  - git switch -c feat/add-(여러분의이름을영어로)
  - git add 파일들
  - 예시 : git commit -m "feat: platform_scraper.py"
  - git push

- push 이후 PR을 날려주세요

  - https://github.com/RecruitRadar/RecruitScraper/pulls 으로 이동! <br>
  - PR의 Title이나 Description은 자유롭게 적어주신 후 PR을 생성해주시면, 확인 후 Merge 합니다 :D
  - 리뷰어도 선택해주세요

- 레포 구성

  ```
  .
  ├── README.md
  ├── airflow
  │   ├── Dockerfile
  │   ├── dags
  │   ├── docker-compose.yaml
  │   ├── plugins
  │   └── requirements.txt
  ├── scraper
  │   ├── jobplanet_scraper
  │   ├── rallit_scraper
  │   ├── venv   #.gitignore add
  │   └── wanted_crawler
  ├── scraper_server
  │   ├── __pycache__
  │   ├── api
  │   ├── main.py
  │   ├── plugin
  │   ├── requirements.txt
  │   ├── server.py
  │   ├── static
  │   ├── tests
  │   └── venv #.gitignore add
  │
  ├── sql
  │
  └── eda
    ├── EDA.ipynb
    ├── requirements.txt
    └── venv #.gitignore add

  ```

  - scraper
    - 각종 플랫폼으로부터 데이터를 스크래핑
  - airflow
    - airflow 관련 파일
  - scraper_server
    ```
    python server.py
    ```
  - sql
    - 데이터 웨어하우스 관련 파일
    - 데이터베이스 관련 파일
    - 데이터 마트 ERD
    - 데이터베이스 스키마
    - 데이터베이스 테이블
    - 데이터베이스 테이블 데이터

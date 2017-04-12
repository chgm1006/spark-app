## Build

DQM 프로젝트는 Apache Maven을 기반으로 구성되어 있으며 소스코드를 빌드하기 위해서 다음의 조건을 만족해야 합니다.

* Oracle JDK 1.8
* Apache Maven 3.3.9

### Apache Maven 설치하기

```bash
# tar xvfz apache-maven-3.3.9-bin.tar.gz
```

### M2_HOME 변수 설정하기

Maven을 사용하기 위해서는 M2_HOME 환경 변수에 Maven의 설치 경로를 지정해야 합니다.

```bash
# export M2_HOME=/opt/apps/apache-maven-3.3.9
```

### PATH 변수 설정하기

Maven을 사용하기 위해서는 PATH 환경 변수에 Maven의 bin 경로를 지정해야 합니다.

```bash
# export PATH=$M2_HOME/bin$PATH
```

### 소스코드 빌드하기

```bash
# git clone http://72.1.1.1/...
# mvn package
```

import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, MetaData
from sqlalchemy.orm import sessionmaker, relationship, declarative_base
import os

# 1. DB 연결 설정 (docker-compose.yml과 일치시킵니다)
DB_USER = 'user'
DB_PASSWORD = '1234'
DB_HOST = 'localhost' # 포트 5432를 로컬에 노출시켰으므로 'localhost'
DB_PORT = '5432'
DB_NAME = 'chatdb'

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

try:
    engine = create_engine(DATABASE_URL)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    Base = declarative_base()
except Exception as e:
    print(f"DB 연결 오류: {e}")
    print("docker-compose up -d 를 실행했는지, DB 접속 정보가 yml 파일과 일치하는지 확인하세요.")
    exit()

# 2. SQLAlchemy 테이블 모델 정의
class Route(Base):
    __tablename__ = 'routes'
    route_id = Column(Integer, primary_key=True, index=True)
    route_name = Column(String, unique=True, index=True)
    trips = relationship("Trip", back_populates="route")

class Trip(Base):
    __tablename__ = 'trips'
    trip_id = Column(Integer, primary_key=True, index=True)
    route_id = Column(Integer, ForeignKey('routes.route_id'))
    day_type = Column(String, index=True)
    notes = Column(String, nullable=True)
    
    route = relationship("Route", back_populates="trips")
    stop_times = relationship("StopTime", back_populates="trip", cascade="all, delete-orphan")

class StopTime(Base):
    __tablename__ = 'stop_times'
    stop_time_id = Column(Integer, primary_key=True, index=True)
    trip_id = Column(Integer, ForeignKey('trips.trip_id'))
    stop_name = Column(String) # 정류장 이름 (CSV의 컬럼명)
    time = Column(String)      # 시간 (Χ, 경유 등이 있으므로 문자열)
    stop_sequence = Column(Integer) # 노선 내 정류장 순서
    
    trip = relationship("Trip", back_populates="stop_times")

# 3. 노선(Route)을 가져오거나 생성하는 도우미 함수
def get_or_create_route(session, route_name):
    route = session.query(Route).filter_by(route_name=route_name).first()
    if not route:
        route = Route(route_name=route_name)
        session.add(route)
        session.flush() # 새 route의 ID를 받아오기 위해 flush
    return route

# 4. CSV 파일을 읽어 DB에 저장하는 메인 함수
def load_csv_to_db(session, file_name, route_name, day_type, stop_columns):
    """
    CSV 파일을 읽어 DB에 맞게 변환하여 저장합니다.
    - file_name: 읽을 CSV 파일 이름
    - route_name: DB에 저장될 노선 이름 (예: '천안터미널')
    - day_type: 요일 타입 (예: '평일', '일요일')
    - stop_columns: CSV에서 정류장 순서를 나타내는 컬럼 이름 리스트
    """
    print(f"--- [시작] '{file_name}' 파일 처리 중... ---")
    
    try:
        df = pd.read_csv(file_name, encoding='cp949')
    except FileNotFoundError:
        print(f"*** [경고] 파일을 찾을 수 없습니다: {file_name}. 건너뜁니다. ***")
        return
    except Exception as e:
        # '천안아산역(토,공휴일)..csv' 파일은 깨져있으므로 여기서 오류가 날 것입니다.
        print(f"*** [오류] '{file_name}' 파일 읽기 실패: {e}. 건너뜁니다. ***")
        return

    # 1. 노선(Route) 정보 가져오기
    route = get_or_create_route(session, route_name)

    # 2. CSV의 각 행(row)을 순회하며 Trip과 StopTimes로 변환
    for _, row in df.iterrows():
        try:
            # 3. Trip 생성
            new_trip = Trip(
                route_id=route.route_id,
                day_type=day_type,
                # '운행 특이사항' 컬럼이 없는 파일도 있으므로 안전하게 접근
                notes=str(row['운행 특이사항']) if '운행 특이사항' in row and pd.notna(row['운행 특이사항']) else None
            )
            session.add(new_trip)
            session.flush() # new_trip의 trip_id를 받아오기 위해 flush

            # 4. StopTimes 생성 (Wide to Long 변환)
            for i, stop_col_name in enumerate(stop_columns):
                
                # CSV에 해당 정류장(컬럼)이 있는지 확인
                if stop_col_name not in row:
                    time_str = 'Χ' # 없으면 'X' 처리
                else:
                    time_str = str(row[stop_col_name]) if pd.notna(row[stop_col_name]) else 'Χ'

                new_stop_time = StopTime(
                    trip_id=new_trip.trip_id,
                    stop_name=stop_col_name, # 컬럼 이름을 정류장 이름으로 사용
                    time=time_str,
                    stop_sequence=i + 1 # 리스트 순서가 곧 정류장 순서
                )
                session.add(new_stop_time)
        
        except Exception as e:
            print(f"*** [오류] {file_name}의 행 처리 중 오류: {e} (순번: {row.get('순', 'N/A')}) ***")
            # 이 행에서 오류가 나도 다음 행은 계속 처리
            session.rollback() # 일단 이 행(Trip)은 롤백
            continue # 다음 행(row)으로 넘어감
            
    print(f"--- [완료] '{file_name}' 파일 처리 완료. ---")


# 5. 스크립트 메인 실행 부분
if __name__ == "__main__":
    
    session = SessionLocal()
    
    try:
        # (주의!) 스크립트를 실행할 때마다 데이터가 중복으로 쌓이는 것을 방지하기 위해
        # 테이블을 비우고 새로 생성합니다. (개발용)
        # 만약 기존 데이터를 유지하려면 이 두 줄을 주석 처리하세요.
        print("--- 기존 테이블 삭제 중... ---")
        Base.metadata.drop_all(bind=engine)
        print("--- 새 테이블 생성 중... ---")
        Base.metadata.create_all(bind=engine)

        # 10개 파일에 대한 처리 정보를 리스트로 정의
        files_to_load = [
            {
                "file_name": "천안터미널(평일).csv",
                "route_name": "천안터미널",
                "day_type": "평일",
                "stops": ['학교(출발)', '천안터미널', '두정동 맥도날드', '홈마트 에브리데이', '서울대정병원', '학교(도착)']
            },
            {
                "file_name": "온양역,터미널(평일).csv",
                "route_name": "온양역,터미널",
                "day_type": "평일",
                "stops": ['학교(출발)', '주은아파트', '온양온천역', '아산터미널', '권곡초', '학교(도착)']
            },
            {
                "file_name": "천안아산역(일요일).csv",
                "route_name": "천안아산역",
                "day_type": "일요일",
                "stops": ['학교(출발)', '천안아산역', '학교(도착)']
            },
            {
                "file_name": "천안아산역(토,공휴일)..csv", # 깨진 파일
                "route_name": "천안아산역",
                "day_type": "토,공휴일",
                "stops": ['학교(출발)', '천안아산역', '학교(도착)']
            },
            {
                "file_name": "천안아산역(평일).csv",
                "route_name": "천안아산역",
                "day_type": "평일",
                "stops": ['학교(출발)', '천안아산역', '학교(도착)']
            },
            {
                "file_name": "천안역(일요일).csv",
                "route_name": "천안역",
                "day_type": "일요일",
                "stops": ['학교(출발)', '천안역', '학교(도착)']
            },
            {
                "file_name": "천안역(토,공휴일).csv",
                "route_name": "천안역",
                "day_type": "토,공휴일",
                "stops": ['학교(출발)', '천안역', '학교(도착)']
            },
            {
                "file_name": "천안역(평일).csv",
                "route_name": "천안역",
                "day_type": "평일",
                "stops": ['학교(출발)', '천안역 출발', '하이렉스파 건너편', '용암마을', '학교(도착)']
            },
            {
                "file_name": "천안터미널(일요일).csv",
                "route_name": "천안터미널",
                "day_type": "일요일",
                "stops": ['학교(출발)', '천안터미널', '학교(도착)']
            },
            {
                "file_name": "천안터미널(토,공휴일).csv",
                "route_name": "천안터미널",
                "day_type": "토,공휴일",
                "stops": ['학교(출발)', '천안터미널', '학교(도착)']
            }
        ]

        # 정의된 리스트를 순회하며 데이터 로딩 함수 실행
        for config in files_to_load:
            load_csv_to_db(
                session=session,
                file_name=config["file_name"],
                route_name=config["route_name"],
                day_type=config["day_type"],
                stop_columns=config["stops"]
            )
        
        # 모든 작업이 성공하면 DB에 최종 저장 (커밋)
        session.commit()
        print("\n[최종 성공] 모든 파일 처리가 완료되었고 DB에 커밋되었습니다.")

    except Exception as e:
        print(f"\n[최종 오류] 스크립트 실행 중 심각한 오류 발생. {e}")
        print("모든 변경 사항을 롤백합니다.")
        session.rollback() # 오류 발생 시 모든 변경 사항을 되돌림
    finally:
        session.close() # 작업 완료 후 세션 종료
        print("DB 연결을 종료합니다.")
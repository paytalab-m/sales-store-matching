import gspread
import pandas as pd
import re
import time
from oauth2client.service_account import ServiceAccountCredentials
from rapidfuzz import fuzz
from tqdm import tqdm
import multiprocessing as mp 
import os
import math
from gspread.exceptions import SpreadsheetNotFound, WorksheetNotFound
from datetime import datetime # (신규) 날짜 기록을 위해 import

# --- 1. 설정 (Configuration) ---

# Google API 및 시트 정보
SERVICE_ACCOUNT_FILE = 'msdproject-466902-dd36cca121a8.json'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive.file']

# (C) 공공데이터 (10만 건)
# (수정) C시트 ID 및 탭 이름 변경
SHEET_C_ID = '1t4fJQ0qPUZ-_oZyKLDEFL-UNzPMc8LcMoSHZ8xn2BK8'
SHEET_C_TAB = '공공데이터 원본'
SHEET_C_NAME_COL = '사업자명'
SHEET_C_ADDR_COL = '도로명전체주소'
# (신규) C시트 증분 처리를 위한 상태 컬럼
SHEET_C_STATUS_COL = '서울영업팀 양도양수 확인'

# (B) 머니핀 데이터
SHEET_B_ID = '1T0UfUzQuPgVBu3Onni3dZP7i1AmQVfnGAcaod3O-Z8c'
SHEET_B_TAB = '서울특별시 DB 리스트'
SHEET_B_NAME_COL = '사업자명'
SHEET_B_ADDR_COL = '도로명전체주소'
SHEET_B_BIZNO_COL = 'biz_no'
SHEET_B_OPEN_DATE_COL = '개업연월일'

# (R) 결과 시트
SHEET_R_ID = '1T0UfUzQuPgVBu3Onni3dZP7i1AmQVfnGAcaod3O-Z8c'
SHEET_R_TAB = '서울특별시 양도양수 매장 리스트'
# (신규) R시트 날짜 기록을 위한 컬럼명
SHEET_R_TS_COL = '처리일자'

# 최종 결과 시트에 저장될 컬럼명
# (수정) '처리일자' 컬럼 추가
RESULT_COLUMNS = [
    '매장명 일치', '주소 일치',
    '머니핀 데이터 매장명', '머니핀 데이터 주소',
    '공공데이터 매장명', '공공 데이터 주소',
    '사업자 번호', '개업연월일',
    SHEET_R_TS_COL # 처리일자
]

# --- 2. 로직 설정 (Logic) ---

# 매장명 음차 변환 사전
PHONETIC_MAP = {
    'mgc': '엠지씨',
    'ediya': '이디야',
    'starbucks': '스타벅스',
    'hollys': '할리스',
    'coffee': '커피',
    'baskinrobbins': '배스킨라빈스',
    'paik': '빽'
}

# 로직 가중치 및 임계값
LOGIC_WEIGHTS = {
    'NAME': 0.7,
    'ADDRESS': 0.3
}
LOGIC_THRESHOLDS = {
    'ADDRESS_GATEWAY': 85.0,  # 1차: 주소 점수 85점 미만 즉시 탈락
    'FINAL_GATEWAY': 75.0    # 2차: 종합 점수 75점 미만 최종 탈락
}

# 주소 정규화용 축약어 맵
ADDRESS_ABBR_MAP = {
    '서울특별시': '서울', '부산광역시': '부산', '대구광역시': '대구',
    '인천광역시': '인천', '광주광역시': '광주', '대전광역시': '대전',
    '울산광역시': '울산', '세종특별자치시': '세종', '경기도': '경기',
    '강원도': '강원', '충청북도': '충북', '충청남도': '충남',
    '전라북도': '전북', '전라남도': '전남', '경상북도': '경북',
    '경상남도': '경남', '제주특별자치도': '제주'
}
# 상세 주소 제거용 키워드 (정규식)
DETAIL_ADDR_REGEX = re.compile(
    r'(\(|\[).*(\)|\])'  # 1. 괄호() 또는 [] 안의 모든 내용 (e.g., (역삼동, 한신인터밸리))
    r'|(\s|,)(지하|지상|옥탑|[0-9]+층|[0-9]+호|[0-9]+동|본관|신관|별관|상가|빌딩|아파트|빌라|건물)\b' # 2. 상세 주소 키워드
)
# 한글, 영문, 숫자, 공백 외 모두 제거
NON_ALPHANUM_REGEX = re.compile(r'[^a-zA-Z0-9가-힣\s]')
# 연속 공백을 단일 공백으로
MULTI_SPACE_REGEX = re.compile(r'\s+')


# --- 3. 핵심 로직 함수 ---

def process_group_pair(task):
    """(병렬처리용) (C그룹 리스트, B그룹 리스트) '작업 쌍'을 받아 비교합니다."""
    c_list, b_list = task
    results_for_group = []
    
    # C그룹 리스트를 순회하며 B그룹 리스트 전체와 비교
    for c_row in c_list:
        match = find_best_match(c_row, b_list)
        if match:
            results_for_group.append(match)
    return results_for_group


def authenticate(service_account_file, scopes):
    """Google Sheets API 인증"""
    print(f"'{service_account_file}'을(를) 사용하여 인증 중...")
    if not os.path.exists(service_account_file):
        print(f"[오류] 서비스 계정 파일('{service_account_file}')을 찾을 수 없습니다.")
        print("사용자가 업로드한 json 파일의 이름을 스크립트와 동일하게 변경했는지 확인하세요.")
        return None
    try:
        creds = ServiceAccountCredentials.from_json_keyfile_name(service_account_file, scopes)
        client = gspread.authorize(creds)
        return client
    except Exception as e:
        print(f"[오류] 인증 실패: {e}")
        return None

# (수정) 증분 처리를 위해 fetch_data_as_dataframe 함수 로직 변경
def fetch_data_as_dataframe(client, sheet_id, tab_name, columns_to_fetch, status_col=None):
    """
    시트 데이터를 Pandas DataFrame로 가져오기.
    (신규) status_col이 지정되면, 해당 컬럼이 비어있는 행만 가져오고, 그 행들의 원본 인덱스를 반환.
    """
    print(f"'{tab_name}' 탭에서 데이터 가져오는 중... (API 호출 1회)")
    try:
        sheet = client.open_by_key(sheet_id).worksheet(tab_name)
        # (수정) get_all_records() 대신 get_all_values()를 사용하여 원본 행 추적
        all_values = sheet.get_all_values()
        
        if not all_values:
            print(f"[경고] '{tab_name}' 탭이 비어있습니다.")
            return pd.DataFrame(columns=columns_to_fetch), []

        header = all_values[0]
        data = all_values[1:]
        
        # 필요한 컬럼의 인덱스 찾기
        col_indices = {}
        for col_name in columns_to_fetch:
            if col_name not in header:
                print(f"[경고] '{tab_name}' 탭에 '{col_name}' 컬럼이 없습니다.")
                return pd.DataFrame(columns=columns_to_fetch), []
            col_indices[col_name] = header.index(col_name)

        status_col_idx = -1
        if status_col:
            if status_col not in header:
                print(f"[오류] '{tab_name}' 탭에 상태 컬럼 '{status_col}'이(가) 없습니다.")
                return pd.DataFrame(columns=columns_to_fetch), []
            status_col_idx = header.index(status_col)
            
        filtered_data = []
        rows_to_update = [] # (신규) 날짜를 기록할 원본 행 번호(1-based)

        # (신규) 증분 처리를 위한 필터링
        for i, row in enumerate(data):
            # 행이 헤더보다 짧은 경우를 대비
            if len(row) <= status_col_idx and status_col:
                continue # 상태 컬럼이 없는 행은 건너뛰기
                
            is_processed = False
            if status_col:
                is_processed = row[status_col_idx].strip() != ""
            
            # (신규) 상태 컬럼이 비어있는 행만 처리
            if not is_processed:
                row_data = {col_name: row[col_idx] for col_name, col_idx in col_indices.items()}
                filtered_data.append(row_data)
                rows_to_update.append(i + 2) # (i + 2) -> (헤더 1줄 + 0-based index)

        if status_col:
            print(f"총 {len(data)} 행 중, '{status_col}'이 비어있는 {len(filtered_data)} 행만 가져왔습니다.")
        else:
            print(f"총 {len(filtered_data)} 행의 데이터를 가져왔습니다.")

        df = pd.DataFrame(filtered_data, columns=columns_to_fetch)
        return df, rows_to_update

    except SpreadsheetNotFound:
        print(f"[오류] Google Sheet 파일을 찾을 수 없습니다 (ID: {sheet_id}).")
        print(f" - 1. 시트 ID가 정확한지 확인하세요: {sheet_id}")
        print(f" - 2. '{SERVICE_ACCOUNT_FILE}'의 서비스 계정이 해당 시트에 '편집자'로 초대되었는지 확인하세요.")
        print(f" - 3. Google Cloud Project에서 'Google Sheets API'가 '사용 설정'되었는지 확인하세요.")
        return pd.DataFrame(columns=columns_to_fetch), []
    except WorksheetNotFound:
        print(f"[오류] 시트 파일은 찾았으나 '{tab_name}' 탭을 찾을 수 없습니다.")
        print(" - 1. 탭 이름이 정확한지, 오타나 공백이 없는지 확인하세요.")
        return pd.DataFrame(columns=columns_to_fetch), []
    except Exception as e:
        print(f"[오류] '{tab_name}' 탭에서 알 수 없는 데이터 가져오기 실패: {e}")
        return pd.DataFrame(columns=columns_to_fetch), []

def get_blocking_key(addr):
    """(최적화용) 주소에서 '시/도 + 시/군/구' 형태의 블로킹 키를 추출합니다."""
    if not isinstance(addr, str) or not addr:
        return "기타"
    
    addr_str = addr
    # 1. 시/도 축약 (e.g., '서울특별시' -> '서울')
    for k, v in ADDRESS_ABBR_MAP.items():
        if addr_str.startswith(k):
            addr_str = addr_str.replace(k, v, 1) # 1번만 치환
            break
    
    # 2. 시/군/구 추출 (공백 기준 첫 두 단어)
    parts = addr_str.split(maxsplit=2)
    if len(parts) >= 2:
        # e.g., "서울 강남구" or "경기 안산시"
        key = f"{parts[0]} {parts[1]}"
        # '구'가 없는 '시' 처리 (e.g., 경기 부천시 -> 경기 부천)
        if not key.endswith("구") and not key.endswith("군") and key.endswith("시"):
             key = parts[0] + " " + parts[1].replace("시", "")
        return key.strip()
    elif len(parts) == 1:
        return parts[0] # e.g., "서울"
    else:
        return "기타" # 빈 주소 등

def smart_normalize_address(addr):
    """(매우 중요) '스마트 주소 정규화' 로직"""
    if not isinstance(addr, str) or not addr:
        return ""
    
    # 1. 괄호 및 상세 주소 키워드 제거
    norm_addr = DETAIL_ADDR_REGEX.sub('', addr)
    
    # 2. 시/도 축약
    for k, v in ADDRESS_ABBR_MAP.items():
        norm_addr = norm_addr.replace(k, v)
        
    # 3. 특수문자 제거 (한글, 영문, 숫자, 공백만 남김)
    norm_addr = NON_ALPHANUM_REGEX.sub('', norm_addr)
    
    # 4. 소문자화 및 연속 공백 제거
    norm_addr = MULTI_SPACE_REGEX.sub(' ', norm_addr).strip().lower()
    
    return norm_addr

def normalize_name(name):
    """매장명 정규화 (음차 변환, 공백/특수문자 제거)"""
    if not isinstance(name, str) or not name:
        return ""
        
    norm_name = name.lower()
    
    # 1. 음차 변환 (긴 키부터)
    sorted_keys = sorted(PHONETIC_MAP.keys(), key=len, reverse=True)
    for key in sorted_keys:
        norm_name = norm_name.replace(key, PHONETIC_MAP[key])
        
    # 2. 한글, 영문, 숫자만 남기고 모두 제거 (공백 포함)
    norm_name = re.sub(r'[^a-zA-Z0-9가-힣]', '', norm_name)
    
    return norm_name

def preprocess_dataframe(df, name_col, addr_col):
    """비교 전 데이터프레임을 전처리 (정규화, 블로킹 키 생성)"""
    df_processed = df.copy()
    
    # tqdm.pandas()를 사용하여 progress_apply로 진행률 표시
    tqdm.pandas(desc="  - 매장명 정규화 중")
    df_processed['norm_name'] = df_processed[name_col].progress_apply(normalize_name)
    
    tqdm.pandas(desc="  - 주소 정규화 중")
    df_processed['norm_addr'] = df_processed[addr_col].progress_apply(smart_normalize_address)
    
    # (신규) 블로킹 키 생성
    tqdm.pandas(desc="  - 블로킹 키 생성 중")
    df_processed['blocking_key'] = df_processed[addr_col].progress_apply(get_blocking_key)
    
    # Jaccard 유사도 비교를 위한 토큰화 (공백 기준)
    df_processed['addr_tokens'] = df_processed['norm_addr'].apply(lambda x: ' '.join(set(x.split())))
    return df_processed

def find_best_match(c_row, relevant_b_list):
    """C시트의 한 행(c_row)을 관련 B그룹 리스트(relevant_b_list)와 비교하여 최고 점수 매칭 찾기"""
    
    # C시트 정보
    c_name_orig = c_row[SHEET_C_NAME_COL]
    c_addr_orig = c_row[SHEET_C_ADDR_COL]
    c_name_norm = c_row['norm_name']
    c_addr_norm = c_row['norm_addr']
    c_addr_tokens = c_row['addr_tokens']
    
    best_match_info = None
    best_final_score = -1.0

    # B시트 관련 그룹만 순회
    for b_row in relevant_b_list:
        b_name_norm = b_row['norm_name']
        b_addr_norm = b_row['norm_addr']
        b_addr_tokens = b_row['addr_tokens']
        
        # --- 1단계: 주소 점수 계산 ---
        addr_score = 0.0
        if not c_addr_norm or not b_addr_norm:
            addr_score = 0.0
        elif c_addr_norm == b_addr_norm:
            addr_score = 100.0
        else:
            # 스마트 정규화가 다를 경우, 토큰 기반 Jaccard 유사도 (token_set_ratio)
            addr_score = fuzz.token_set_ratio(c_addr_tokens, b_addr_tokens)

        # --- 2단계: 주소 게이트웨이 필터 ---
        if addr_score < LOGIC_THRESHOLDS['ADDRESS_GATEWAY']:
            continue # 주소 점수 90점 미만 즉시 탈락

        # --- 3단계: 매장명 점수 계산 (편집 거리) ---
        name_score = fuzz.ratio(c_name_norm, b_name_norm)
        
        # --- 4단계: 종합 점수 계산 ---
        final_score = (name_score * LOGIC_WEIGHTS['NAME']) + (addr_score * LOGIC_WEIGHTS['ADDRESS'])
        
        # --- 5단계: 최종 게이트웨이 필터 ---
        if final_score >= LOGIC_THRESHOLDS['FINAL_GATEWAY']:
            # 현재 C행에 대해 80점이 넘는 B 매장들 중, 최고 종합 점수를 가진 B 매장을 찾음
            if final_score > best_final_score:
                best_final_score = final_score
                best_match_info = {
                    'name_score': name_score,
                    'addr_score': addr_score,
                    'b_name': b_row[SHEET_B_NAME_COL],
                    'b_addr': b_row[SHEET_B_ADDR_COL],
                    'b_bizno': b_row.get(SHEET_B_BIZNO_COL, ''), # .get으로 안전하게
                    'b_open_date': b_row.get(SHEET_B_OPEN_DATE_COL, '') # .get으로 안전하게
                }

    # C행에 대한 최고 점수 매칭 결과 반환
    if best_match_info:
        return {
            RESULT_COLUMNS[0]: round(best_match_info['name_score'], 2),
            RESULT_COLUMNS[1]: round(best_match_info['addr_score'], 2),
            RESULT_COLUMNS[2]: best_match_info['b_name'],
            RESULT_COLUMNS[3]: best_match_info['b_addr'],
            RESULT_COLUMNS[4]: c_name_orig,
            RESULT_COLUMNS[5]: c_addr_orig,
            RESULT_COLUMNS[6]: best_match_info['b_bizno'],
            RESULT_COLUMNS[7]: best_match_info['b_open_date'],
            # (수정) 처리일자 컬럼은 main 함수에서 일괄 추가
        }
    return None

# (수정) 결과 시트 '누적 저장(Append)' 로직으로 변경
def upload_results_to_sheet(client, sheet_id, tab_name, results_df):
    """결과 DataFrame을 Google Sheet에 누적하여 업로드 (Append)"""
    print(f"\n'{tab_name}' 탭에 결과 업로드 중... (API 호출 1~2회)")
    try:
        sheet = client.open_by_key(sheet_id).worksheet(tab_name)
        
        # (신규) 시트가 비어있는지 확인하기 위해 A1 셀만 읽기
        is_sheet_empty = sheet.acell('A1').value is None
        
        if is_sheet_empty:
            print(" - 시트가 비어있습니다. 헤더와 함께 신규 데이터를 작성합니다...")
            # 헤더 + 데이터 업로드 (첫 실행)
            sheet.update([results_df.columns.values.tolist()] + results_df.values.tolist(),
                         value_input_option='USER_ENTERED')
        else:
            print(" - 기존 데이터가 있습니다. 마지막 행에 이어서 데이터를 추가합니다...")
            # (수정) 데이터만 누적하여 추가 (2회차 이후)
            sheet.append_rows(results_df.values.tolist(), 
                              value_input_option='USER_ENTERED')
        
        print(f"'{tab_name}' 탭에 성공적으로 업로드 완료 ({len(results_df)} 행 추가).")
    except Exception as e:
        print(f"[오류] 결과 업로드 실패: {e}")

# (신규) C시트 상태 컬럼에 날짜를 일괄 기록하는 함수
def update_source_sheet_status(client, sheet_id, tab_name, rows_to_update, status_col_name, new_value):
    """(C)원본 시트의 처리된 행들에 대해 상태 컬럼(e.g., 'F')을 일괄 업데이트 (배치)"""
    if not rows_to_update:
        print("\n[알림] (C)원본 시트에 업데이트할 행이 없습니다.")
        return

    print(f"\n'{tab_name}' 탭의 '{status_col_name}' 컬럼에 {len(rows_to_update)} 개 행의 상태를 '{new_value}'(으)로 업데이트 중... (API 호출 1~2회)")
    try:
        sheet = client.open_by_key(sheet_id).worksheet(tab_name)
        
        # (신규) 상태 컬럼의 '열 번호 (1-based)' 찾기
        header = sheet.row_values(1)
        if status_col_name not in header:
            print(f"[오류] (C)시트 '{tab_name}'에 '{status_col_name}' 컬럼을 찾을 수 없어 날짜 기록을 건너뜁니다.")
            return
        status_col_idx = header.index(status_col_name) + 1 # 1-based index
        
        # (신규) gspread.Cell 객체를 리스트로 만들어 'batch_update' (한 번의 API 호출)
        cells_to_update = []
        for row_num in rows_to_update:
            cells_to_update.append(gspread.Cell(row=row_num, col=status_col_idx, value=new_value))
        
        print(f" - {len(cells_to_update)}개 셀 배치 업데이트 중...")
        sheet.update_cells(cells_to_update, value_input_option='USER_ENTERED')
        print(f" - (C)원본 시트 상태 업데이트 완료.")

    except Exception as e:
        print(f"[오류] (C)원본 시트 상태 업데이트 실패: {e}")


# --- 4. 메인 실행 ---

def main():
    start_time = time.time()
    # (신규) 오늘 날짜 문자열 생성 (e.g., '2025-11-07')
    today_str = datetime.now().strftime("%Y-%m-%d")
    
    client = authenticate(SERVICE_ACCOUNT_FILE, SCOPES)
    if not client:
        return

    # --- 데이터 로드 ---
    print("\n--- 1. 데이터 로드 ---")
    # (수정) C시트 로드 시 '상태 컬럼'을 전달하고, '업데이트할 행 번호' 리스트를 받음
    df_c, c_rows_to_update = fetch_data_as_dataframe(client, SHEET_C_ID, SHEET_C_TAB, 
                                                   [SHEET_C_NAME_COL, SHEET_C_ADDR_COL],
                                                   status_col=SHEET_C_STATUS_COL) # (신규) 상태 컬럼 전달
    
    # (수정) B시트 로드 (상태 컬럼 없으므로 None, 업데이트 행 리스트는 무시)
    df_b, _ = fetch_data_as_dataframe(client, SHEET_B_ID, SHEET_B_TAB, 
                                      [SHEET_B_NAME_COL, SHEET_B_ADDR_COL, SHEET_B_BIZNO_COL, SHEET_B_OPEN_DATE_COL],
                                      status_col=None)

    if df_c.empty or df_b.empty:
        if df_c.empty:
            print("[알림] (C)공공데이터에서 새로 처리할 행이 없습니다. 작업을 종료합니다.")
        else:
            print("[오류] (B)머니핀 데이터가 비어있어 비교를 중단합니다.")
        return

    # --- 데이터 전처리 ---
    print("\n--- 2. 데이터 전처리 ---")
    print("[B] 머니핀 데이터 전처리 중...")
    df_b_processed = preprocess_dataframe(df_b, SHEET_B_NAME_COL, SHEET_B_ADDR_COL)
    
    print("\n[C] 공공데이터 전처리 중...")
    df_c_processed = preprocess_dataframe(df_c, SHEET_C_NAME_COL, SHEET_C_ADDR_COL)
    
    # B시트 데이터를 '블로킹 키' 기반의 딕셔너리(해시맵)로 변환
    print("\n[B] 머니핀 데이터 블로킹 그룹 생성 중...")
    b_data_grouped = {
        key: group.to_dict('records')
        for key, group in df_b_processed.groupby('blocking_key')
    }
    b_group_count = len(b_data_grouped)
    print(f"[B] 머니핀 데이터 {b_group_count}개의 그룹으로 분류 완료.")

    # C시트 데이터도 '블로킹 키' 기반의 딕셔너리(해시맵)로 변환
    print("\n[C] 공공데이터 블로킹 그룹 생성 중...")
    c_data_grouped = {
        key: group.to_dict('records')
        for key, group in df_c_processed.groupby('blocking_key')
    }
    c_group_count = len(c_data_grouped)
    print(f"[C] 공공데이터 {c_group_count}개의 그룹으로 분류 완료.")
    
    # '작업 쌍' 리스트 생성
    task_list = []
    for key, c_list in c_data_grouped.items():
        if key in b_data_grouped:
            b_list = b_data_grouped[key]
            task_list.append((c_list, b_list)) # (C그룹 리스트, B그룹 리스트)

    print(f"\n--- 3. 매칭 시작 (총 {len(task_list)}개 지역 그룹 비교) ---")
    print(f"로직: 1) 주소 >= {LOGIC_THRESHOLDS['ADDRESS_GATEWAY']}%  2) 종합 >= {LOGIC_THRESHOLDS['FINAL_GATEWAY']}%")
    
    all_results = []
    
    num_cores = os.cpu_count()
    print(f"병렬 처리를 시작합니다. (사용 코어: {num_cores})")

    try:
        # Pool이 '작업 쌍' 리스트를 처리
        with mp.Pool(processes=num_cores) as pool:
            
            # tqdm으로 작업 진행률 표시
            with tqdm(total=len(task_list), desc="전체 매칭 진행률") as pbar:
                for results_chunk in pool.imap_unordered(process_group_pair, task_list):
                    all_results.extend(results_chunk)
                    pbar.update(1) # 그룹 하나 완료 시 진행률 1 증가
                    
    except Exception as e:
        print(f"\n[오류] 병렬 처리 풀(Pool) 실행 중 심각한 오류 발생: {e}")
        return

    print(f"\n--- 4. 매칭 완료 ---")
    print(f"총 {len(all_results)} 건의 신규 매칭을 찾았습니다.")

    if not all_results:
        print("신규 매칭된 결과가 없어 업로드를 건너뜁니다.")
    else:
        # --- 결과 업로드 ---
        print("\n--- 5. (R)결과 시트 업로드 ---")
        # 결과를 DataFrame으로 변환
        df_results = pd.DataFrame(all_results)
        
        # (신규) 처리일자 컬럼에 오늘 날짜 일괄 추가 (Feature 5)
        df_results[SHEET_R_TS_COL] = today_str
        
        # 컬럼 순서 보장 (신규 컬럼 포함)
        df_results = df_results[RESULT_COLUMNS]
        
        # (수정) 누적 저장 함수 호출 (Feature 4)
        upload_results_to_sheet(client, SHEET_R_ID, SHEET_R_TAB, df_results)

    # (신규) --- (C)원본 시트 상태 업데이트 ---
    # (주석) 매칭 결과(all_results)가 있든 없든,
    # (C)시트에서 읽어온 행들(c_rows_to_update)은 모두 "처리 완료"로 간주하고 날짜를 기록합니다.
    print("\n--- 6. (C)원본 시트 상태 업데이트 ---")
    update_source_sheet_status(client, SHEET_C_ID, SHEET_C_TAB, 
                               c_rows_to_update, # 1단계에서 가져온 '처리 대상 행' 리스트
                               SHEET_C_STATUS_COL, 
                               today_str) # 오늘 날짜

    end_time = time.time()
    print(f"\n--- 작업 완료 ---")
    print(f"총 실행 시간: {end_time - start_time:.2f} 초")


if __name__ == "__main__":
    # Windows에서 multiprocessing을 위한 freeze_support()
    mp.freeze_support() 
    main()

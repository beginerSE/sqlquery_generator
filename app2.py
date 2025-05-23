# import streamlit as st
# import pandas as pd

# st.title("BigQuery 検品用クエリ生成ツール")

# # 疑似エクセル入力エリア（初期値2行）
# st.write("📋 カラム名とデータ型を手入力またはコピペしてください（必要に応じて行を追加）")
# default_data = pd.DataFrame({
#     "col_name": ["", ""],
#     "data_type": ["STRING", "TIMESTAMP"]
# })
# edited_df = st.data_editor(default_data, num_rows="dynamic", use_container_width=True)



import streamlit as st
import pandas as pd
import io

st.title("BigQuery 検品用クエリ生成ツール")

st.write("📋 Excelで作成したカラム名とデータ型（2列）をコピーして下に貼り付け、「表に反映」ボタンを押してください")

# セッション状態に表データを保持
if "column_data" not in st.session_state:
    st.session_state["column_data"] = pd.DataFrame({
        "col_name": ["", ""],
        "data_type": ["STRING", "TIMESTAMP"]
    })

# 貼り付け用エリア
tsv_input = st.text_area("TSV入力欄（例: user_id\\tSTRING）", height=150, key="tsv_input")

# TSVパース関数
def parse_tsv(tsv_text):
    lines = tsv_text.strip().splitlines()
    data = []
    for line in lines:
        if '\t' in line:
            parts = line.split('\t')
            if len(parts) == 2:
                col_name, data_type = parts
                data.append({"col_name": col_name.strip(), "data_type": data_type.strip().upper()})
    return pd.DataFrame(data)

# 表に反映ボタン
if st.button("🧾 表に反映"):
    new_df = parse_tsv(tsv_input)
    if not new_df.empty:
        st.session_state["column_data"] = new_df
    else:
        st.warning("貼り付け内容が無効です。カラム名とデータ型をタブ区切りで入力してください。")

# 編集可能な表を表示
edited_df = st.data_editor(st.session_state["column_data"], num_rows="dynamic", use_container_width=True)

# 以降のクエリ生成処理などに使うため保持
st.session_state["column_data"] = edited_df




# テーブル名の入力
table_ref_input = st.text_input("🔗 BigQueryのテーブル名を入力してください（例: project.dataset.table）", "")

# 期間指定
# use_date_filter = st.checkbox("📅 期間指定を使用する")
# if use_date_filter:
#     term_column = st.text_input("フィルタ対象の日付カラム名", value="impression_date")
#     start_date = st.date_input("開始日", value=pd.to_datetime("2025-03-01"))
#     end_date = st.date_input("終了日", value=pd.to_datetime("2025-03-31"))
# else:
#     term_column = None


if use_date_filter:
    col_names = st.session_state["column_data"]["col_name"].dropna().unique().tolist()
    if col_names:
        term_column = st.selectbox("フィルタ対象の日付カラム名", options=col_names, index=0)
    else:
        st.warning("まずはカラム名を表に入力してください。")
        term_column = ""
    start_date = st.date_input("開始日", value=pd.to_datetime("2025-03-01"))
    end_date = st.date_input("終了日", value=pd.to_datetime("2025-03-31"))
else:
    term_column = None





# 実行ボタン
if st.button("🛠️ クエリ生成") and table_ref_input:
    selects = []

    # フィルター句生成
    if use_date_filter:
        date_filter = f"WHERE DATE({term_column}) BETWEEN '{start_date}' AND '{end_date}'"
    else:
        date_filter = ""

    for _, row in edited_df.iterrows():
        col, dtype = row["col_name"], row["data_type"]
        if not col or not dtype:
            continue  # 空行は無視

        if dtype == "STRING":
            select = f"""
SELECT
'{col}' AS column_name,
NULL AS column_ronti,
'{dtype}_ARRAY' AS data_type,
NULL AS data_ronti,
NULL AS data_length,
NULL AS null_able,
NULL AS selection_columns,
NULL AS consideration_columns,
COUNT(*) AS total_count,
COUNTIF({col} IS NULL) AS null_count,
COUNTIF(TRIM({col}) = '') AS empty_string_count,
COUNT(DISTINCT {col}) AS unique_count,
SAFE_DIVIDE(COUNTIF({col} IS NULL OR TRIM({col}) = ''), COUNT(*)) * 100 AS missing_rate_percent,
SAFE_CAST(MIN({col}) AS STRING) AS min_value,
SAFE_CAST(MAX({col}) AS STRING) AS max_value
FROM `{table_ref_input}` {date_filter}
"""
        elif dtype in ["BOOLEAN", "INTEGER", "FLOAT", "TIMESTAMP", "DATE"]:
            select = f"""
SELECT
'{col}' AS column_name,
NULL AS column_ronti,
'{dtype}_ARRAY' AS data_type,
NULL AS data_ronti,
NULL AS data_length,
NULL AS null_able,
NULL AS selection_columns,
NULL AS consideration_columns,
COUNT(*) AS total_count,
COUNTIF({col} IS NULL) AS null_count,
NULL AS empty_string_count,
COUNT(DISTINCT {col}) AS unique_count,
SAFE_DIVIDE(COUNTIF({col} IS NULL), COUNT(*)) * 100 AS missing_rate_percent,
SAFE_CAST(MIN({col}) AS STRING) AS min_value,
SAFE_CAST(MAX({col}) AS STRING) AS max_value
FROM `{table_ref_input}` {date_filter}
"""
        else:
            continue

        selects.append(select.strip())

    full_query = "\nUNION ALL\n".join(selects)
    st.success("🎉 クエリが生成されました。下記からコピーしてください。")
    st.code(full_query, language="sql")
    st.download_button("📄 クエリをテキストファイルで保存", full_query, file_name="bq_inspection_query.sql")

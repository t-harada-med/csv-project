import flet as ft
import pandas as pd

async def main(page: ft.Page):
    page.title = "Data Checker"
    
    # 状態管理
    state = {
        "df_csv": None, 
        "df_csv_unregistered": None, 
        "df_excel": None, 
        "excel_path": None,
        "target_emails": set(),
        "email_to_assigned_id": {},  # 画面上で確定したID（既存or新規）を保存用
        "new_id_emails": set(),
    }

    log_display = ft.Text("ファイルを読み込んでください", color="blue")
    data_table = ft.DataTable(
        columns=[ft.DataColumn(ft.Text("照合結果"))],
        rows=[]
    )

    save_file_picker = ft.FilePicker()
    
    # --- 1. ハンドラー ---

    async def handle_pick_csv(e):
        files = await ft.FilePicker().pick_files(
            allowed_extensions=["csv"],
            dialog_title="1. ユーザーアカウント申請エクスポートCSVを選択"
        )
        if files:
            state["df_csv"] = pd.read_csv(files[0].path)
            log_display.value = f"CSV1 読み込み完了: {files[0].name}"
            page.update()

    async def handle_pick_csv_unregistered(e):
        files = await ft.FilePicker().pick_files(
            allowed_extensions=["csv"],
            dialog_title="2. 未登録ユーザーCSVを選択"
        )
        if files:
            state["df_csv_unregistered"] = pd.read_csv(files[0].path)
            log_display.value = f"CSV2 読み込み完了: {files[0].name}"
            page.update()

    async def handle_pick_excel(e):
        files = await ft.FilePicker().pick_files(
            allowed_extensions=["xlsx", "xls"],
            dialog_title="3. アカウント登録申請書Excelを選択"
        )
        if files:
            state["excel_path"] = files[0].path
            state["df_excel"] = pd.read_excel(files[0].path, sheet_name=0, engine='openpyxl', skiprows=7)
            log_display.value = f"Excel 読み込み完了（照合用）: {files[0].name}"
            page.update()

    # --- 2. ダウンロードボタンのハンドラー (ID入力ロジック追加) ---
    async def handle_download_csv(e):
        if state["excel_path"] is None:
            log_display.value = "エラー: 先にExcelファイルを読み込んでください"
            page.update()
            return

        if not state["new_id_emails"]:
            log_display.value = "エラー: 照合を実行して、対象（新規ID）があるか確認してください"
            page.update()
            return

        path = await save_file_picker.save_file(
            file_name="filtered_upload_data.csv",
            allowed_extensions=["csv"]
        )

        if path:
            try:
                # Uploadシートを読み込み
                df_upload = pd.read_excel(state["excel_path"], sheet_name="Upload", engine='openpyxl')
                
                # 対象のメールアドレスのみ抽出
                df_filtered = df_upload[df_upload["email"].astype(str).isin(state["new_id_emails"])].copy()
                
                # --- IDの書き込み処理 ---
                # 1列目 (iloc[:, 0]) に、照合時に決定したIDをマップする
                # 画面上の表示に基づいたID (email_to_assigned_id) を適用
                df_filtered.iloc[:, 0] = df_filtered["email"].astype(str).map(state["email_to_assigned_id"])
                # -----------------------

                df_filtered.to_csv(path, index=False, encoding='utf-8-sig')
                log_display.value = f"ID入力済みCSV出力完了（{len(df_filtered)}件）: {path}"
            except Exception as ex:
                log_display.value = f"出力エラー: {str(ex)}"
            page.update()

    # --- 3. 照合ロジック ---

    async def handle_run_compare(e):
        if state["df_csv"] is None or state["df_excel"] is None:
            log_display.value = "エラー: CSV1とExcelを必ず選択してください"
            page.update()
            return

        log_display.value = "照合を実行中..."
        page.update()

        try:
            state["target_emails"] = set() 
            state["email_to_assigned_id"] = {} # ID管理をリセット
            df_csv = state["df_csv"]
            df_excel_full = state["df_excel"]
            df_unreg = state["df_csv_unregistered"]

            # ID最大値の取得
            try:
                max_id = pd.to_numeric(df_csv.iloc[:, 0], errors='coerce').max()
                if pd.isna(max_id): max_id = 0
            except:
                max_id = 0
            
            next_new_id = int(max_id + 1)

            # 既存メールとIDのマップ
            email_to_id_map = {}
            for target_col in [8, 16, 25, 42]:
                if len(df_csv.columns) > target_col:
                    temp_df = df_csv.iloc[:, [0, target_col]].dropna()
                    for _, row_csv in temp_df.iterrows():
                        email_to_id_map[str(row_csv.iloc[1])] = str(row_csv.iloc[0])

            # データ整形
            df_excel = df_excel_full.iloc[:, 1:10].copy()
            new_headers = ["区分", "施設名", "姓", "名", "空白1", "空白2", "メールアドレス", "利用権限", "備考"]
            if len(df_excel.columns) == len(new_headers):
                df_excel.columns = new_headers

            csv_sets = [
                set(df_csv.iloc[:, 8].astype(str).tolist()) if len(df_csv.columns) > 8 else set(),
                set(df_csv.iloc[:, 16].astype(str).tolist()) if len(df_csv.columns) > 16 else set(),
                set(df_csv.iloc[:, 25].astype(str).tolist()) if len(df_csv.columns) > 25 else set(),
                set(df_csv.iloc[:, 42].astype(str).tolist()) if len(df_csv.columns) > 42 else set()
            ]

            unreg_data_map = {}
            if df_unreg is not None and len(df_unreg.columns) >= 8:
                temp_unreg = df_unreg.iloc[:, [5, 7]].dropna()
                for _, r in temp_unreg.iterrows():
                    unreg_data_map[str(r.iloc[0])] = str(r.iloc[1])

            status_headers = ["ID", "プレ個別", "個別", "インポート", "一括", "未登録PID"]
            cols = [ft.DataColumn(ft.Text(h)) for h in status_headers]
            cols.extend([ft.DataColumn(ft.Text(str(col))) for col in df_excel.columns])
            data_table.columns = cols

            new_rows = []
            for _, row in df_excel.iterrows():
                email_to_check = str(row["メールアドレス"])
                csv_id_value = email_to_id_map.get(email_to_check, "")
                
                # --- ID確定ロジック ---
                if csv_id_value:
                    display_id = csv_id_value
                    id_color = "blue"
                else:
                    display_id = str(next_new_id)
                    next_new_id += 1
                    id_color = "purple"
                    state["new_id_emails"].add(email_to_check)
                
                # 確定したIDを保存しておく（出力用）
                state["email_to_assigned_id"][email_to_check] = display_id
                # --------------------

                status_cells = [ft.DataCell(ft.Text(display_id, weight="bold", color=id_color))]
                results = []

                for s in csv_sets:
                    exists = email_to_check in s
                    results.append(exists)
                    status_cells.append(ft.DataCell(ft.Text("あり" if exists else "なし", color="green" if exists else "red")))

                unreg_value = unreg_data_map.get(email_to_check, "")
                is_in_unreg = email_to_check in unreg_data_map
                status_cells.append(ft.DataCell(ft.Text(unreg_value if is_in_unreg else "-", color="orange" if is_in_unreg else "grey")))

                row_color = None
                if is_in_unreg:
                    row_color = "orange,0.3" 
                    state["target_emails"].add(email_to_check)
                elif (not results[1]) and (not results[3]): 
                    row_color = "red,0.1"
                    state["target_emails"].add(email_to_check)

                new_rows.append(ft.DataRow(cells=status_cells + [ft.DataCell(ft.Text(str(val), selectable=True)) for val in row], color=row_color))

            data_table.rows = new_rows
            log_display.value = f"照合完了！ 対象行: {len(state['target_emails'])}件"

        except Exception as ex:
            log_display.value = f"実行エラー: {str(ex)}"
        
        page.update()

    # --- 4. 画面レイアウト ---
    page.add(
        ft.Text("照合ツール", size=20, weight="bold"),
        ft.Row(controls=[
            ft.Button("1. エクスポートCSV", on_click=handle_pick_csv),
            ft.Button("2. 未登録CSV", on_click=handle_pick_csv_unregistered),
            ft.Button("3. 申請書Excel", on_click=handle_pick_excel),
            ft.FilledButton("4. 照合を実行", on_click=handle_run_compare),
            ft.OutlinedButton("対象のみCSV出力", on_click=handle_download_csv),
        ]),
        log_display,
        ft.Divider(),
        ft.Column([ft.Row([data_table], scroll="always")], scroll="always", expand=True),
    )

if __name__ == "__main__":
    ft.run(main)
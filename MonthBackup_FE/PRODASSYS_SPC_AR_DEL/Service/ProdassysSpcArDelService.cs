using System;
using System.Data;
using System.Collections.Generic;
using CPISData.Data;
using MonthBackup_FE.Helper;

namespace MonthBackup_FE.PRODASSYS_SPC_AR_DEL.Service
{
    public static class ProdassysSpcArDelService
    {
        #region NonShipData

        /// <summary>
        /// 對應 FUNCTION non_ship_data()
        /// 建立未出貨 LOT 清單到 unship_apg_tmp1
        /// </summary>
        //        public static void NonShipData(
        //            IFXTransaction tx,
        //            int fromDate,
        //            int toDate,
        //            ref short errCnt,
        //            Action<string> logCallback)
        //        {
        //            string sql1 = @"
        //SELECT imgnz01, imgnz02, imgnz03, imgnz04
        //  FROM ds2@d270tips:img_nonzero
        // WHERE imgnz02[1,3] = 'MS1'
        //   AND imgnz03[1]   = 'G'";

        //            int nonShipCnt = 0;
        //            int nullDataCount = 0;
        //            try
        //            {
        //                using (var reader1 = IfxDataAccess.ExecuteReader(tx, sql1))
        //                {
        //                    while (reader1.Read())
        //                    {
        //                        string v_imgnz01 = reader1.GetString(0).Trim();
        //                        string v_imgnz02 = reader1.GetString(1).Trim();
        //                        string v_imgnz03 = reader1.GetString(2).Trim();
        //                        string v_imgnz04 = reader1.GetString(3).Trim();

        //                        string v_imgnz03_1_4 = string.Empty;
        //                        if (!string.IsNullOrEmpty(v_imgnz03) && v_imgnz03.Length >= 4)
        //                        {
        //                            v_imgnz03_1_4 = v_imgnz03.Substring(0, 4);
        //                        }

        //                        string sql2 = $@"
        //SELECT a.oper3, b.lot_number, b.lot_id
        //  FROM ds2@d270tips:csi_file a,
        //       ds2@d270tips:csj_file b
        // WHERE a.receipt_no = b.receipt_no
        //   AND a.receipt_no[1,3] = '205'
        //   AND a.post = 'Y'
        //   AND b.part_id  = '{EscapeQuote(v_imgnz01)}'
        //   AND b.store    = '{EscapeQuote(v_imgnz02)}'
        //   AND b.location[1,4] = '{EscapeQuote(v_imgnz03_1_4)}'
        //   AND b.lot_number    = '{EscapeQuote(v_imgnz04)}'
        //   AND a.oper3 = 'A2'";
        //                        using (var reader2 = IfxDataAccess.ExecuteReader(tx, sql2))
        //                        {
        //                            bool hasData = false;
        //                            while (reader2.Read())
        //                            {
        //                                hasData = true;
        //                                string v_oper3 = reader2.GetString(0).Trim();
        //                                string v_lot_number = reader2.GetString(1).Trim();
        //                                string v_lot_id = reader2.GetString(2).Trim();

        //                                string v_oper = "APG";
        //                                string v_assy_lot = v_lot_id.Length >= 9
        //                                    ? v_lot_id.Substring(0, 9)
        //                                    : v_lot_id;

        //                                nonShipCnt++;
        //                                logCallback(nonShipCnt + "    OPER : " + v_oper +
        //                                    "    v_assy_lot : " + v_assy_lot +
        //                                    " from_date " + fromDate + " to_date " + toDate);

        //                                string insertSql = $@"
        //INSERT INTO unship_apg_tmp1(line, assy_lot, from_date, to_date)
        //VALUES('{EscapeQuote(v_oper)}',
        //       '{EscapeQuote(v_assy_lot)}',
        //       {fromDate},
        //       {toDate})";

        //                                IfxDataAccess.ExecuteNonQuery(tx, insertSql);
        //                                logCallback(insertSql);
        //                            }
        //                            if (!hasData)
        //                            {
        //                                nullDataCount++;
        //                                logCallback($"No Data Count:{nullDataCount}");
        //                            }
        //                        }
        //                    }
        //                }

        //                logCallback("non_ship_data 完成，未出貨筆數: " + nonShipCnt);
        //            }
        //            catch (Exception ex)
        //            {
        //                errCnt++;
        //                logCallback("non_ship_data 執行錯誤: " + ex.Message);
        //                LogHelper.WriteLog("PRODASSYS_SPC_AR_DEL",
        //                    "non_ship_data 執行錯誤: " + ex.Message);
        //            }
        //        }
        public static void NonShipData(
            IFXTransaction tx,
            int fromDate,
            int toDate,
            ref short errCnt,
            Action<string> logCallback)
        {
            // 合併後的 SQL
            string combinedSql = $@"
SELECT 
    'APG' as v_oper,
    CASE 
        WHEN LENGTH(b.lot_id) >= 9 THEN b.lot_id[1,9] 
        ELSE b.lot_id 
    END as v_assy_lot
FROM ds2@d270tips:img_nonzero i
JOIN ds2@d270tips:csj_file b ON 
    i.imgnz01 = b.part_id AND 
    i.imgnz02 = b.store AND 
    i.imgnz04 = b.lot_number AND 
    i.imgnz03[1,4] = b.location[1,4] -- 對應原本 v_imgnz03_1_4 的邏輯
JOIN ds2@d270tips:csi_file a ON 
    a.receipt_no = b.receipt_no
WHERE 
    i.imgnz02[1,3] = 'MS1' AND 
    i.imgnz03[1] = 'G' AND 
    a.receipt_no[1,3] = '205' AND 
    a.post = 'Y' AND 
    a.oper3 = 'A2'";

            int nonShipCnt = 0;
            try
            {
                // 1. 使用合併查詢直接抓取結果
                using (var reader = IfxDataAccess.ExecuteReader(tx, combinedSql))
                {
                    while (reader.Read())
                    {
                        nonShipCnt++;
                        string v_oper = reader.GetString(0).Trim();
                        string v_assy_lot = reader.GetString(1).Trim();

                        logCallback($"{nonShipCnt}    OPER : {v_oper}    v_assy_lot : {v_assy_lot} from_date {fromDate} to_date {toDate}");

                        // 2. 執行 Insert
                        string insertSql = $@"
INSERT INTO unship_apg_tmp1(line, assy_lot, from_date, to_date)
VALUES('{EscapeQuote(v_oper)}', '{EscapeQuote(v_assy_lot)}', {fromDate}, {toDate})";

                        IfxDataAccess.ExecuteNonQuery(tx, insertSql);
                    }
                }

                if (nonShipCnt == 0)
                {
                    logCallback("No Data Found.");
                }

                logCallback("non_ship_data 完成，未出貨筆數: " + nonShipCnt);
            }
            catch (Exception ex)
            {
                errCnt++;
                logCallback("non_ship_data 執行錯誤: " + ex.Message);
                // ... LogHelper 略
            }
        }

        #endregion

        #region DelData

        /// <summary>
        /// 對應 FUNCTION del_data()
        /// GlobalSettings.IsDeleteMode:
        /// - true  → 真的刪除（批次刪除）
        /// - false → 不刪除，只把「原本要刪的資料」匯出 .995（批次匯出）
        /// 
        /// batchSize: 每批最多處理筆數（建議 200000）
        /// </summary>
        public static void DelData(
            IFXTransaction tx,
            int fromDate,
            int toDate,
            ref short errCnt,
            Action<string> logCallback,
            int batchSize)
        {
            bool delMode = GlobalSettings.IsDeleteMode;

            logCallback(" 2F ----------------------------->");
            logCallback(delMode ? "【刪除模式】" : "【匯出模式】");
            logCallback($"批次筆數：{batchSize}");

            int cnt;

            // 1. spc_ds  (APG, LOT-based)
            logCallback(" 1. APG : spc_ds ");
            ProcessApgLotByBatch(
                tx, delMode, fromDate, toDate,
                tableName: "spc_ds",
                keyColumn: "sds_alot_no",
                dateColumn: "sds_keyin_date",
                fileName: "spc_ds.995",
                logCallback: logCallback,
                errCnt: ref errCnt,
                batchSize: batchSize);

            // 2. spc_da  (APG, LOT-based)
            logCallback(" 2. APG : spc_da ");
            ProcessApgLotByBatch(
                tx, delMode, fromDate, toDate,
                tableName: "spc_da",
                keyColumn: "sda_alot_no",
                dateColumn: "sda_keyin_date",
                fileName: "spc_da.995",
                logCallback: logCallback,
                errCnt: ref errCnt,
                batchSize: batchSize);

            // 3. spc_wb  (APG, LOT-based)
            logCallback(" 3. APG : spc_wb ");
            ProcessApgLotByBatch(
                tx, delMode, fromDate, toDate,
                tableName: "spc_wb",
                keyColumn: "swb_alot_no",
                dateColumn: "swb_keyin_date",
                fileName: "spc_wb.995",
                logCallback: logCallback,
                errCnt: ref errCnt,
                batchSize: batchSize);

            // 4. spc_md  (APG, LOT-based)
            logCallback(" 4. APG : spc_md ");
            ProcessApgLotByBatch(
                tx, delMode, fromDate, toDate,
                tableName: "spc_md",
                keyColumn: "smd_alot_no",
                dateColumn: "smd_keyin_date",
                fileName: "spc_md.995",
                logCallback: logCallback,
                errCnt: ref errCnt,
                batchSize: batchSize);

            // 5. spc_wg  (APG, LOT-based)
            logCallback(" 5. APG : spc_wg ");
            ProcessApgLotByBatch(
                tx, delMode, fromDate, toDate,
                tableName: "spc_wg",
                keyColumn: "swg_alot_no",
                dateColumn: "swg_keyin_date",
                fileName: "spc_wg.995",
                logCallback: logCallback,
                errCnt: ref errCnt,
                batchSize: batchSize);

            // 6. spc_sp  (APG, LOT-based)
            logCallback(" 6. APG : spc_sp ");
            ProcessApgLotByBatch(
                tx, delMode, fromDate, toDate,
                tableName: "spc_sp",
                keyColumn: "ssp_alot_no",
                dateColumn: "ssp_keyin_date",
                fileName: "spc_sp.995",
                logCallback: logCallback,
                errCnt: ref errCnt,
                batchSize: batchSize);

            // 7. spc_tf  (APG, LOT-based)
            logCallback(" 7. APG : spc_tf ");
            ProcessApgLotByBatch(
                tx, delMode, fromDate, toDate,
                tableName: "spc_tf",
                keyColumn: "stf_alot_no",
                dateColumn: "stf_keyin_date",
                fileName: "spc_tf.995",
                logCallback: logCallback,
                errCnt: ref errCnt,
                batchSize: batchSize);


            // 8. spc_di (使用 SKIP/FIRST 進行穩定分批)
            logCallback(" 8. APG : spc_di ");
            try
            {
                int offset = 0;
                int batchNo = 0;
                bool hasMoreData = true;

                while (hasMoreData)
                {
                    // 1. 使用 SKIP {offset} 確保即使不刪除也能翻頁
                    string selBatchSql = $@"
            SELECT SKIP {offset} FIRST {batchSize} *
              FROM eit@prodassys:spc_di
             WHERE sdi_keyin_date >= {fromDate}
               AND sdi_keyin_date <  {toDate}";

                    DataTable dtBatch = IfxDataAccess.ExecuteDataTable(tx, selBatchSql);

         

                    batchNo++;
                    logCallback($"  spc_di 批次 {batchNo} 筆數：{dtBatch.Rows.Count}");

                    // 2. 執行匯出 (無論是否刪除都先匯出備份)
                    DataExporter.ExportData_Append(
                        dtBatch,
                        "spc_di.995",
                        $"PRODASSYS_SPC_AR_DEL{DateTime.Now:yyyyMM}",
                        logCallback);

                    // 3. 處理刪除與位移邏輯
                    if (delMode)
                    {
                        /* 
                           注意：分批刪除不能直接用日期，否則會一次刪光。
                           必須根據這批抓出來的資料(dtBatch)的唯一鍵值或具體內容來刪除。
                           如果 spc_di 沒有單一主鍵，建議至少利用其 keyin_date 與關鍵欄位精確刪除。
                        */
                        // 這裡假設你有辦法識別這批資料，或是你想整段刪除：
                        // 如果要分批刪除，通常建議像上一段 code 一樣，先抓出 ID 清單再 IN (...) 刪除。

                        // 若此表沒主鍵，必須一次刪除全日期區間，則直接結束迴圈：
                        string delSql = $@"
                DELETE FROM eit@prodassys:spc_di
                 WHERE sdi_keyin_date >= {fromDate}
                   AND sdi_keyin_date <  {toDate}";

                        int delRows = IfxDataAccess.ExecuteNonQuery(tx, delSql);
                        logCallback($"  已整批刪除日期區間資料，共 {delRows} 筆。");
                        hasMoreData = false; // 既然全刪了，就不用再跑下一批
                    }
                    else
                    {
                        // 匯出模式：不刪除，則增加 offset 進行翻頁
                        offset += batchSize;

                        // 如果抓到的資料少於 batchSize，代表最後一頁了
                        if (dtBatch.Rows.Count < batchSize)
                            hasMoreData = false;
                    }
                }
                logCallback("  spc_di 處理完成");
            }
            catch (Exception ex)
            {
                RaiseError(ref errCnt, logCallback, "spc_di", ex);
            }

            // 9. spc_bp  (APG, LOT-based)
            logCallback(" 9. APG : spc_bp ");
            ProcessApgLotByBatch(
                tx, delMode, fromDate, toDate,
                tableName: "spc_bp",
                keyColumn: "sbp_alot_no",
                dateColumn: "sbp_keyin_date",
                fileName: "spc_bp.995",
                logCallback: logCallback,
                errCnt: ref errCnt,
                batchSize: batchSize);

            // 10. spc_lp  (APG, LOT-based)
            logCallback(" 10. APG : spc_lp ");
            ProcessApgLotByBatch(
                tx, delMode, fromDate, toDate,
                tableName: "spc_lp",
                keyColumn: "slp_alot_no",
                dateColumn: "slp_keyin_date",
                fileName: "spc_lp.995",
                logCallback: logCallback,
                errCnt: ref errCnt,
                batchSize: batchSize);

            // 11. spc_pmc (日期區間，使用 SKIP 分批處理)
            logCallback(" 11. APG : spc_pmc ");
            try
            {
                int offset = 0; // 紀錄位移筆數
                int batchNo = 0;
                bool hasMoreData = true;

                while (hasMoreData)
                {
                    // 1. 使用 SKIP {offset} FIRST {batchSize} 來實現真正的分頁
                    string selBatchSql = $@"
            SELECT SKIP {offset} FIRST {batchSize} *
              FROM eit@prodassys:spc_pmc
             WHERE spmc_keyin_date >= {fromDate}
               AND spmc_keyin_date <  {toDate}";

                    DataTable dtBatch = IfxDataAccess.ExecuteDataTable(tx, selBatchSql);


                    batchNo++;
                    logCallback($"  spc_pmc 批次 {batchNo} 筆數：{dtBatch.Rows.Count}");

                    // 2. 執行匯出 (無論是否刪除都進行備份)
                    DataExporter.ExportData_Append(
                        dtBatch,
                        "spc_pmc.995",
                        $"PRODASSYS_SPC_AR_DEL{DateTime.Now:yyyyMM}",
                        logCallback);

                    // 3. 根據 delMode 決定如何處理下一批
                    if (delMode)
                    {
                        // 如果要刪除，這張表如果是按日期區間刪除，一次就會刪完
                        string delSql = $@"
                DELETE FROM eit@prodassys:spc_pmc
                 WHERE spmc_keyin_date >= {fromDate}
                   AND spmc_keyin_date <  {toDate}";

                        int delRows = IfxDataAccess.ExecuteNonQuery(tx, delSql);
                        logCallback($"  [刪除模式] 已執行全日期區間刪除，受影響筆數：{delRows}");

                        // 既然全範圍都刪除了，就不需要再跑下一批迴圈
                        hasMoreData = false;
                    }
                    else
                    {
                        // [匯出模式] 增加 offset 才能在下一輪抓到後面 8 筆，解決死迴圈
                        offset += batchSize;
                        logCallback($"  [匯出模式] 已跳過 {offset} 筆資料...");

                        // 如果拿到的資料少於 batchSize，代表已經到最後一頁
                        if (dtBatch.Rows.Count < batchSize)
                            hasMoreData = false;
                    }
                }

                logCallback("  spc_pmc 處理完成");
            }
            catch (Exception ex)
            {
                RaiseError(ref errCnt, logCallback, "spc_pmc", ex);
            }

            // 12. spc_wr (日期區間，使用 SKIP/FIRST 實現分批匯出與刪除)
            logCallback(" 12. APG : spc_wr ");
            try
            {
                int offset = 0; // 用於不刪除模式下的翻頁
                int batchNo = 0;
                bool hasMoreData = true;

                while (hasMoreData)
                {
                    // 使用 SKIP {offset} 確保即使不刪除資料，下一輪也會撈到後面的筆數
                    string selBatchSql = $@"
            SELECT SKIP {offset} FIRST {batchSize} *
              FROM eit@prodassys:spc_wr
             WHERE swr_keyin_date >= {fromDate}
               AND swr_keyin_date <  {toDate}";

                    DataTable dtBatch = IfxDataAccess.ExecuteDataTable(tx, selBatchSql);

         

                    batchNo++;
                    logCallback($"  spc_wr 批次 {batchNo} 筆數：{dtBatch.Rows.Count}");

                    // 無論模式為何，皆先執行匯出備份 (Append 模式)
                    DataExporter.ExportData_Append(
                        dtBatch,
                        "spc_wr.995",
                        $"PRODASSYS_SPC_AR_DEL{DateTime.Now:yyyyMM}",
                        logCallback);

                    if (delMode)
                    {
                        // [刪除模式] 執行全區間刪除
                        string delSql = $@"
                DELETE FROM eit@prodassys:spc_wr
                 WHERE swr_keyin_date >= {fromDate}
                   AND swr_keyin_date <  {toDate}";

                        int delRows = IfxDataAccess.ExecuteNonQuery(tx, delSql);
                        logCallback($"  [刪除模式] 已執行整批刪除，受影響筆數：{delRows}");

                        // 既然整批日期已刪除，就不用再跑下一輪 SKIP 查詢
                        hasMoreData = false;
                    }
                    else
                    {
                        // [匯出模式] 不刪除資料，因此必須增加 offset 才能撈到後面的資料
                        offset += batchSize;
                        logCallback($"  [匯出模式] 已完成批次 {batchNo}，下一批將 Skip {offset} 筆...");

                        // 如果本批抓到的筆數小於 batchSize，表示已到最後一頁
                        if (dtBatch.Rows.Count < batchSize)
                            hasMoreData = false;
                    }
                }

                logCallback("  spc_wr 處理完成");
            }
            catch (Exception ex)
            {
                RaiseError(ref errCnt, logCallback, "spc_wr", ex);
            }

            // 13. spc_ls  (APG, LOT-based)
            logCallback(" 13. APG : spc_ls ");
            ProcessApgLotByBatch(
                tx, delMode, fromDate, toDate,
                tableName: "spc_ls",
                keyColumn: "sls_alot_no",
                dateColumn: "sls_keyin_date",
                fileName: "spc_ls.995",
                logCallback: logCallback,
                errCnt: ref errCnt,
                batchSize: batchSize);

            // 14. spc_pda (日期區間，使用 SKIP/FIRST 實現分頁)
            logCallback(" 14. APG : spc_pda ");
            try
            {
                int offset = 0; // 翻頁位移
                int batchNo = 0;
                bool hasMoreData = true;

                while (hasMoreData)
                {
                    // 加上 SKIP {offset}，即使不刪除也能正確往後抓下一批 8 筆
                    string selBatchSql = $@"
            SELECT SKIP {offset} FIRST {batchSize} *
              FROM eit@prodassys:spc_pda
             WHERE spda_keyin_date >= {fromDate}
               AND spda_keyin_date <  {toDate}";

                    DataTable dtBatch = IfxDataAccess.ExecuteDataTable(tx, selBatchSql);

          

                    batchNo++;
                    logCallback($"  spc_pda 批次 {batchNo} 筆數：{dtBatch.Rows.Count}");

                    // 匯出備份 (Append 模式)
                    DataExporter.ExportData_Append(
                        dtBatch,
                        "spc_pda.995",
                        $"PRODASSYS_SPC_AR_DEL{DateTime.Now:yyyyMM}",
                        logCallback);

                    if (delMode)
                    {
                        // [刪除模式] 執行全區間刪除
                        string delSql = $@"
                DELETE FROM eit@prodassys:spc_pda
                 WHERE spda_keyin_date >= {fromDate}
                   AND spda_keyin_date <  {toDate}";

                        int delRows = IfxDataAccess.ExecuteNonQuery(tx, delSql);
                        logCallback($"  [刪除模式] 已執行整批刪除，筆數：{delRows}");

                        // 刪除後不需再 SKIP，直接跳出迴圈
                        hasMoreData = false;
                    }
                    else
                    {
                        // [匯出模式] 增加 offset 進行翻頁
                        offset += batchSize;
                        logCallback($"  [匯出模式] 批次 {batchNo} 完成，Skip 累計: {offset}");

                        // 若這批抓不滿 batchSize，代表已經到最後一頁
                        if (dtBatch.Rows.Count < batchSize)
                            hasMoreData = false;
                    }
                }

                logCallback("  spc_pda 處理完成");
            }
            catch (Exception ex)
            {
                RaiseError(ref errCnt, logCallback, "spc_pda", ex);
            }

            // 15. spc_criteria_ent (日期區間，使用 SKIP/FIRST 實現分頁)
            logCallback(" 15. APG : spc_criteria_ent ");
            try
            {
                int offset = 0; // 翻頁位移
                int batchNo = 0;
                bool hasMoreData = true;

                while (hasMoreData)
                {
                    // 加入 SKIP {offset} 確保在不刪除的情況下也能翻到下一頁
                    string selBatchSql = $@"
            SELECT SKIP {offset} FIRST {batchSize} *
              FROM eit@prodassys:spc_criteria_ent
             WHERE keyin_date >= {fromDate}
               AND keyin_date <  {toDate}";

                    DataTable dtBatch = IfxDataAccess.ExecuteDataTable(tx, selBatchSql);

              

                    batchNo++;
                    logCallback($"  spc_criteria_ent 批次 {batchNo} 筆數：{dtBatch.Rows.Count}");

                    // 匯出備份 (不論是否刪除皆執行 Append 模式匯出)
                    DataExporter.ExportData_Append(
                        dtBatch,
                        "spc_criteria_ent.995",
                        $"PRODASSYS_SPC_AR_DEL{DateTime.Now:yyyyMM}",
                        logCallback);

                    if (delMode)
                    {
                        // [刪除模式] 執行全日期區間刪除
                        string delSql = $@"
                DELETE FROM eit@prodassys:spc_criteria_ent
                 WHERE keyin_date >= {fromDate}
                   AND keyin_date <  {toDate}";

                        int delRows = IfxDataAccess.ExecuteNonQuery(tx, delSql);
                        logCallback($"  [刪除模式] 已執行整批刪除，影響筆數：{delRows}");

                        // 既然整批已刪除，就不用再執行 SKIP 查詢，直接跳出
                        hasMoreData = false;
                    }
                    else
                    {
                        // [匯出模式] 因為不刪除，必須增加 offset 才能在下一輪抓到後面的資料
                        offset += batchSize;
                        logCallback($"  [匯出模式] 已處理至第 {offset} 筆...");

                        // 若本批抓到的筆數小於 batchSize，表示已到最後一頁
                        if (dtBatch.Rows.Count < batchSize)
                            hasMoreData = false;
                    }
                }

                logCallback("  spc_criteria_ent 處理完成");
            }
            catch (Exception ex)
            {
                RaiseError(ref errCnt, logCallback, "spc_criteria_ent", ex);
            }

            // 16. spc_criteria_lot  (APG, LOT-based)
            logCallback(" 16. APG : spc_criteria_lot ");
            ProcessApgLotByBatch(
                tx, delMode, fromDate, toDate,
                tableName: "spc_criteria_lot",
                keyColumn: "assy_lot",
                dateColumn: "keyin_date",
                fileName: "spc_criteria_lot.995",
                logCallback: logCallback,
                errCnt: ref errCnt,
                batchSize: batchSize);

            // 17. spc_edc  (APG, LOT-based)
            logCallback(" 17. APG : spc_edc ");
            ProcessApgLotByBatch(
                tx, delMode, fromDate, toDate,
                tableName: "spc_edc",
                keyColumn: "ec_lot_number",
                dateColumn: "ec_date",
                fileName: "spc_edc.995",
                logCallback: logCallback,
                errCnt: ref errCnt,
                batchSize: batchSize);
        }

        #endregion

        #region Private helpers

        /// <summary>
        /// APG 類（以 LOT 為 key，且需排除 unship_apg_tmp1）的批次刪除 / 匯出
        /// - delMode = true  → 匯出 + 刪除
        /// - delMode = false → 只匯出，不刪除
        /// </summary>
        private static void ProcessApgLotByBatch(
     IFXTransaction tx,
     bool delMode,
     int fromDate,
     int toDate,
     string tableName,
     string keyColumn,
     string dateColumn,
     string fileName,
     Action<string> logCallback,
     ref short errCnt,
     int batchSize)
        {
            try
            {
                int offset = 0; // 紀錄目前跳過的筆數
                int totalProcessed = 0;
                bool hasMoreData = true;

                logCallback($"[APG:{tableName}] 開始處理 (模式: {(delMode ? "刪除" : "預覽")})");

                while (hasMoreData)
                {
                    // 1. 使用 SKIP 和 FIRST 進行分頁查詢，這樣不刪除也能往後撈
                    string lotListSql = $@"
                SELECT SKIP {offset} FIRST {batchSize} DISTINCT {keyColumn} AS alot_no
                  FROM eit@prodassys:{tableName} t
                 WHERE {dateColumn} < {toDate}
                   AND NOT EXISTS (
                         SELECT 1
                           FROM unship_apg_tmp1 u
                          WHERE u.line = 'APG'
                            AND u.assy_lot = t.{keyColumn}
                       )";

                    DataTable lotDt = IfxDataAccess.ExecuteDataTable(tx, lotListSql);


                    DataExporter.ExportData_Append(
                   new DataTable(),
                   fileName,
                   $"PRODASSYS_SPC_AR_DEL{DateTime.Now:yyyyMM}",
                   logCallback);

                    List<string> lots = new List<string>();
                    foreach (DataRow row in lotDt.Rows)
                    {
                        string lot = (row["alot_no"]?.ToString() ?? string.Empty).Trim();
                        if (!string.IsNullOrEmpty(lot))
                            lots.Add(EscapeQuote(lot));
                    }

                    if (lots.Count > 0)
                    {
                        // 2. 取得這批 Lot 的詳細資料並匯出
                        string inClause = string.Join(",", lots.ConvertAll(l => $"'{l}'"));
                        string selSql = $@"SELECT * FROM eit@prodassys:{tableName} WHERE {keyColumn} IN ({inClause})";

                        // 執行匯出
                        ExportSelected(tx, selSql, fileName, logCallback);

                        // 3. 只有刪除模式才執行 DELETE
                        if (delMode)
                        {
                            string delSql = $@"DELETE FROM eit@prodassys:{tableName} WHERE {keyColumn} IN ({inClause})";
                            IfxDataAccess.ExecuteNonQuery(tx, delSql);

                            // 如果刪除了資料，offset 不需要增加
                            logCallback($"   批次處理 {lots.Count} 筆並刪除...");
                        }
                        else
                        {
                            // 如果不刪除資料，offset 必須增加
                            offset += batchSize;
                            logCallback($"   批次預覽 {lots.Count} 筆 (Skip 累計: {offset})...");
                        }

                        totalProcessed += lots.Count;
                    }
                    else
                    {
                        // 如果這批次沒有撈到任何有效的 lot，則標記為結束

                        hasMoreData = false;
                        logCallback($"   此批次無資料，結束處理。");
                    }

                    // 如果拿到的筆數小於 batchSize，表示沒資料了
                    if (lotDt.Rows.Count < batchSize)
                        hasMoreData = false;


                    //// 2. 取得這批 Lot 的詳細資料並匯出
                    //string inClause = string.Join(",", lots.ConvertAll(l => $"'{l}'"));
                    //string selSql = $@"SELECT * FROM eit@prodassys:{tableName} WHERE {keyColumn} IN ({inClause})";

                    //// 執行匯出 (這裡假設你的 ExportSelected 是 Append 模式)
                    //ExportSelected(tx, selSql, fileName, logCallback);

                    //// 3. 只有刪除模式才執行 DELETE
                    //if (delMode)
                    //{
                    //    string delSql = $@"DELETE FROM eit@prodassys:{tableName} WHERE {keyColumn} IN ({inClause})";
                    //    IfxDataAccess.ExecuteNonQuery(tx, delSql);

                    //    // 【重點】如果刪除了資料，offset 不需要增加，因為後面的資料會自動往前遞補
                    //    logCallback($"   批次處理 {lots.Count} 筆並刪除...");
                    //}
                    //else
                    //{
                    //    // 【重點】如果不刪除資料，offset 必須增加，下一輪才會撈到後面的資料
                    //    offset += batchSize;
                    //    logCallback($"   批次預覽 {lots.Count} 筆 (Skip 累計: {offset})...");
                    //}

                    //totalProcessed += lots.Count;

                    //// 如果拿到的筆數小於 batchSize，表示沒資料了
                    //if (lotDt.Rows.Count < batchSize)
                    //    hasMoreData = false;
                }

                logCallback($"[APG:{tableName}] 處理完成，總計處理 Lot 數: {totalProcessed}");
            }
            catch (Exception ex)
            {
                RaiseError(ref errCnt, logCallback, tableName, ex);
            }
        }

        private static void ExportSelected(
            IFXTransaction tx,
            string selectSql,
            string fileName,
            Action<string> logCallback)
        {
            DataTable dt = IfxDataAccess.ExecuteDataTable(tx, selectSql);
            DataExporter.ExportData_Append(
                dt,
                fileName,
                $"PRODASSYS_SPC_AR_DEL{DateTime.Now:yyyyMM}",
                logCallback);
        }

        private static string EscapeQuote(string s)
        {
            return (s ?? string.Empty).Replace("'", "''");
        }

        private static void RaiseError(
            ref short errCnt,
            Action<string> logCallback,
            string tableName,
            Exception ex)
        {
            errCnt++;
            string msg = $"error---[APG : {tableName}]> " + ex.Message;
            logCallback(msg);
            LogHelper.WriteLog("PRODASSYS_SPC_AR_DEL", msg);
        }

        #endregion
    }
}
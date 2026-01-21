using CPISData.Data;
using MonthBackup_FE.Helper;
using System;
using System.Data;
using System.IO;
using System.Linq;

namespace MonthBackup_FE.AR_DEL.Provider
{
    public static class CommonProvider
    {
        /// <summary>
        /// 刪除 as_pds 資料
        /// </summary>
        public static void DeleteAsPds(IFXTransaction tx, Action<string> logCallback) => DeleteByCchuSubquery(tx, "as_pds", "apds_assylot", logCallback);

        /// <summary>
        /// 刪除 wiplth 資料
        /// </summary>
        public static void DeleteWiplth(IFXTransaction tx, Action<string> logCallback) => DeleteByCchuSubquery(tx, "wiplth", "wlth_lot_number", logCallback);

        /// <summary>
        /// 刪除 wiplsh 資料
        /// </summary>
        public static void DeleteWiplsh(IFXTransaction tx, Action<string> logCallback) => DeleteByCchuSubquery(tx, "wiplsh", "wlsh_lot_number", logCallback);

        /// <summary>
        /// 刪除 wiplha 資料
        /// </summary>
        public static void DeleteWiplha(IFXTransaction tx, Action<string> logCallback) => DeleteByCchuSubquery(tx, "wiplha", "wlha_lot_number", logCallback);

        /// <summary>
        /// 刪除 wiplsp 資料
        /// </summary>
        public static void DeleteWiplsp(IFXTransaction tx, Action<string> logCallback) => DeleteByCchuSubquery(tx, "wiplsp", "wlsp_lot_number", logCallback);

        /// <summary>
        /// 刪除 wiplta 資料
        /// </summary>
        public static void DeleteWiplta(IFXTransaction tx, Action<string> logCallback) => DeleteByCchuSubquery(tx, "wiplta", "wlta_lot_number", logCallback);

        /// <summary>
        /// 刪除 wipdlt 資料
        /// </summary>
        public static void DeleteWipdlt(IFXTransaction tx, Action<string> logCallback) => DeleteByCchuSubquery(tx, "wipdlt", "wdlt_lot_number", logCallback);

        /// <summary>
        /// 刪除 wip_lbr 資料
        /// </summary>
        public static void DeleteWipLbr(IFXTransaction tx, Action<string> logCallback) => DeleteByCchuSubquery(tx, "wip_lbr", "wlbr_lot_number", logCallback);

        /// <summary>
        /// 刪除 oper_turn 資料
        /// </summary>
        public static void DeleteOperTurn(IFXTransaction tx, Action<string> logCallback) => DeleteByCchuSubquery(tx, "oper_turn", "turn_lot_number", logCallback);

        /// <summary>
        /// 刪除 qc_hist 資料
        /// </summary>
        public static void DeleteQcHist(IFXTransaction tx, Action<string> logCallback) => DeleteByCchuSubquery(tx, "qc_hist", "qc_lot_number", logCallback);

        /// <summary>
        /// 刪除 redo_hist 資料
        /// </summary>
        public static void DeleteRedoHist(IFXTransaction tx, Action<string> logCallback) => DeleteByCchuSubquery(tx, "redo_hist", "redo_lot_number", logCallback);

        /// <summary>
        /// 刪除 redo_dat 資料
        /// </summary>
        public static void DeleteRedoDat(IFXTransaction tx, Action<string> logCallback) => DeleteByCchuSubquery(tx, "redo_dat", "redo_lot_number", logCallback);

        /// <summary>
        /// 刪除 fail_hist 資料
        /// </summary>
        public static void DeleteFailHist(IFXTransaction tx, Action<string> logCallback) => DeleteByCchuSubquery(tx, "fail_hist", "fail_lot_number", logCallback);

        /// <summary>
        /// 刪除 samp_hist 資料
        /// </summary>
        public static void DeleteSampHist(IFXTransaction tx, Action<string> logCallback) => DeleteByCchuSubquery(tx, "samp_hist", "samp_lot_number", logCallback);

        /// <summary>
        /// 刪除 mat_hist 資料
        /// </summary>
        public static void DeleteMatHist(IFXTransaction tx, Action<string> logCallback) => DeleteByCchuSubquery(tx, "mat_hist", "mat_lot_number", logCallback);

        /// <summary>
        /// 刪除 wip_del 資料
        /// </summary>
        public static void DeleteWipDel(IFXTransaction tx, Action<string> logCallback) => DeleteByCchuSubquery(tx, "wip_del", "wdel_lot_number", logCallback);

        /// <summary>
        /// 刪除 ship_hist 資料
        /// </summary>
        public static void DeleteShipHist(IFXTransaction tx, Action<string> logCallback) => DeleteByCchuSubquery(tx, "ship_hist", "assy_lot_no", logCallback);

        /// <summary>
        /// 刪除 wip_mvou_entity 資料
        /// </summary>
        public static void DeleteWipMvouEntity(IFXTransaction tx, Action<string> logCallback) => DeleteByCchuSubquery(tx, "wip_mvou_entity", "wmvo_lot_number", logCallback);

        /// <summary>
        /// 刪除 fail_dat 資料
        /// </summary>
        public static void DeleteFailDat(IFXTransaction tx, Action<string> logCallback) => DeleteByCchuSubquery(tx, "fail_dat", "fail_lot_number", logCallback);

        /// <summary>
        /// 刪除 lbl_hist 資料
        /// </summary>
        public static void DeleteLblHist(IFXTransaction tx, Action<string> logCallback) => DeleteByCchuSubquery(tx, "lbl_hist", "lbl_lot_number", logCallback);

        /// <summary>
        /// 刪除 loss_hist 資料
        /// </summary>
        public static void DeleteLossHist(IFXTransaction tx, Action<string> logCallback) => DeleteByCchuSubquery(tx, "loss_hist", "loss_lot_number", logCallback);

        /// <summary>
        /// 刪除 loss_dat 資料
        /// </summary>
        public static void DeleteLossDat(IFXTransaction tx, Action<string> logCallback) => DeleteByCchuSubquery(tx, "loss_dat", "loss_lot_number", logCallback);

        /// <summary>
        /// 刪除 abn_mvou_hist 資料
        /// </summary>
        public static void DeleteAbnMvouHist(IFXTransaction tx, Action<string> logCallback) => DeleteByCchuSubquery(tx, "abn_mvou_hist", "abnm_lot_number", logCallback);

        /// <summary>
        /// 刪除 wip_ent 資料
        /// </summary>
        public static void DeleteWipEnt(IFXTransaction tx, Action<string> logCallback) => DeleteByCchuSubquery(tx, "wip_ent", "went_lot_number", logCallback);

        /// <summary>
        /// 刪除 hold_hist 資料
        /// </summary>
        public static void DeleteHoldHist(IFXTransaction tx, Action<string> logCallback) => DeleteByCchuSubquery(tx, "hold_hist", "lot_number", logCallback);

        /// <summary>
        /// 刪除 bin_hist 資料
        /// </summary>
        public static void DeleteBinHist(IFXTransaction tx, Action<string> logCallback) => DeleteByCchuSubquery(tx, "bin_hist", "bhis_lot_number", logCallback);

        /// <summary>
        /// 刪除 bin_hllt 資料
        /// </summary>
        public static void DeleteBinHllt(IFXTransaction tx, Action<string> logCallback) => DeleteByCchuSubquery(tx, "bin_hllt", "bhlt_lot_number", logCallback);

        /// <summary>
        /// 刪除 waflot_hist (wafer) 資料
        /// </summary>
        public static void DeleteWaflotHist(IFXTransaction tx, Action<string> logCallback) => DeleteByCchuSubquery(tx, "waflot_hist", "wflt_waflot_no", logCallback);

        /// <summary>
        /// 刪除 waflot_bkup (wafer) 資料
        /// </summary>
        public static void DeleteWaflotBkup(IFXTransaction tx, Action<string> logCallback) => DeleteByCchuSubquery(tx, "waflot_bkup", "wfbk_waflot_no", logCallback);

        /// <summary>
        /// 刪除 punch_hist 資料
        /// </summary>
        public static void DeletePunchHist(IFXTransaction tx, Action<string> logCallback) => DeleteByCchuSubquery(tx, "punch_hist", "punch_lot_number", logCallback);

        /// <summary>
        /// 刪除 remark_hist 資料
        /// </summary>
        public static void DeleteRemarkHist(IFXTransaction tx, Action<string> logCallback) => DeleteByCchuSubquery(tx, "remark_hist", "rmk_lot_number", logCallback);

        /// <summary>
        /// 刪除 ink_hist 資料
        /// </summary>
        public static void DeleteInkHist(IFXTransaction tx, Action<string> logCallback) => DeleteByCchuSubquery(tx, "ink_hist", "ink_lot_number", logCallback);

        /// <summary>
        /// 刪除 redo_u2_hist 資料
        /// </summary>
        public static void DeleteRedoU2Hist(IFXTransaction tx, Action<string> logCallback) => DeleteByCchuSubquery(tx, "redo_u2_hist", "redo_u2_lot_number", logCallback);

        /// <summary>
        /// 刪除 rwlt_cp_hist 資料
        /// </summary>
        public static void DeleteRwltCpHist(IFXTransaction tx, Action<string> logCallback) => DeleteByCchuSubquery(tx, "rwlt_cp_hist", "rcp_lot_number", logCallback);

        /// <summary>
        /// 刪除 fail_hist2 資料
        /// </summary>
        public static void DeleteFailHist2(IFXTransaction tx, Action<string> logCallback) => DeleteByCchuSubquery(tx, "fail_hist2", "fail_lot_number", logCallback);

        /// <summary>
        /// 刪除 ilb_wf_chk 資料
        /// </summary>
        public static void DeleteIlbWfChk(IFXTransaction tx, Action<string> logCallback) => DeleteByCchuSubquery(tx, "ilb_wf_chk", "ilwc_taplot_no", logCallback);

        /// <summary>
        /// 刪除 qc_sel_lot 資料
        /// </summary>
        public static void DeleteQcSelLot(IFXTransaction tx, Action<string> logCallback) => DeleteByCchuSubquery(tx, "qc_sel_lot", "lot_number", logCallback);

        /// <summary>
        /// 刪除 wf_vir_splt 資料
        /// </summary>
        public static void DeleteWfVirSplt(IFXTransaction tx, Action<string> logCallback) => DeleteByCchuSubquery(tx, "wf_vir_splt", "wvsp_taplot_no", logCallback);

        /// <summary>
        /// 刪除 wipalr 資料
        /// </summary>
        public static void DeleteWipalr(IFXTransaction tx, Action<string> logCallback) => DeleteByCchuSubquery(tx, "wipalr", "walr_lot_number", logCallback);

        /// <summary>
        /// 刪除 bill_rpt 資料
        /// </summary>
        public static void DeleteBillRpt(IFXTransaction tx, Action<string> logCallback) => DeleteByCchuSubquery(tx, "bill_rpt", "wsm_lot", logCallback);

        /// <summary>
        /// 刪除 mklbl_hist 資料
        /// </summary>
        public static void DeleteMklblHist(IFXTransaction tx, Action<string> logCallback) => DeleteByCchuSubquery(tx, "mklbl_hist", "mklbl_assy_lot", logCallback);

        /// <summary>
        /// 刪除 doc_hist 資料
        /// </summary>
        public static void DeleteDocHist(IFXTransaction tx, Action<string> logCallback) => DeleteByCchuSubquery(tx, "doc_hist", "dhst_lot_number", logCallback);

        /// <summary>
        /// 刪除 lot_mat_rec 資料
        /// </summary>
        public static void DeleteLotMatRec(IFXTransaction tx, Action<string> logCallback) => DeleteByCchuSubquery(tx, "lot_mat_rec", "lmrc_lot_number", logCallback);

        /// <summary>
        /// 刪除 pre_3ip_cplt 資料
        /// </summary>
        public static void DeletePre3ipCplt(IFXTransaction tx, Action<string> logCallback) => DeleteByCchuSubquery(tx, "pre_3ip_cplt", "wlot_lot_number", logCallback);

        /// <summary>
        /// 刪除 cassette_hist 資料
        /// </summary>
        public static void DeleteCassetteHist(IFXTransaction tx, Action<string> logCallback) => DeleteByCchuSubquery(tx, "cassette_hist", "wlot_lot_number", logCallback);

        /// <summary>
        /// 刪除 run_time 資料
        /// </summary>
        public static void DeleteRunTime(IFXTransaction tx, Action<string> logCallback) => DeleteByCchuSubquery(tx, "run_time", "run_lot_number", logCallback);


        /// <summary>
        /// 刪除 wiplot 資料
        /// </summary>
        public static void DeleteWiplot(IFXTransaction tx, Action<string> logCallback) => DeleteByCchuSubquery(tx, "wiplot", "wlot_lot_number", logCallback);

        /// <summary>
        /// 通用刪除封裝方法 (對應 4GL 的 delete 邏輯)
        /// 使用子查詢優化，避免 4GL 的 foreach 迴圈導致多次資料庫往返
        /// </summary>
        //private static void DeleteByCchuSubquery(IFXTransaction tx, string tableName, string lotColumnName)
        //{
        //    try
        //    {
        //        Console.WriteLine($"delete {tableName} .....");

        //        // 使用子查詢一次刪除，對應 4GL 的 foreach + delete 邏輯
        //        string deleteSQL = $@"
        //            DELETE FROM {tableName}
        //            WHERE {lotColumnName} IN (
        //                SELECT wlot_lot_number
        //                FROM cchu
        //            )
        //        ";

        //        int affectedRows = IfxDataAccess.ExecuteNonQuery(tx, deleteSQL);
        //        Console.WriteLine($"{tableName} 刪除完成，影響筆數: {affectedRows}");
        //    }
        //    catch (Exception ex)
        //    {
        //        // 對應 4GL 的 if sqlca.sqlcode <> 0 then goto exit_ar
        //        Console.WriteLine($"== error == 刪除 {tableName} 時發生錯誤: {ex.Message}");
        //        throw; // 拋出例外，讓上層處理 rollback
        //    }
        //}


        private static void DeleteByCchuSubquery(IFXTransaction tx, string tableName, string lotColumnName, Action<string> logCallback, bool delMode = false)
        {
            delMode = GlobalSettings.IsDeleteMode;
            try
            {
                logCallback($"處理 {tableName} .....");

                // 先查詢要刪除的資料
                string selectSQL = $@"
            SELECT *
            FROM {tableName}
            WHERE {lotColumnName} IN (
                SELECT wlot_lot_number
                FROM cchu
            )
        ";

                DataTable queryResult = IfxDataAccess.ExecuteDataTable(tx, selectSQL);


                if (delMode)
                {
                    // 模式：直接刪除
                    //        string deleteSQL = $@"
                    //    DELETE FROM {tableName}
                    //    WHERE {lotColumnName} IN (
                    //        SELECT wlot_lot_number
                    //        FROM cchu
                    //    )
                    //";
                    string deleteSQL = $@"
                        DELETE FROM {tableName}
                        WHERE EXISTS (
                            SELECT 1 
                            FROM cchu 
                            WHERE cchu.wlot_lot_number = {tableName}.{lotColumnName}
                        )";
                    int affectedRows = IfxDataAccess.ExecuteNonQuery(tx, deleteSQL);
                    logCallback($"{tableName} 刪除完成，影響筆數: {affectedRows}");
                    LogHelper.WriteLog("AR_DEL", $"{tableName} 刪除完成，影響筆數: {affectedRows}");

                }
                else
                {
                    // 預設模式：匯出到 .995 檔案
                    string targetFileName = $"{tableName}.995";
                    DataExporter.ExportData(queryResult, targetFileName, $"AR_Del_{DateTime.Now.ToString("yyyyMM")}", logCallback);
                    logCallback($"{tableName} 資料已匯出至 {targetFileName}，共 {queryResult.Rows.Count} 筆");
                    LogHelper.WriteLog("AR_DEL", $"{tableName} 資料已匯出至 {targetFileName}，共 {queryResult.Rows.Count} 筆");


                }

            }
            catch (Exception ex)
            {
                // 對應 4GL 的 if sqlca.sqlcode <> 0 then goto exit_ar
                logCallback($"== error == 處理 {tableName} 時發生錯誤: {ex.Message}");
                throw; // 拋出例外，讓上層處理 rollback
            }
        }

        /// <summary>
        /// 通用查詢封裝方法 (對應 4GL 的 unload 邏輯)
        /// </summary>
        public static DataTable FetchCchuData(IFXTransaction tx, Action<string> logCallback)
        {
            try
            {
                logCallback($"unload cchu..... (Target: cchu.995)");

                // 使用子查詢優化，避免 4GL 的 foreach 迴圈導致多次資料庫往返
                string query = $@"
                SELECT *
                FROM cchu
               ";

                DataTable dt = IfxDataAccess.ExecuteDataTable(tx, query);
                string targetFileName = "cchu.995";
                DataExporter.ExportData(dt, targetFileName, $"AR_Del_{DateTime.Now.ToString("yyyyMM")}", logCallback);
                logCallback($"cchu 資料已匯出至 {targetFileName}，共 {dt.Rows.Count} 筆");
                logCallback($"cchu完成，回傳筆數: {dt?.Rows.Count ?? 0}");
                LogHelper.WriteLog("AR_DEL", $"cchu 資料已匯出至 {targetFileName}，共 {dt.Rows.Count} 筆");

                return dt;
            }
            catch (Exception ex)
            {
                // 對應 4GL 的 if sqlca.sqlcode <> 0 
                logCallback($"== error == 查詢 cchu 發生錯誤: {ex.Message}");
                LogHelper.WriteLog("AR_DEL", $"== error == 查詢 cchu 發生錯誤: {ex.Message}");
                return null;
            }
        }



        //public static void ExportData(DataTable queryResult, string targetFileName,string targetFolderName,Action<string> logCallback)
        //{
        //    // 1. 定義資料夾名稱與路徑
        //    string folderName = $"AR_Del_{DateTime.Now.ToString("yyyyMM")}";
        //    // 取得程式執行目錄下的 BackupOutput 資料夾完整路徑
        //    string folderPath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, folderName);

        //    // 2. 組合完整的檔案路徑
        //    string targetPath = Path.Combine(folderPath, targetFileName);
        //    string tempFileName = Path.Combine(folderPath, "tmp1");

        //    try
        //    {
        //        // 3. 檢查資料夾是否存在，不存在則建立
        //        if (!Directory.Exists(folderPath))
        //        {
        //            Directory.CreateDirectory(folderPath);
        //        }

        //        // 將 DataTable 寫入臨時檔案
        //        using (StreamWriter tempWriter = new StreamWriter(tempFileName))
        //        {
        //            if (queryResult != null)
        //            {
        //                foreach (DataRow row in queryResult.Rows)
        //                {
        //                    string line = string.Join("|", row.ItemArray.Select(item => item?.ToString().Trim() ?? ""));
        //                    tempWriter.WriteLine(line);
        //                }
        //            }
        //        }

        //        // 將臨時檔案追加到目標檔案
        //        using (StreamWriter targetWriter = new StreamWriter(targetPath, append: false))
        //        {
        //            using (StreamReader tempReader = new StreamReader(tempFileName))
        //            {
        //                string line;
        //                while ((line = tempReader.ReadLine()) != null)
        //                {
        //                    targetWriter.WriteLine(line);
        //                }
        //            }
        //        }

        //        logCallback($"成功匯出資料到 {targetPath}");
        //    }
        //    catch (Exception ex)
        //    {
        //        logCallback($"匯出資料時發生錯誤: {ex.Message}");
        //    }
        //    finally
        //    {
        //        // 刪除臨時檔案
        //        if (File.Exists(tempFileName))
        //        {
        //            File.Delete(tempFileName);
        //        }
        //    }
        //}
    }
}
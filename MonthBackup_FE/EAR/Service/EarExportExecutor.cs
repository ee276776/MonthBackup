using System;
using System.Collections.Generic;
using System.Globalization;
using CPISData.Data;
using MonthBackup_FE.EAR.Provider;
using MonthBackup_FE.EAR.Service;
using MonthBackup_FE.EAR_DEL.Provider;
using MonthBackup_FE.Helper;
using MonthBackup_FE.Service;

namespace MonthBackup_FE.EAR.Service
{
    public class EarExportExecutor
    {
        public static void Execute(Dictionary<string, string> parameters, Action<string> logCallback)
        {
            try
            {
                logCallback("========================================");
                logCallback("         EAR 資料匯出程式");
                logCallback("========================================\n");

                // 取得參數
                int begDate = int.Parse(parameters["beg_date"]);
                int endDate = int.Parse(parameters["end_date"]);
                string outputFolder = parameters.ContainsKey("outputFolder")
                    ? parameters["outputFolder"]
                    : $"Ear_{DateTime.Now:yyyyMM}";

                // 驗證日期
                if (!DatabaseProvider.ValidateDate(begDate, out string error1))
                {
                    logCallback($"[X] {error1}");
                    Environment.ExitCode = 1;
                    return;
                }

                if (!DatabaseProvider.ValidateDate(endDate, out string error2))
                {
                    logCallback($"[X] {error2}");
                    Environment.ExitCode = 1;
                    return;
                }

                // 設定輸出資料夾
                EarExportService.SetOutputFolder(outputFolder);
                logCallback($"輸出資料夾: {outputFolder}");
                logCallback($"日期範圍: {begDate} ~ {endDate}\n");

                var startTime = DateTime.Now;
                logCallback("開始時間：" + startTime.ToString("yyyy/MM/dd HH:mm:ss"));
                LogHelper.WriteLog("EAR", "開始時間：" + startTime.ToString("yyyy/MM/dd HH:mm:ss"));
                logCallback("unload START ====\n");

                using (var service = new UserDefineTableService())
                using (var tx = service.BeginTransaction())
                {
                    // SET ISOLATION TO DIRTY READ
                    string setIsolationSql = "SET ISOLATION TO DIRTY READ";
                    IfxDataAccess.ExecuteNonQuery(tx, setIsolationSql);
                    logCallback("已設定 ISOLATION TO DIRTY READ\n");

                    // 取得 entity 清單
                    var entityList = DatabaseProvider.EntityList;
                    logCallback($"已載入 {entityList.Count} 個 entity\n");

                    // 主迴圈：處理每個日期
                    for (int transDate = begDate; transDate <= endDate; transDate++)
                    {
                        logCallback($"========================================");
                        logCallback($"unload START ==== {transDate}");
                        logCallback($"========================================");

                        // 轉換日期格式（給需要 DateTime 的欄位使用）
                        DateTime currentDate = DatabaseProvider.ConvertIntToDate(transDate);

                        // 階段 1: 匯出 ntcenh（使用 IN 查詢）
                        logCallback($"\n--- 階段 1: 匯出 ntcenh ---");
                        EarExportService.ProcessNtcenh(tx, entityList, transDate, logCallback);

                        // 階段 2: 匯出 wipent（使用 IN 查詢）
                        logCallback($"\n--- 階段 2: 匯出 wipent ---");
                        EarExportService.ProcessWipent(tx, entityList, transDate, logCallback);

                        //// 階段 1: 匯出 ntcenh（需要遍歷 entity）
                        //logCallback($"\n--- 階段 1: 匯出 ntcenh ---");
                        //foreach (string entity in entityList)
                        //{
                        //    EarExportService.ExportNtcenh(tx, entity, transDate, logCallback);
                        //}
                        //logCallback("[V] ntcenh 匯出完成\n");

                        //// 階段 2: 匯出 wipent（需要遍歷 entity）
                        //logCallback("--- 階段 2: 匯出 wipent ---");
                        //foreach (string entity in entityList)
                        //{
                        //    EarExportService.ExportWipent(tx, entity, transDate, logCallback);
                        //}
                        //logCallback("[V] wipent 匯出完成\n");

                        // 階段 3: 匯出 ent_job
                        logCallback("--- 階段 3: 匯出 ent_job ---");
                        EarExportService.ExportEntJob(tx, transDate, logCallback);
                        logCallback("[V] ent_job 匯出完成\n");

                        // 階段 4: 匯出 cyctime
                        logCallback("--- 階段 4: 匯出 cyctime ---");
                        EarExportService.ExportCyctime(tx, transDate, logCallback);
                        logCallback("[V] cyctime 匯出完成\n");

                        // 階段 5: 匯出 dacyc
                        logCallback("--- 階段 5: 匯出 dacyc ---");
                        EarExportService.ExportDacyc(tx, transDate, logCallback);
                        logCallback("[V] dacyc 匯出完成\n");

                        // 階段 6: 匯出 wbcyc
                        logCallback("--- 階段 6: 匯出 wbcyc ---");
                        EarExportService.ExportWbcyc(tx, transDate, logCallback);
                        logCallback("[V] wbcyc 匯出完成\n");

                        // 階段 7: 匯出 mdcyc
                        logCallback("--- 階段 7: 匯出 mdcyc ---");
                        EarExportService.ExportMdcyc(tx, transDate, logCallback);
                        logCallback("[V] mdcyc 匯出完成\n");

                        // 階段 8: 匯出 tfcyc
                        logCallback("--- 階段 8: 匯出 tfcyc ---");
                        EarExportService.ExportTfcyc(tx, transDate, logCallback);
                        logCallback("[V] tfcyc 匯出完成\n");

                        // 階段 9: 匯出 wip_onhand
                        logCallback("--- 階段 9: 匯出 wip_onhand ---");
                        EarExportService.ExportWipOnhand(tx, transDate, logCallback);
                        logCallback("[V] wip_onhand 匯出完成\n");

                        // 階段 10: 匯出 curing_h（使用 DateTime）
                        logCallback("--- 階段 10: 匯出 curing_h ---");
                        EarExportService.ExportCuringH(tx, currentDate, logCallback);
                        logCallback("[V] curing_h 匯出完成\n");

                        // 階段 11: 匯出 curing_d（使用 DateTime）
                        logCallback("--- 階段 11: 匯出 curing_d ---");
                        EarExportService.ExportCuringD(tx, currentDate, logCallback);
                        logCallback("[V] curing_d 匯出完成\n");

                        // 階段 12: 匯出 lot_attr_rec
                        logCallback("--- 階段 12: 匯出 lot_attr_rec ---");
                        EarExportService.ExportLotAttrRec(tx, transDate, logCallback);
                        logCallback("[V] lot_attr_rec 匯出完成\n");

                        // 階段 13: 匯出 loc_wb_hist
                        logCallback("--- 階段 13: 匯出 loc_wb_hist ---");
                        EarExportService.ExportLocWbHist(tx, transDate, logCallback);
                        logCallback("[V] loc_wb_hist 匯出完成\n");

                        // 階段 14: 匯出 ent_attr_rec_hist
                        logCallback("--- 階段 14: 匯出 ent_attr_rec_hist ---");
                        EarExportService.ExportEntAttrRecHist(tx, transDate, logCallback);
                        logCallback("[V] ent_attr_rec_hist 匯出完成\n");

                        // 階段 15: 匯出 lf_st_hist
                        logCallback("--- 階段 15: 匯出 lf_st_hist ---");
                        EarExportService.ExportLfStHist(tx, transDate, logCallback);
                        logCallback("[V] lf_st_hist 匯出完成\n");

                        // 階段 16: 匯出 lf_st_extra
                        logCallback("--- 階段 16: 匯出 lf_st_extra ---");
                        EarExportService.ExportLfStExtra(tx, transDate, logCallback);
                        logCallback("[V] lf_st_extra 匯出完成\n");

                        // 階段 17: 匯出 epoxy_hist
                        logCallback("--- 階段 17: 匯出 epoxy_hist ---");
                        EarExportService.ExportEpoxyHist(tx, transDate, logCallback);
                        logCallback("[V] epoxy_hist 匯出完成\n");

                        // 階段 18: 匯出 md_cp_hist
                        logCallback("--- 階段 18: 匯出 md_cp_hist ---");
                        EarExportService.ExportMdCpHist(tx, transDate, logCallback);
                        logCallback("[V] md_cp_hist 匯出完成\n");

                        // 階段 19: 匯出 wafer_id_rec
                        logCallback("--- 階段 19: 匯出 wafer_id_rec ---");
                        EarExportService.ExportWaferIdRec(tx, transDate, logCallback);
                        logCallback("[V] wafer_id_rec 匯出完成\n");

                        // 階段 20: 匯出 special_rllt
                        logCallback("--- 階段 20: 匯出 special_rllt ---");
                        EarExportService.ExportSpecialRllt(tx, transDate, logCallback);
                        logCallback("[V] special_rllt 匯出完成\n");

                        // 階段 21: 匯出 swr_lot
                        logCallback("--- 階段 21: 匯出 swr_lot ---");
                        EarExportService.ExportSwrLot(tx, transDate, logCallback);
                        logCallback("[V] swr_lot 匯出完成\n");

                        // 階段 22: 匯出 efrm 相關表格（使用 DateTime）
                        logCallback("--- 階段 22: 匯出 efrm 相關表格 ---");
                        EarExportService.ExportEfrmTables(tx, currentDate, logCallback);
                        logCallback("[V] efrm 相關表格匯出完成\n");

                        // 階段 23: 匯出 ent_lot
                        logCallback("--- 階段 23: 匯出 ent_lot ---");
                        EarExportService.ExportEntLot(tx, transDate, logCallback);
                        logCallback("[V] ent_lot 匯出完成\n");

                        // 階段 24: 匯出 ent_lot1
                        logCallback("--- 階段 24: 匯出 ent_lot1 ---");
                        EarExportService.ExportEntLot1(tx, transDate, logCallback);
                        logCallback("[V] ent_lot1 匯出完成\n");

                        // 階段 25: 匯出 ent_use
                        logCallback("--- 階段 25: 匯出 ent_use ---");
                        EarExportService.ExportEntUse(tx, transDate, logCallback);
                        logCallback("[V] ent_use 匯出完成\n");

                        // 階段 26: 匯出 sta_transa
                        logCallback("--- 階段 26: 匯出 sta_transa ---");
                        EarExportService.ExportStaTransa(tx, transDate, logCallback);
                        logCallback("[V] sta_transa 匯出完成\n");

                        logCallback($"[V] 日期 {transDate} 匯出完成\n");
                    }

                    // 提交交易
                    logCallback("--- 提交交易 (Commit) ---");
                    tx.Commit();
                    logCallback("[V] 交易已成功提交\n");
                }

                var endTime = DateTime.Now;
                TimeSpan duration = endTime - startTime;
                logCallback("結束時間：" + endTime.ToString("yyyy/MM/dd HH:mm:ss"));
                logCallback($"歷經時間：{(int)duration.TotalHours:00} 小時 {duration.Minutes:00} 分 {duration.Seconds:00} 秒");
                LogHelper.WriteLog("EAR", "結束時間：" + endTime.ToString("yyyy/MM/dd HH:mm:ss"));
                LogHelper.WriteLog("EAR", $"歷經時間：{(int)duration.TotalHours:00} 小時 {duration.Minutes:00} 分 {duration.Seconds:00} 秒");

                logCallback("\n========================================");
                logCallback("[V] EAR 資料匯出完成！");
                logCallback("========================================");
                LogHelper.WriteLog("EAR", "[V] EAR 資料匯出完成！");
                LogHelper.ExecuteRecord("ear.4ge", $"{DateTime.Now.ToString("f", new CultureInfo("zh-TW"))}", $"日期範圍: {begDate} ~ {endDate}");
            }
            catch (Exception ex)
            {
                logCallback("\n========================================");
                logCallback($"[X] 程式執行出現錯誤: {ex.Message}");
                logCallback($"[X] 堆疊追蹤:\n{ex.StackTrace}");
                logCallback("========================================");
                LogHelper.WriteLog("EAR", $"[X] 程式執行出現錯誤: {ex.Message}");
                Environment.ExitCode = 1;
            }
        }
    }
}
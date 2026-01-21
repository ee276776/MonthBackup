using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using CPISData.Data;
using MonthBackup_FE.EAR.Provider;
using MonthBackup_FE.EAR.Service;
using MonthBackup_FE.EAR_DEL.Provider;
using MonthBackup_FE.EAR_DEL.Service;
using MonthBackup_FE.Helper;
using MonthBackup_FE.Service;

namespace MonthBackup_FE.EAR_DEL.Service
{
    public class EarDeleteExecutor
    {
        public static void Execute(Dictionary<string, string> parameters, Action<string> logCallback)
        {
            try
            {
                logCallback("========================================");
                logCallback(EarDeleteProvider.IsDeleteMode ? "         EAR 資料刪除程式" : "         EAR 待刪除資料匯出程式");
                logCallback("========================================\n");

                // 取得參數
                int begDate = int.Parse(parameters["beg_date"]);
                int endDate = int.Parse(parameters["end_date"]);
                string outputFolder = parameters.ContainsKey("outputFolder")
                    ? parameters["outputFolder"]
                    : $"EarDel_{DateTime.Now:yyyyMM}";

                // 設定刪除模式
                if (parameters.ContainsKey("deleteMode"))
                {
                    EarDeleteProvider.IsDeleteMode = bool.Parse(parameters["deleteMode"]);
                }
                else
                {
                    EarDeleteProvider.IsDeleteMode = GlobalSettings.IsDeleteMode;
                }

                // 驗證日期
                if (!EarDeleteProvider.ValidateDate(begDate, out string error1))
                {
                    logCallback($"[X] {error1}");
                    Environment.ExitCode = 1;
                    return;
                }

                if (!EarDeleteProvider.ValidateDate(endDate, out string error2))
                {
                    logCallback($"[X] {error2}");
                    Environment.ExitCode = 1;
                    return;
                }

                // 設定輸出資料夾（如果是匯出模式）
                if (!EarDeleteProvider.IsDeleteMode)
                {
                    EarDeleteService.SetOutputFolder(outputFolder);
                    logCallback($"輸出資料夾: {outputFolder}");
                }

                logCallback($"操作模式: {(EarDeleteProvider.IsDeleteMode ? "刪除" : "匯出")}");
                logCallback($"日期範圍: {begDate} ~ {endDate}\n");

                var startTime = DateTime.Now;
                logCallback("開始時間：" + startTime.ToString("yyyy/MM/dd HH:mm:ss"));
                LogHelper.WriteLog("EAR_DEL", "開始時間：" + startTime.ToString("yyyy/MM/dd HH:mm:ss"));
                logCallback("delete START ====\n");

                using (var service = new UserDefineTableService())
                using (var tx = service.BeginTransaction())
                {
                    // SET ISOLATION TO DIRTY READ
                    string setIsolationSql = "SET ISOLATION TO DIRTY READ";
                    IfxDataAccess.ExecuteNonQuery(tx, setIsolationSql);
                    logCallback("已設定 ISOLATION TO DIRTY READ\n");

                    // 取得 entity 清單
                    var entityList = EarDeleteProvider.EntityList;
                    logCallback($"已載入 {entityList.Count} 個 entity\n");

                    // 主迴圈：處理每個日期
                    for (int transDate = begDate; transDate <= endDate; transDate++)
                    {
                        logCallback($"========================================");
                        logCallback($"delete START ==== {transDate}");
                        logCallback($"========================================");

                        // 轉換日期格式
                        DateTime currentDate = EarDeleteProvider.ConvertIntToDate(transDate);
                        string receDate = EarDeleteProvider.ConvertIntToDateString(transDate);

                        // 階段 1: 處理 ntcenh 和 wipent（需要遍歷 entity）
                        logCallback($"\n--- 階段 1: 處理 ntcenh 和 wipent ---");

                        EarDeleteService.ProcessNtcenh(tx, entityList, transDate, logCallback);
                        EarDeleteService.ProcessWipent(tx, entityList, transDate, logCallback);

                        //foreach (string entity in entityList)
                        //{
                        //    // 處理 ntcenh - 需要檢查計數
                        //    EarDeleteService.ProcessNtcenh(tx, entity, transDate, logCallback);

                        //    // 檢查是否跳過 wipent
                        //    if (EarDeleteProvider.ShouldSkipEntityDate(entity, transDate))
                        //    {
                        //        logCallback($"跳過 wipent: {entity} {transDate}");
                        //        continue;
                        //    }

                        //    // 處理 wipent
                        //    EarDeleteService.ProcessWipent(tx, entity, transDate, logCallback);
                        //}
                        logCallback("[V] ntcenh 和 wipent 處理完成\n");

                        // 階段 2: 處理 ent_job
                        logCallback("--- 階段 2: 處理 ent_job ---");
                        EarDeleteService.ProcessEntJob(tx, transDate, logCallback);
                        logCallback("[V] ent_job 處理完成\n");

                        // 階段 3: 處理 backup_ejb (2003/06/18 only delete, not backup)
                        logCallback("--- 階段 3: 處理 backup_ejb ---");
                        EarDeleteService.ProcessBackupEjb(tx, transDate, logCallback);
                        logCallback("[V] backup_ejb 處理完成\n");

                        // 階段 4: 處理 cyctime
                        logCallback("--- 階段 4: 處理 cyctime ---");
                        EarDeleteService.ProcessCyctime(tx, transDate, logCallback);
                        logCallback("[V] cyctime 處理完成\n");

                        // 階段 5: 處理 dacyc
                        logCallback("--- 階段 5: 處理 dacyc ---");
                        EarDeleteService.ProcessDacyc(tx, transDate, logCallback);
                        logCallback("[V] dacyc 處理完成\n");

                        // 階段 6: 處理 wbcyc
                        logCallback("--- 階段 6: 處理 wbcyc ---");
                        EarDeleteService.ProcessWbcyc(tx, transDate, logCallback);
                        logCallback("[V] wbcyc 處理完成\n");

                        // 階段 7: 處理 mdcyc
                        logCallback("--- 階段 7: 處理 mdcyc ---");
                        EarDeleteService.ProcessMdcyc(tx, transDate, logCallback);
                        logCallback("[V] mdcyc 處理完成\n");

                        // 階段 8: 處理 tfcyc
                        logCallback("--- 階段 8: 處理 tfcyc ---");
                        EarDeleteService.ProcessTfcyc(tx, transDate, logCallback);
                        logCallback("[V] tfcyc 處理完成\n");

                        // 階段 9: 處理 wip_onhand
                        logCallback("--- 階段 9: 處理 wip_onhand ---");
                        EarDeleteService.ProcessWipOnhand(tx, transDate, logCallback);
                        logCallback("[V] wip_onhand 處理完成\n");

                        // 階段 10: 處理 turn_lot_hist 和 turn_hist
                        logCallback("--- 階段 10: 處理 turn_lot_hist 和 turn_hist ---");
                        EarDeleteService.ProcessTurnHist(tx, transDate, logCallback);
                        logCallback("[V] turn_hist 處理完成\n");

                        // 階段 11: 處理 csj_file 和 csi_file
                        logCallback("--- 階段 11: 處理 csj_file 和 csi_file ---");
                        EarDeleteService.ProcessCsiCsjFile(tx, receDate, logCallback);
                        logCallback("[V] csi_file 和 csj_file 處理完成\n");

                        // 階段 12: 處理 curing_h
                        logCallback("--- 階段 12: 處理 curing_h ---");
                        EarDeleteService.ProcessCuringH(tx, currentDate, logCallback);
                        logCallback("[V] curing_h 處理完成\n");

                        // 階段 13: 處理 curing_d
                        logCallback("--- 階段 13: 處理 curing_d ---");
                        EarDeleteService.ProcessCuringD(tx, currentDate, logCallback);
                        logCallback("[V] curing_d 處理完成\n");

                        // 階段 14: 處理 lot_attr_rec
                        logCallback("--- 階段 14: 處理 lot_attr_rec ---");
                        EarDeleteService.ProcessLotAttrRec(tx, transDate, logCallback);
                        logCallback("[V] lot_attr_rec 處理完成\n");

                        // 階段 15: 處理 loc_wb_hist
                        logCallback("--- 階段 15: 處理 loc_wb_hist ---");
                        EarDeleteService.ProcessLocWbHist(tx, transDate, logCallback);
                        logCallback("[V] loc_wb_hist 處理完成\n");

                        // 階段 16: 處理 ent_attr_rec_hist
                        logCallback("--- 階段 16: 處理 ent_attr_rec_hist ---");
                        EarDeleteService.ProcessEntAttrRecHist(tx, transDate, logCallback);
                        logCallback("[V] ent_attr_rec_hist 處理完成\n");

                        // 階段 17: 處理 lf_st_hist
                        logCallback("--- 階段 17: 處理 lf_st_hist ---");
                        EarDeleteService.ProcessLfStHist(tx, transDate, logCallback);
                        logCallback("[V] lf_st_hist 處理完成\n");

                        // 階段 18: 處理 lf_st_extra
                        logCallback("--- 階段 18: 處理 lf_st_extra ---");
                        EarDeleteService.ProcessLfStExtra(tx, transDate, logCallback);
                        logCallback("[V] lf_st_extra 處理完成\n");

                        // 階段 19: 處理 epoxy_hist
                        logCallback("--- 階段 19: 處理 epoxy_hist ---");
                        EarDeleteService.ProcessEpoxyHist(tx, transDate, logCallback);
                        logCallback("[V] epoxy_hist 處理完成\n");

                        // 階段 20: 處理 md_cp_hist
                        logCallback("--- 階段 20: 處理 md_cp_hist ---");
                        EarDeleteService.ProcessMdCpHist(tx, transDate, logCallback);
                        logCallback("[V] md_cp_hist 處理完成\n");

                        // 階段 21: 處理 wafer_id_rec
                        logCallback("--- 階段 21: 處理 wafer_id_rec ---");
                        EarDeleteService.ProcessWaferIdRec(tx, transDate, logCallback);
                        logCallback("[V] wafer_id_rec 處理完成\n");

                        // 階段 22: 處理 special_rllt
                        logCallback("--- 階段 22: 處理 special_rllt ---");
                        EarDeleteService.ProcessSpecialRllt(tx, transDate, logCallback);
                        logCallback("[V] special_rllt 處理完成\n");

                        // 階段 23: 處理 swr_lot
                        logCallback("--- 階段 23: 處理 swr_lot ---");
                        EarDeleteService.ProcessSwrLot(tx, transDate, logCallback);
                        logCallback("[V] swr_lot 處理完成\n");

                        // 階段 24: 處理 efrm 相關表格
                        logCallback("--- 階段 24: 處理 efrm 相關表格 ---");
                        EarDeleteService.ProcessEfrmTables(tx, currentDate, logCallback);
                        logCallback("[V] efrm 相關表格處理完成\n");

                        // 階段 25: 處理 ent_lot
                        logCallback("--- 階段 25: 處理 ent_lot ---");
                        EarDeleteService.ProcessEntLot(tx, transDate, logCallback);
                        logCallback("[V] ent_lot 處理完成\n");

                        // 階段 26: 處理 ent_lot1
                        logCallback("--- 階段 26: 處理 ent_lot1 ---");
                        EarDeleteService.ProcessEntLot1(tx, transDate, logCallback);
                        logCallback("[V] ent_lot1 處理完成\n");

                        // 階段 27: 處理 ent_use
                        logCallback("--- 階段 27: 處理 ent_use ---");
                        EarDeleteService.ProcessEntUse(tx, transDate, logCallback);
                        logCallback("[V] ent_use 處理完成\n");

                        // 階段 28: 處理 sta_transa
                        logCallback("--- 階段 28: 處理 sta_transa ---");
                        EarDeleteService.ProcessStaTransa(tx, transDate, logCallback);
                        logCallback("[V] sta_transa 處理完成\n");

                        logCallback($"[V] 日期 {transDate} 處理完成\n");

                        // 對應 4GL: SLEEP 120
                        //logCallback("等待 120 秒...");
                        //Thread.Sleep(120000); // 120 秒
                        Thread.Sleep(5000); // 等待5秒

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
                LogHelper.WriteLog("EAR_DEL", "結束時間：" + endTime.ToString("yyyy/MM/dd HH:mm:ss"));
                LogHelper.WriteLog("EAR_DEL", $"歷經時間：{(int)duration.TotalHours:00} 小時 {duration.Minutes:00} 分 {duration.Seconds:00} 秒");
                logCallback("\n========================================");
                logCallback(EarDeleteProvider.IsDeleteMode ? "[V] EAR 資料刪除完成！" : "[V] EAR 資料匯出完成！");
                LogHelper.WriteLog("EAR_DEL", EarDeleteProvider.IsDeleteMode ? "[V] EAR 資料刪除完成！" : "[V] EAR 資料匯出完成！");
                LogHelper.ExecuteRecord("ear_del.4ge", $"{DateTime.Now.ToString("f", new CultureInfo("zh-TW"))}", $"日期範圍: {begDate} ~ {endDate}");

                logCallback("========================================");
            }
            catch (Exception ex)
            {
                logCallback("\n========================================");
                logCallback($"[X] 程式執行出現錯誤: {ex.Message}");
                logCallback($"[X] 堆疊追蹤:\n{ex.StackTrace}");
                logCallback("========================================");
                LogHelper.WriteLog("EAR_DEL", $"[X] 程式執行出現錯誤: {ex.Message}");

                Environment.ExitCode = 1;
            }
        }
    }
}
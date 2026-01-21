using System;
using System.Collections.Generic;
using System.Globalization;
using CPISData.Data;
using MonthBackup_FE.AR_EXTEND.Provider;
using MonthBackup_FE.AR_EXTEND.Service;
using MonthBackup_FE.Helper;
using MonthBackup_FE.Service;

namespace MonthBackup_FE.AR_EXTEND.Service
{
    /// <summary>
    /// 對應 ar_extend.4gl MAIN
    /// </summary>
    public class ArExtendExecutor
    {
        /// <summary>
        /// parameters 需包含：
        /// - end_date : 4GL 的 arg_val(1)，字串 yyyyMMdd（原 4GL 只是記錄）
        /// - flag     : 4GL 的 arg_val(2)，"UNLOAD" 或 "DELETE"
        /// - file_name: 4GL 的 arg_val(3)，例如 cchu.995
        /// </summary>
        public static void Execute(Dictionary<string, string> parameters, Action<string> logCallback)
        {
            try
            {
                string endDate = parameters.ContainsKey("end_date") ? parameters["end_date"] : string.Empty;
                string flag = parameters.ContainsKey("flag") ? parameters["flag"] : "UNLOAD";
                string fileName = parameters.ContainsKey("file_name") ? parameters["file_name"] : string.Empty;

                flag = flag.Trim().ToUpper();

                ArExtendProvider.EndDateRaw = endDate;
                ArExtendProvider.Flag = flag;
                ArExtendProvider.LotFileName = fileName;

                bool isDelete = (flag == "DELETE");

                logCallback("========================================");
                logCallback(isDelete
                    ? "       AR_EXTEND 刪除程式 (DELETE)"
                    : "       AR_EXTEND 匯出程式 (UNLOAD)");
                logCallback("========================================");
                logCallback(string.Empty);

                logCallback("參數 end_date : " + endDate);
                logCallback("參數 flag     : " + flag);
                logCallback("參數 file_name: " + fileName);
                logCallback(string.Empty);

                DateTime startTime = DateTime.Now;
                logCallback("開始時間：" + startTime.ToString("yyyy/MM/dd HH:mm:ss"));
                LogHelper.WriteLog("AR_EXTEND", "開始時間：" + startTime.ToString("yyyy/MM/dd HH:mm:ss"));

                using (UserDefineTableService service = new UserDefineTableService())
                using (IFXTransaction tx = service.BeginTransaction())
                {
                    // SET ISOLATION TO DIRTY READ
                    IfxDataAccess.ExecuteNonQuery(tx, "SET ISOLATION TO DIRTY READ");
                    logCallback("已設定 ISOLATION TO DIRTY READ");
                    logCallback(string.Empty);

                    // 建立 TEMP TABLE ar_temp
                    logCallback("建立暫存表 ar_temp ...");
                    string createTempSql = @"
CREATE TEMP TABLE ar_temp
(
    wlot_lot_number VARCHAR(11)
) WITH NO LOG";
                    IfxDataAccess.ExecuteNonQuery(tx, createTempSql);
                    logCallback("暫存表 ar_temp 已建立");

                    // LOAD FROM g.file_name INSERT INTO ar_temp
                    logCallback("從檔案載入 lot 資料到 ar_temp: " + fileName);
                    ArExtendService.LoadArTempFromFile(tx, fileName, isDelete, logCallback);

                    // CALL gen_ar_lot()
                    logCallback("執行 gen_ar_lot() ...");
                    ArExtendService.GenArLot(tx, logCallback);
                    logCallback("gen_ar_lot() 完成");

                    // CALL gen_ar_date()
                    logCallback("執行 gen_ar_date() ...");
                    ArExtendService.GenArDate(tx, logCallback);
                    logCallback("gen_ar_date() 完成");

                    // DROP TABLE ar_temp
                    logCallback("刪除暫存表 ar_temp ...");
                    IfxDataAccess.ExecuteNonQuery(tx, "DROP TABLE ar_temp");
                    logCallback("暫存表 ar_temp 已刪除");

                    tx.Commit();
                    logCallback("[V] 交易已成功提交");
                }

                DateTime endTime = DateTime.Now;
                TimeSpan duration = endTime - startTime;
                logCallback("結束時間：" + endTime.ToString("yyyy/MM/dd HH:mm:ss"));
                logCallback(string.Format("歷經時間：{0:00} 小時 {1:00} 分 {2:00} 秒",
                    (int)duration.TotalHours, duration.Minutes, duration.Seconds));
                LogHelper.WriteLog("AR_EXTEND", "結束時間：" + endTime.ToString("yyyy/MM/dd HH:mm:ss"));
                LogHelper.WriteLog("AR_EXTEND", string.Format("歷經時間：{0:00} 小時 {1:00} 分 {2:00} 秒",
                    (int)duration.TotalHours, duration.Minutes, duration.Seconds));

                logCallback(string.Empty);
                logCallback("========================================");
                logCallback(isDelete
                    ? "[V] AR_EXTEND DELETE 完成！"
                    : "[V] AR_EXTEND UNLOAD 完成！");
                LogHelper.WriteLog("AR_EXTEND", isDelete
                    ? "[V] AR_EXTEND DELETE 完成！"
                    : "[V] AR_EXTEND UNLOAD 完成！");
                LogHelper.ExecuteRecord("ar_extend.4ge", $"{DateTime.Now.ToString("f", new CultureInfo("zh-TW"))}", $"endDate: {endDate}");

                logCallback("========================================");
            }
            catch (Exception ex)
            {
                logCallback(string.Empty);
                logCallback("========================================");
                logCallback("[X] AR_EXTEND 執行出現錯誤: " + ex.Message);
                logCallback("[X] 堆疊追蹤:");
                logCallback(ex.StackTrace ?? string.Empty);
                logCallback("========================================");
                LogHelper.WriteLog("AR_EXTEND", "[X] 程式執行出現錯誤: " + ex.Message);
                Environment.ExitCode = 1;
            }
        }
    }
}
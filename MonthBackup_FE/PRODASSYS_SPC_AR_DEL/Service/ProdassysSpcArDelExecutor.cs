using System;
using System.Collections.Generic;
using System.Globalization;
using CPISData.Data;
using MonthBackup_FE.Helper;
using MonthBackup_FE.PRODASSYS_SPC_AR_DEL.Provider;
using MonthBackup_FE.PRODASSYS_SPC_AR_DEL.Service;
using MonthBackup_FE.Service;

namespace MonthBackup_FE.PRODASSYS_SPC_AR_DEL.Service
{
    /// <summary>
    /// 對應 prodassys_spc_ar_del.4gl MAIN
    /// </summary>
    public class ProdassysSpcArDelExecutor
    {
        /// <summary>
        /// parameters:
        /// - v_run_date (選填): yyyyMM / yyyy-MM / yyyyMMdd，若沒帶就用今天的 yyyy-MM
        ///   對應 4GL 的 v_run_date / arg_val(1)
        /// </summary>
        public static void Execute(Dictionary<string, string> parameters, Action<string> logCallback)
        {
            try
            {
                // 1. 初始化日期
                string runDate = parameters.ContainsKey("v_run_date")
                    ? parameters["v_run_date"]
                    : string.Empty;

                ProdassysSpcArDelProvider.InitDates(runDate);

                string actualRunDate = ProdassysSpcArDelProvider.RunDateRaw;
                int fromDate = ProdassysSpcArDelProvider.FromDateInt;
                int toDate = ProdassysSpcArDelProvider.ToDateInt;

                logCallback("*****************************************");
                logCallback("  run date : " + actualRunDate);
                logCallback("  PRODASSYS SPC DELETE");
                logCallback(string.Empty);
                logCallback("  From Date: " + fromDate);
                logCallback("  To Date  : " + toDate);
                logCallback(string.Empty);

                DateTime startTime = DateTime.Now;
                logCallback("開始時間：" + startTime.ToString("yyyy/MM/dd HH:mm:ss"));
                LogHelper.WriteLog("PRODASSYS_SPC_AR_DEL", "開始時間：" + startTime.ToString("yyyy/MM/dd HH:mm:ss"));

                short errCnt = 0;

                using (UserDefineTableService service = new UserDefineTableService())
                using (IFXTransaction tx = service.BeginTransaction())
                {
                    // SET isolation TO dirty read
                    IfxDataAccess.ExecuteNonQuery(tx, "SET ISOLATION TO DIRTY READ");

                    // 建 TEMP TABLE unship_apg_tmp1
                    logCallback("建立暫存表 unship_apg_tmp1 ...");
                    string createTempSql = @"
CREATE TEMP TABLE unship_apg_tmp1
(
    line      CHAR(4),
    assy_lot  CHAR(12),
    from_date INTEGER,
    to_date   INTEGER
) WITH NO LOG";
                    IfxDataAccess.ExecuteNonQuery(tx, createTempSql);
                    logCallback("暫存表 unship_apg_tmp1 已建立");

                    // non_ship_data()
                    logCallback("執行 non_ship_data() ...");
                    ProdassysSpcArDelService.NonShipData(tx, fromDate, toDate, ref errCnt, logCallback);
                    logCallback("non_ship_data() 完成");

                    // del_data()
                    logCallback("執行 del_data() ...");
                    ProdassysSpcArDelService.DelData(tx, fromDate, toDate, ref errCnt, logCallback,200000);
                    logCallback("del_data() 完成");

                    // DROP TEMP TABLE
                    logCallback("刪除暫存表 unship_apg_tmp1 ...");
                    IfxDataAccess.ExecuteNonQuery(tx, "DROP TABLE unship_apg_tmp1");
                    logCallback("暫存表 unship_apg_tmp1 已刪除");

                    tx.Commit();
                    logCallback("[V] 交易已成功提交");
                }

                // 4GL: 寄信 —— delete 版主旨是 spc_system_delete
                string mailTo = "MESSUP07@chipmos.com.tw";
                string fromStr = ProdassysSpcArDelProvider.FromDateInt.ToString();
                string yyyymm = fromStr.Length >= 6 ? fromStr.Substring(0, 6) : fromStr;

                if (errCnt != 0)
                {
                    string subject = yyyymm + " APG spc_system_delete !!ERROR!!";
                    string body = "PRODASSYS SPC 刪除完成，但發生 " + errCnt + " 筆錯誤，請檢查 log。";

                    logCallback("刪除完成，但發生 " + errCnt + " 筆錯誤，請檢查 log。");
                    LogHelper.WriteLog("PRODASSYS_SPC_AR_DEL", subject);

                    MailHelper.SendMail(subject, body, mailTo);
                }
                else
                {
                    string subject = yyyymm + " APG spc_system_delete complete";
                    string body = "PRODASSYS SPC 刪除完成，無錯誤。";

                    logCallback("刪除完成，無錯誤。");
                    LogHelper.WriteLog("PRODASSYS_SPC_AR_DEL", subject);

                    MailHelper.SendMail(subject, body, mailTo);
                }

                DateTime endTime = DateTime.Now;
                TimeSpan duration = endTime - startTime;

                logCallback("結束時間：" + endTime.ToString("yyyy/MM/dd HH:mm:ss"));
                logCallback(string.Format("歷經時間：{0:00} 小時 {1:00} 分 {2:00} 秒",
                    (int)duration.TotalHours, duration.Minutes, duration.Seconds));
                LogHelper.WriteLog("PRODASSYS_SPC_AR_DEL",
                    "結束時間：" + endTime.ToString("yyyy/MM/dd HH:mm:ss"));
                LogHelper.WriteLog("PRODASSYS_SPC_AR_DEL",
                    string.Format("歷經時間：{0:00} 小時 {1:00} 分 {2:00} 秒",
                        (int)duration.TotalHours, duration.Minutes, duration.Seconds));
                LogHelper.ExecuteRecord("prodassys_spc_ar_del.4ge", $"{DateTime.Now.ToString("f", new CultureInfo("zh-TW"))}", $"runDate: {runDate}");

                logCallback("*****************************************");
                logCallback("[V] PRODASSYS SPC 資料刪除完成！");
            }
            catch (Exception ex)
            {
                logCallback("========================================");
                logCallback("[X] PRODASSYS_SPC_AR_DEL 執行出現錯誤: " + ex.Message);
                logCallback("[X] 堆疊追蹤:");
                logCallback(ex.StackTrace ?? string.Empty);
                logCallback("========================================");

                LogHelper.WriteLog("PRODASSYS_SPC_AR_DEL", "[X] 程式執行出現錯誤: " + ex.Message);
                Environment.ExitCode = 1;
            }
        }
    }
}
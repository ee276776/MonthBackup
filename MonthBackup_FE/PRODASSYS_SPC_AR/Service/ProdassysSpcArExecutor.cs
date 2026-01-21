using System;
using System.Collections.Generic;
using System.Globalization;
using CPISData.Data;
using MonthBackup_FE.Helper;
using MonthBackup_FE.PRODASSYS_SPC_AR.Provider;
using MonthBackup_FE.PRODASSYS_SPC_AR.Service;
using MonthBackup_FE.Service;

namespace MonthBackup_FE.PRODASSYS_SPC_AR.Service
{
    /// <summary>
    /// 對應 prodassys_spc_ar.4gl MAIN
    /// </summary>
    public class ProdassysSpcArExecutor
    {
        /// <summary>
        /// parameters:
        /// - run_date (選填): yyyy-MM，若沒帶就用今天的 yyyy-MM
        /// </summary>
        public static void Execute(Dictionary<string, string> parameters, Action<string> logCallback)
        {
            try
            {
                // 1. 初始化日期
                string runDate = parameters.ContainsKey("v_run_date")
                    ? parameters["v_run_date"]
                    : string.Empty;

                ProdassysSpcArProvider.InitDates(runDate);

                string actualRunDate = ProdassysSpcArProvider.RunDateRaw;
                int fromDate = ProdassysSpcArProvider.FromDateInt;
                int toDate = ProdassysSpcArProvider.ToDateInt;

                logCallback("*****************************************");
                logCallback("  run date : " + actualRunDate);
                logCallback("  PRODASSYS  ");
                logCallback(string.Empty);
                logCallback(" from date : " + fromDate);
                logCallback("   to date : " + toDate);
                logCallback(string.Empty);

                DateTime startTime = DateTime.Now;
                logCallback("開始時間：" + startTime.ToString("yyyy/MM/dd HH:mm:ss"));
                LogHelper.WriteLog("PRODASSYS_SPC_AR", "開始時間：" + startTime.ToString("yyyy/MM/dd HH:mm:ss"));

                short errCnt = 0;

                using (UserDefineTableService service = new UserDefineTableService())
                using (IFXTransaction tx = service.BeginTransaction())
                {
                    // SET isolation TO dirty read
                    IfxDataAccess.ExecuteNonQuery(tx, "SET ISOLATION TO DIRTY READ");

                    // 執行 backup_data() 對應的 C# 方法
                    ProdassysSpcArService.BackupData(tx, toDate, ref errCnt, logCallback);

                    tx.Commit();
                    logCallback("[V] 交易已成功提交");
                }

                // ===== 這一段就是 4GL 的 mail 邏輯，已改成用 MailHelper 寄信 =====

                string mailTo = "MESSUP07@chipmos.com.tw"; // 收件人與 4GL 相同

                // 4GL: from_yyyymm[1,6] clipped → 例如 "200409"
                string fromStr = ProdassysSpcArProvider.FromDateInt.ToString();
                string yyyymm = fromStr.Length >= 6 ? fromStr.Substring(0, 6) : fromStr;

                if (errCnt != 0)
                {
                    string subject = yyyymm + " APG spc_system_backup !!ERROR!!";
                    string body = "PRODASSYS SPC 備份完成，但發生 " + errCnt + " 筆錯誤，請檢查 log。";

                    logCallback("備份完成，但發生 " + errCnt + " 筆錯誤，請檢查 log。");
                    LogHelper.WriteLog("PRODASSYS_SPC_AR", subject);

                    // 寄出錯誤通知信
                    MailHelper.SendMail(subject, body, mailTo);
                }
                else
                {
                    string subject = yyyymm + " APG spc_system_backup complete";
                    string body = "PRODASSYS SPC 備份完成，無錯誤。";

                    logCallback("備份完成，無錯誤。");
                    LogHelper.WriteLog("PRODASSYS_SPC_AR", subject);

                    // 寄出成功通知信
                    MailHelper.SendMail(subject, body, mailTo);
                }

                // ===== 以上是 mail 對應 4GL 的 runshell & run runshell =====

                DateTime endTime = DateTime.Now;
                TimeSpan duration = endTime - startTime;

                logCallback("結束時間：" + endTime.ToString("yyyy/MM/dd HH:mm:ss"));
                logCallback(string.Format("歷經時間：{0:00} 小時 {1:00} 分 {2:00} 秒",
                    (int)duration.TotalHours, duration.Minutes, duration.Seconds));
                LogHelper.WriteLog("PRODASSYS_SPC_AR",
                    "結束時間：" + endTime.ToString("yyyy/MM/dd HH:mm:ss"));
                LogHelper.WriteLog("PRODASSYS_SPC_AR",
                    string.Format("歷經時間：{0:00} 小時 {1:00} 分 {2:00} 秒",
                        (int)duration.TotalHours, duration.Minutes, duration.Seconds));
                LogHelper.ExecuteRecord("prodassys_spc_ar.4ge", $"{DateTime.Now.ToString("f", new CultureInfo("zh-TW"))}", $"runDate: {runDate}");
                logCallback("*****************************************");
                logCallback("[V] PRODASSYS SPC 資料備份完成！");
            }
            catch (Exception ex)
            {
                logCallback("========================================");
                logCallback("[X] PRODASSYS_SPC_AR 執行出現錯誤: " + ex.Message);
                logCallback("[X] 堆疊追蹤:");
                logCallback(ex.StackTrace ?? string.Empty);
                logCallback("========================================");

                LogHelper.WriteLog("PRODASSYS_SPC_AR", "[X] 程式執行出現錯誤: " + ex.Message);
                Environment.ExitCode = 1;
            }
        }
    }
}
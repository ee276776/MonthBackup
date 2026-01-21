using System;
using System.Collections.Generic;
using CPISData.Data;
using MonthBackup_FE.Helper;
using MonthBackup_FE.Service; // 如果需要 UserDefineTableService, IFXTransaction

namespace MonthBackup_FE.ciMesArchive.Eqp.Service
{
    /// <summary>
    /// 第 9 個功能：設備歷史歸檔 (對應 4GL MAIN)
    /// 參數:
    /// - v_begin_date (選填): yyyyMM / yyyy-MM / yyyyMMdd / yyyy-MM-dd
    /// - v_end_date   (選填): 同上，若沒帶，預設用 v_begin_date 當月份的月底 / 或今天
    /// </summary>
    public class EquipArchiveExecutor
    {
        public static void Execute(Dictionary<string, string> parameters, Action<string> logCallback)
        {
            try
            {
                // 1. 解析日期參數 (這裡用最簡單版本，你可以之後抽成 Provider)
                string beginRaw = parameters.ContainsKey("v_begin_date")
                    ? parameters["v_begin_date"]
                    : string.Empty;

                string endRaw = parameters.ContainsKey("v_end_date")
                    ? parameters["v_end_date"]
                    : string.Empty;

                // 將輸入轉成 yyyy-MM-dd 字串
                string beginDate = NormalizeDate(beginRaw, DateTime.Today.AddMonths(-1)); // 沒帶就預設上個月今天
                string endDate = NormalizeDate(endRaw, DateTime.Today);                 // 沒帶就預設今天

                logCallback("*****************************************");
                logCallback("  EMS EQUIPMENT ARCHIVE");
                logCallback(string.Empty);
                logCallback("  Begin Date: " + beginDate);
                logCallback("  End   Date: " + endDate);
                logCallback(string.Empty);

                DateTime startTime = DateTime.Now;
                logCallback("開始時間：" + startTime.ToString("yyyy/MM/dd HH:mm:ss"));
                LogHelper.WriteLog("EMS_EQP_ARCHIVE", "開始時間：" + startTime.ToString("yyyy/MM/dd HH:mm:ss"));

                using (UserDefineTableService service = new UserDefineTableService("cimes"))
                using (IFXTransaction tx = service.BeginTransaction())
                {
                    // SET isolation TO dirty read
                    IfxDataAccess.ExecuteNonQuery(tx, "SET ISOLATION TO DIRTY READ");
                    logCallback("已設定 ISOLATION TO DIRTY READ");
                    logCallback(string.Empty);

                    // 建立歸檔器
                    var archiver = new MonthBackup_FE.ciMesArchive.Eqp.Provider.EquipmentArchiver(logCallback);

                    // 跑所有設備 (這裡第 9 功能就是呼叫你剛改好的 ArchiveAllEquipments)
                    List<string> archiveSqls = archiver.ArchiveAllEquipments(
                        tx: tx,
                        beginTime: beginDate,
                        endTime: endDate,
                        GlobalSettings.IsDeleteMode 
                    );

                    //// 匯出檔案
                    //string outputPath = $@"C:\Archive\EMS_EQP_{DateTime.Now:yyyyMMdd_HHmmss}.sql";
                    //archiver.ExportToFile(outputPath, archiveSqls);

                    tx.Commit();
                    logCallback("[V] 交易已成功提交");
                    //logCallback($"歸檔 SQL 共 {archiveSqls.Count} 筆，已輸出至: {outputPath}");
                }

                DateTime endTime = DateTime.Now;
                TimeSpan duration = endTime - startTime;

                logCallback("結束時間：" + endTime.ToString("yyyy/MM/dd HH:mm:ss"));
                logCallback(string.Format("歷經時間：{0:00} 小時 {1:00} 分 {2:00} 秒",
                    (int)duration.TotalHours, duration.Minutes, duration.Seconds));

                LogHelper.WriteLog("EMS_EQP_ARCHIVE",
                    "結束時間：" + endTime.ToString("yyyy/MM/dd HH:mm:ss"));
                LogHelper.WriteLog("EMS_EQP_ARCHIVE",
                    string.Format("歷經時間：{0:00} 小時 {1:00} 分 {2:00} 秒",
                        (int)duration.TotalHours, duration.Minutes, duration.Seconds));

                logCallback("*****************************************");
                logCallback("[V] EMS 設備歷史歸檔完成！");
            }
            catch (Exception ex)
            {
                logCallback("========================================");
                logCallback("[X] EMS_EQP_ARCHIVE 執行出現錯誤: " + ex.Message);
                logCallback("[X] 堆疊追蹤:");
                logCallback(ex.StackTrace ?? string.Empty);
                logCallback("========================================");

                LogHelper.WriteLog("EMS_EQP_ARCHIVE", "[X] 程式執行出現錯誤: " + ex.Message);
                Environment.ExitCode = 1;
            }
        }

        /// <summary>
        /// 把 yyyyMM / yyyy-MM / yyyyMMdd / yyyy-MM-dd 轉成 yyyy-MM-dd
        /// 若空字串，回傳 defaultDate.ToString("yyyy-MM-dd")
        /// </summary>
        private static string NormalizeDate(string raw, DateTime defaultDate)
        {
            if (string.IsNullOrWhiteSpace(raw))
            {
                return defaultDate.ToString("yyyy-MM-dd");
            }

            string trimmed = raw.Trim();

            // 已經是 yyyy-MM-dd
            if (DateTime.TryParse(trimmed, out var dtParsed))
            {
                return dtParsed.ToString("yyyy-MM-dd");
            }

            // yyyyMMdd
            if (trimmed.Length == 8 && int.TryParse(trimmed, out _))
            {
                if (DateTime.TryParseExact(trimmed, "yyyyMMdd",
                    System.Globalization.CultureInfo.InvariantCulture,
                    System.Globalization.DateTimeStyles.None,
                    out dtParsed))
                {
                    return dtParsed.ToString("yyyy-MM-dd");
                }
            }

            // yyyyMM
            if (trimmed.Length == 6 && int.TryParse(trimmed, out _))
            {
                if (DateTime.TryParseExact(trimmed, "yyyyMM",
                    System.Globalization.CultureInfo.InvariantCulture,
                    System.Globalization.DateTimeStyles.None,
                    out dtParsed))
                {
                    // 用該月 1 號
                    dtParsed = new DateTime(dtParsed.Year, dtParsed.Month, 1);
                    return dtParsed.ToString("yyyy-MM-dd");
                }
            }

            // yyyy-MM (假設是月)
            if (trimmed.Length == 7 && trimmed[4] == '-')
            {
                if (DateTime.TryParseExact(trimmed, "yyyy-MM",
                    System.Globalization.CultureInfo.InvariantCulture,
                    System.Globalization.DateTimeStyles.None,
                    out dtParsed))
                {
                    dtParsed = new DateTime(dtParsed.Year, dtParsed.Month, 1);
                    return dtParsed.ToString("yyyy-MM-dd");
                }
            }

            // 其他格式就用 defaultDate
            return defaultDate.ToString("yyyy-MM-dd");
        }
    }
}
using System;
using System.Collections.Generic;
using CPISData.Data;
using MonthBackup_FE.Helper;
using MonthBackup_FE.Service;

namespace MonthBackup_FE.ciMesArchive.Lot.Service
{
    /// <summary>
    /// Lot 歷史歸檔執行器
    /// 參數:
    /// - v_begin_date (必填): yyyyMM / yyyy-MM / yyyyMMdd / yyyy-MM-dd
    /// - v_end_date   (選填): 同上，若沒帶則預設為當日
    /// </summary>
    public class LotArchiveExecutor
    {
        public static void Execute(Dictionary<string, string> parameters, Action<string> logCallback)
        {
            try
            {
                // 1. 解析日期參數
                string beginRaw = parameters.ContainsKey("v_begin_date")
                    ? parameters["v_begin_date"]
                    : string.Empty;

                string endRaw = parameters.ContainsKey("v_end_date")
                    ? parameters["v_end_date"]
                    : string.Empty;

                if (string.IsNullOrWhiteSpace(beginRaw))
                {
                    throw new ArgumentException("開始日期 (v_begin_date) 為必填參數");
                }

                // 將輸入轉成 yyyy-MM-dd 字串
                string beginDate = NormalizeDate(beginRaw, DateTime.Today.AddMonths(-1));
                string endDate = NormalizeDate(endRaw, DateTime.Today);

                logCallback("*****************************************");
                logCallback("  LOT ARCHIVE");
                logCallback(string.Empty);
                logCallback("  Begin Date: " + beginDate);
                logCallback("  End   Date: " + endDate);
                logCallback(string.Empty);

                DateTime startTime = DateTime.Now;
                logCallback("開始時間：" + startTime.ToString("yyyy/MM/dd HH:mm:ss"));
                LogHelper.WriteLog("LOT_ARCHIVE", "開始時間：" + startTime.ToString("yyyy/MM/dd HH:mm:ss"));

                using (UserDefineTableService service = new UserDefineTableService("cimes"))
                using (IFXTransaction tx = service.BeginTransaction())
                {
                    // 建立歸檔器
                    var archiver = new MonthBackup_FE.ciMesArchive.Lot.Provider.LotArchiver(logCallback);

                    // 執行批次歸檔
                    List<string> archiveSqls = archiver.ArchiveAllLots(
                        tx: tx,
                        beginTime: beginDate,
                        endTime: endDate,
                        deleteFromDB: GlobalSettings.IsDeleteMode
                    );

                    tx.Commit();
                    logCallback("[V] 交易已成功提交");
                    logCallback($"歸檔 SQL 共 {archiveSqls.Count} 筆");
                }

                DateTime endTime = DateTime.Now;
                TimeSpan duration = endTime - startTime;

                logCallback("結束時間：" + endTime.ToString("yyyy/MM/dd HH:mm:ss"));
                logCallback(string.Format("歷經時間：{0:00} 小時 {1:00} 分 {2:00} 秒",
                    (int)duration.TotalHours, duration.Minutes, duration.Seconds));

                LogHelper.WriteLog("LOT_ARCHIVE",
                    "結束時間：" + endTime.ToString("yyyy/MM/dd HH:mm:ss"));
                LogHelper.WriteLog("LOT_ARCHIVE",
                    string.Format("歷經時間：{0:00} 小時 {1:00} 分 {2:00} 秒",
                        (int)duration.TotalHours, duration.Minutes, duration.Seconds));

                logCallback("*****************************************");
                logCallback("[V] Lot 歷史歸檔完成！");
            }
            catch (Exception ex)
            {
                logCallback("========================================");
                logCallback("[X] LOT_ARCHIVE 執行出現錯誤: " + ex.Message);
                logCallback("[X] 堆疊追蹤:");
                logCallback(ex.StackTrace ?? string.Empty);
                logCallback("========================================");

                LogHelper.WriteLog("LOT_ARCHIVE", "[X] 程式執行出現錯誤: " + ex.Message);
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

            // 已經是 yyyy-MM-dd 或 yyyy/MM/dd
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
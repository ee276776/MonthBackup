using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MonthBackup_FE.Helper
{
    public static class LogHelper
    {
        private static readonly string LogFilePath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "log", "ExecuteRecord.log");

        /// <summary>
        /// 記錄執行資訊：[FunctionName]-[FinishTime]-[Parameter]
        /// </summary>
        public static void ExecuteRecord(string functionName, string finishTime, string parameter)
        {
            try
            {
                string logDirectory = Path.GetDirectoryName(LogFilePath);
                if (!Directory.Exists(logDirectory))
                {
                    Directory.CreateDirectory(logDirectory);
                }

                // 依照格式組合訊息
                string logMessage = $"[{functionName}]-[{finishTime}]-[{parameter}]";

                // 使用 AppendAllLines 持續新增內容
                File.AppendAllLines(LogFilePath, new[] { logMessage }, Encoding.UTF8);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"紀錄寫入失敗: {ex.Message}");
            }
        }

        /// <summary>
        /// 解析器：讀取檔案並抓取特定 FunctionName 的最後一筆 Parameter
        /// </summary>
        public static string ExecuteRecordParser(string functionName)
        {
            try
            {
                if (!File.Exists(LogFilePath)) return null;

                // 從後往前搜尋，提高效能
                string targetHeader = $"[{functionName}]-";

                // 讀取所有行並反轉，找到第一筆符合 FunctionName 的資料
                string lastRecord = File.ReadLines(LogFilePath)
                                        .Reverse()
                                        .FirstOrDefault(line => line.StartsWith(targetHeader));

                if (lastRecord != null)
                {
                    // 格式為 [Func]-[Time]-[Param]，依據 '-' 分割並取出最後一部分
                    // 若 Parameter 本身含有 '-'，建議改用 LastIndexOf 取出最後一個括號後的內容
                    int lastDashIndex = lastRecord.LastIndexOf("]-[");
                    if (lastDashIndex != -1)
                    {
                        string paramPart = lastRecord.Substring(lastDashIndex + 3);
                        return paramPart.TrimEnd(']'); // 移除結尾的中括號
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"解析紀錄失敗: {ex.Message}");
            }

            return null;
        }


        /// <summary>
        /// 記錄 Log 資訊
        /// </summary>
        /// <param name="unit">單元名稱 (例如：Method 名稱或功能模組)</param>
        /// <param name="content">記錄內容</param>
        public static void WriteLog(string unit, string content)
        {
            try
            {
                // 1. 設定目錄路徑與檔名 (2025 年格式)
                string logDirectory = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "log");
                string fileName = $"{DateTime.Now:yyyy-MM-dd}_log.txt";
                string filePath = Path.Combine(logDirectory, fileName);

                // 2. 檢查目錄是否存在，若不存在則建立
                if (!Directory.Exists(logDirectory))
                {
                    Directory.CreateDirectory(logDirectory);
                }

                // 3. 組合 Log 訊息格式
                string logMessage = $"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] [{unit}] {content}";

                // 4. 寫入檔案 (使用 Append 模式，並確保編碼為 UTF8)
                // File.AppendAllLines 會自動處理檔案開啟與關閉，並在每行末尾加上換行符
                File.AppendAllLines(filePath, new[] { logMessage }, Encoding.UTF8);
            }
            catch (Exception ex)
            {
                // 若 Log 寫入失敗，通常會輸出至 Console 以利偵錯
                Console.WriteLine($"Log 寫入失敗: {ex.Message}");
            }
        }
    }
}

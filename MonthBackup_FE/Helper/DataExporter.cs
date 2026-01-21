using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MonthBackup_FE.Helper
{
    public static class DataExporter
    {
        ///// <summary>
        ///// 將資料表的查詢結果存入指定的 .995 檔案
        ///// </summary>
        ///// <param name="queryResult">查詢得到的資料 (DataTable)</param>
        ///// <param name="targetFileName">指定的目標檔案 (如 as_pds.995)</param>
        //public static void ExportData(DataTable queryResult, string targetFileName,string targetFolderName, Action<string> logCallback)
        //{
        //    string tempFileName = "tmp1";

        //    try
        //    {
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
        //        using (StreamWriter targetWriter = new StreamWriter(targetFileName, append: true))
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

        //        logCallback($"成功匯出資料到 {targetFileName}");
        //    }
        //    catch (Exception ex)
        //    {
        //        logCallback($"匯出資料到 {targetFileName} 時發生錯誤: {ex.Message}");
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
        public static void ExportData(DataTable queryResult, string targetFileName, string targetFolderName, Action<string> logCallback)
        {
            // 1. 定義資料夾名稱與路徑
            string folderName = $"{targetFolderName}";
            // 取得程式執行目錄下的 BackupOutput 資料夾完整路徑
            string folderPath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, folderName);

            // 2. 組合完整的檔案路徑
            string targetPath = Path.Combine(folderPath, targetFileName);
            string tempFileName = Path.Combine(folderPath, "tmp1");

            try
            {
                // 3. 檢查資料夾是否存在，不存在則建立
                if (!Directory.Exists(folderPath))
                {
                    Directory.CreateDirectory(folderPath);
                }

                // 將 DataTable 寫入臨時檔案
                using (StreamWriter tempWriter = new StreamWriter(tempFileName))
                {
                    if (queryResult != null)
                    {
                        foreach (DataRow row in queryResult.Rows)
                        {
                            string line = string.Join("|", row.ItemArray.Select(item => item?.ToString().Trim() ?? ""));
                            tempWriter.WriteLine(line);
                        }
                    }
                }

                // 將臨時檔案追加到目標檔案
                using (StreamWriter targetWriter = new StreamWriter(targetPath, append: false))
                {
                    using (StreamReader tempReader = new StreamReader(tempFileName))
                    {
                        string line;
                        while ((line = tempReader.ReadLine()) != null)
                        {
                            targetWriter.WriteLine(line);
                        }
                    }
                }

                logCallback($"成功匯出資料到 {targetPath}");
            }
            catch (Exception ex)
            {
                logCallback($"匯出資料時發生錯誤: {ex.Message}");
            }
            finally
            {
                // 刪除臨時檔案
                if (File.Exists(tempFileName))
                {
                    File.Delete(tempFileName);
                }
            }
        }

        public static void ExportData_Append(DataTable queryResult, string targetFileName, string targetFolderName, Action<string> logCallback)
        {
            // 1. 定義資料夾名稱與路徑
            string folderName = $"{targetFolderName}";
            // 取得程式執行目錄下的 BackupOutput 資料夾完整路徑
            string folderPath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, folderName);

            // 2. 組合完整的檔案路徑
            string targetPath = Path.Combine(folderPath, targetFileName);
            string tempFileName = Path.Combine(folderPath, "tmp1");

            try
            {
                // 3. 檢查資料夾是否存在，不存在則建立
                if (!Directory.Exists(folderPath))
                {
                    Directory.CreateDirectory(folderPath);
                }

                // 將 DataTable 寫入臨時檔案
                using (StreamWriter tempWriter = new StreamWriter(tempFileName))
                {
                    if (queryResult != null)
                    {
                        foreach (DataRow row in queryResult.Rows)
                        {
                            string line = string.Join("|", row.ItemArray.Select(item => item?.ToString().Trim() ?? ""));
                            tempWriter.WriteLine(line);
                        }
                    }
                }

                // 將臨時檔案追加到目標檔案
                using (StreamWriter targetWriter = new StreamWriter(targetPath, append: true))
                {
                    using (StreamReader tempReader = new StreamReader(tempFileName))
                    {
                        string line;
                        while ((line = tempReader.ReadLine()) != null)
                        {
                            targetWriter.WriteLine(line);
                        }
                    }
                }

                logCallback($"成功匯出資料到 {targetPath}");
            }
            catch (Exception ex)
            {
                logCallback($"匯出資料時發生錯誤: {ex.Message}");
            }
            finally
            {
                // 刪除臨時檔案
                if (File.Exists(tempFileName))
                {
                    File.Delete(tempFileName);
                }
            }
        }
    }
}

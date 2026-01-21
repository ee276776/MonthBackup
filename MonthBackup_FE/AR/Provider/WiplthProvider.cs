using CPISData.Data;
using System;
using System.Data;
using System.IO;
using System.Linq;

namespace MonthBackup_FE.AR.Provider
{
    public static class WiplthProvider
    {
        ///// <summary>
        ///// 匯出 wiplth 資料至 wiplth1.995 和 wiplth2.995
        ///// </summary>
        ///// <param name="tx">資料庫交易物件 (IFXTransaction)</param>
        //public static void ExportWiplthData(IFXTransaction tx)
        //{
        //    try
        //    {
        //        Console.WriteLine("開始匯出 wiplth 資料...");

        //        // 啟用旗標，用於決定輸出至 wiplth1.995 或 wiplth2.995
        //        int tmpFlag = 0;

        //        // 查詢 cchu 的 lot_no
        //        string cursorQuery = "SELECT wlot_lot_number FROM cchu";
        //        var cursorResults = IfxDataAccess.ExecuteDataTable(tx, cursorQuery);

        //        // 檢查是否有 lot_no 結果
        //        if (cursorResults.Rows.Count == 0)
        //        {
        //            Console.WriteLine("cchu 表中無數據，無需匯出 wiplth 資料。");
        //            return;
        //        }

        //        // 1. 定義臨時檔案和目標檔案的名稱
        //        string tempFile = "tmp1";
        //        string outputFile1 = "wiplth1.995";
        //        string outputFile2 = "wiplth2.995";

        //        // 2. 初始化目標檔案 (清空舊內容)
        //        if (File.Exists(outputFile1)) File.Delete(outputFile1);
        //        if (File.Exists(outputFile2)) File.Delete(outputFile2);

        //        // 3. 遍歷每個 lot_no 的查詢處理
        //        foreach (DataRow row in cursorResults.Rows)
        //        {
        //            string lotNo = row["wlot_lot_number"].ToString();

        //            // 更新 ees:s1975，APG沒有s1975 先註解掉
        //            //string updateQuery = string.Format(@"
        //            //    UPDATE ees:s1975
        //            //    SET assy_lot = '{0}'", lotNo);
        //            //IfxDataAccess.ExecuteNonQuery(tx, updateQuery);

        //            // 查詢 wiplth 數據
        //            string selectQuery = string.Format(@"
        //                SELECT *
        //                FROM wiplth
        //                WHERE wlth_lot_number = '{0}'", lotNo);
        //            var wiplthResults = IfxDataAccess.ExecuteDataTable(tx, selectQuery);

        //            if (wiplthResults == null || wiplthResults.Rows.Count == 0)
        //            {
        //                Console.WriteLine($"== 錯誤 == wiplth 無法找到 wlth_lot_number = {lotNo}");
        //                continue; // 跳過當前的 lot_no
        //            }

        //            // 將 wiplth 的查詢結果寫入臨時檔案 tmp1
        //            using (StreamWriter tempWriter = new StreamWriter(tempFile, false)) // 覆寫 tmp1
        //            {
        //                foreach (DataRow wiplthRow in wiplthResults.Rows)
        //                {
        //                    string line = string.Join("|", wiplthRow.ItemArray.Select(item => item?.ToString().Trim() ?? ""));
        //                    tempWriter.WriteLine(line);
        //                }

        //            }

        //            // 將臨時檔案的內容追加到目標檔案 (根據 tmpFlag 決定是 wiplth1.995 或 wiplth2.995)
        //            string targetFile = tmpFlag == 0 ? outputFile1 : outputFile2;

        //            using (StreamWriter targetWriter = new StreamWriter(targetFile, true)) // 追加模式
        //            {
        //                using (StreamReader tempReader = new StreamReader(tempFile))
        //                {
        //                    string line;
        //                    while ((line = tempReader.ReadLine()) != null)
        //                    {
        //                        targetWriter.WriteLine(line);
        //                    }
        //                }
        //            }

        //            // 切換 tmpFlag
        //            tmpFlag = tmpFlag == 0 ? 1 : 0;
        //        }

        //        // 4. 刪除臨時檔案
        //        if (File.Exists(tempFile)) File.Delete(tempFile);

        //        Console.WriteLine("wiplth 資料匯出完成！");
        //    }
        //    catch (Exception ex)
        //    {
        //        Console.WriteLine($"匯出 wiplth 資料時發生錯誤: {ex.Message}");
        //    }
        //}
        public static void ExportWiplthData_old(IFXTransaction tx)
        {
            try
            {
                Console.WriteLine("開始匯出 wiplth 資料 (子查詢優化版)...");

                // 1. 使用子查詢一次性抓取所有符合 cchu 條件的 wiplth 資料
                // 使用 JOIN 通常比 IN 子查詢在大型資料庫中效能更好
                
                string selectQuery = @"
            SELECT w.* 
            FROM wiplth w
            INNER JOIN cchu c ON w.wlth_lot_number = c.wlot_lot_number";

                var allResults = IfxDataAccess.ExecuteDataTable(tx, selectQuery);

                if (allResults == null || allResults.Rows.Count == 0)
                {
                    Console.WriteLine("查無相關資料，無需匯出。");
                    return;
                }

                // 2. 初始化目標檔案 (清空舊內容)
                string outputFile1 = "wiplth1.995";
                string outputFile2 = "wiplth2.995";
                if (File.Exists(outputFile1)) File.Delete(outputFile1);
                if (File.Exists(outputFile2)) File.Delete(outputFile2);

                // 3. 在記憶體中根據 LotNumber 分組，模擬原本的 tmpFlag 交替邏輯
                var groupedData = allResults.AsEnumerable()
                    .GroupBy(row => row.Field<string>("wlth_lot_number"))
                    .ToList();

                using (var sw1 = new StreamWriter(outputFile1, false))
                using (var sw2 = new StreamWriter(outputFile2, false))
                {
                    for (int i = 0; i < groupedData.Count; i++)
                    {
                        // 根據索引奇偶數決定寫入哪個檔案 (0, 2, 4... -> File1; 1, 3, 5... -> File2)
                        var targetWriter = (i % 2 == 0) ? sw1 : sw2;

                        foreach (var row in groupedData[i])
                        {
                            string line = string.Join("|", row.ItemArray.Select(item => item?.ToString().Trim() ?? ""));
                            targetWriter.WriteLine(line);
                        }
                    }
                }

                Console.WriteLine($"匯出完成！共處理 {groupedData.Count} 組 Lot 資料。");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"匯出資料時發生錯誤: {ex.Message}");
            }
        }


        public static void ExportWiplthData(IFXTransaction tx)
        {
            try
            {
                // 1. 設定基礎目錄與月份資料夾
                string baseDir = AppDomain.CurrentDomain.BaseDirectory;
                string folderName = $"AR_{DateTime.Now.ToString("yyyyMM")}";
                string targetFolderPath = Path.Combine(baseDir, folderName);

                // 2. 自動建立資料夾 (若已存在則會自動跳過，不會報錯)
                if (!Directory.Exists(targetFolderPath))
                {
                    Directory.CreateDirectory(targetFolderPath);
                    Console.WriteLine($"建立資料夾: {targetFolderPath}");
                }

                Console.WriteLine("開始匯出 wiplth 資料...");

                string selectQuery = @"
            SELECT w.* 
            FROM wiplth w
            INNER JOIN cchu c ON w.wlth_lot_number = c.wlot_lot_number";

                var allResults = IfxDataAccess.ExecuteDataTable(tx, selectQuery);

                if (allResults == null || allResults.Rows.Count == 0)
                {
                    Console.WriteLine("查無相關資料。");
                    return;
                }

                // 3. 使用 Path.Combine 結合資料夾路徑與檔名
                string outputFile1 = Path.Combine(targetFolderPath, "wiplth1.995");
                string outputFile2 = Path.Combine(targetFolderPath, "wiplth2.995");

                // 4. 分組邏輯
                var groupedData = allResults.AsEnumerable()
                    .GroupBy(row => row["wlth_lot_number"]?.ToString().Trim() ?? "Unknown")
                    .ToList();

                // 5. 寫入檔案
                using (var sw1 = new StreamWriter(outputFile1, false, System.Text.Encoding.UTF8))
                using (var sw2 = new StreamWriter(outputFile2, false, System.Text.Encoding.UTF8))
                {
                    for (int i = 0; i < groupedData.Count; i++)
                    {
                        var targetWriter = (i % 2 == 0) ? sw1 : sw2;
                        foreach (var row in groupedData[i])
                        {
                            string line = string.Join("|", row.ItemArray.Select(item => item?.ToString().Trim() ?? ""));
                            targetWriter.WriteLine(line);
                        }
                    }
                }

                Console.WriteLine($"匯出完成！檔案已存放在: {targetFolderPath}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"發生錯誤: {ex.Message}");
            }
        }
    }
}
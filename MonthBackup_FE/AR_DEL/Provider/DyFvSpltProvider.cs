using CPISData.Data;
using MonthBackup_FE.Helper;
using System;
using System.Data;

namespace MonthBackup_FE.AR_DEL.Provider
{
    public static class DyFvSpltProvider
    {
        ///// <summary>
        ///// 刪除 dy_fv_splt 資料
        ///// 根據 customer 和 lot_no 長度有不同的刪除邏輯
        ///// </summary>
        //public static void DeleteDyFvSplt(IFXTransaction tx)
        //{
        //    try
        //    {
        //        Console.WriteLine("delete dy_fv_splt .........");

        //        // 先查詢所有 cchu 中的 lot_no
        //        string cchuQuery = "SELECT wlot_lot_number FROM cchu";
        //        DataTable cchuResults = IfxDataAccess.ExecuteDataTable(tx, cchuQuery);

        //        int processedCount = 0;

        //        foreach (DataRow row in cchuResults.Rows)
        //        {
        //            string lotNo = row["wlot_lot_number"].ToString().Trim();

        //            // 從 wiplot 查詢 customer (wlot_crt_dat_al_1)
        //            string customerQuery = string.Format(@"
        //                SELECT wlot_crt_dat_al_1
        //                FROM wiplot
        //                WHERE wlot_lot_number = '{0}'
        //            ", lotNo);

        //            DataTable customerResult = IfxDataAccess.ExecuteDataTable(tx, customerQuery);

        //            string customer = "";
        //            if (customerResult != null && customerResult.Rows.Count > 0 && customerResult.Rows[0][0] != DBNull.Value)
        //            {
        //                customer = customerResult.Rows[0][0].ToString().Trim();
        //            }

        //            // 根據 customer 判斷刪除邏輯
        //            if (customer != "ProMOS")
        //            {
        //                // 非 ProMOS: 刪除 ori_assy_lot = lot_no AND splt_assy_lot = ''
        //                string deleteSQL = string.Format(@"
        //                    DELETE FROM dy_fv_splt
        //                    WHERE ori_assy_lot = '{0}'
        //                      AND splt_assy_lot = ''
        //                ", lotNo);

        //                IfxDataAccess.ExecuteNonQuery(tx, deleteSQL);
        //            }
        //            else
        //            {
        //                // ProMOS: 如果 lot_no 長度為 10,取前 9 碼
        //                if (lotNo.Length == 10)
        //                {
        //                    string lotNo9 = lotNo.Substring(0, 9);

        //                    string deleteSQL = string.Format(@"
        //                        DELETE FROM dy_fv_splt
        //                        WHERE ori_assy_lot = '{0}'
        //                          AND splt_assy_lot = '{1}'
        //                    ", lotNo9, lotNo);

        //                    IfxDataAccess.ExecuteNonQuery(tx, deleteSQL);
        //                }
        //            }

        //            processedCount++;
        //        }

        //        Console.WriteLine($"dy_fv_splt 刪除完成，共處理 {processedCount} 筆");
        //    }
        //    catch (Exception ex)
        //    {
        //        Console.WriteLine($"== error == 刪除 dy_fv_splt 時發生錯誤: {ex.Message}");
        //        throw;
        //    }
        //}
        public static void DeleteDyFvSplt(IFXTransaction tx,Action<string>logCallback, bool delMode = false)
        {
            string tableName = "dy_fv_splt";
            string targetFileName = $"{tableName}.995";

            try
            {
                //Console.WriteLine($"處理 {tableName} ( 刪除: {tableName}) .........");
                logCallback($"unload dy_fv_splt..... (Target: dy_fv_splt.995)");
                // 使用 JOIN 與 CASE 邏輯一次性篩選出所有符合條件的資料
                // 條件 A: 非 ProMOS 且 splt_assy_lot 為空
                // 條件 B: 是 ProMOS 且長度為 10 且符合前 9 碼關聯規則
                string baseCondition = @"
            EXISTS (
                SELECT 1 FROM cchu c
                JOIN wiplot w ON c.wlot_lot_number = w.wlot_lot_number
                WHERE 
                    (
                        w.wlot_crt_dat_al_1 <> 'ProMOS' 
                        AND dy_fv_splt.ori_assy_lot = c.wlot_lot_number 
                        AND dy_fv_splt.splt_assy_lot = ''
                    )
                    OR 
                    (
                        w.wlot_crt_dat_al_1 = 'ProMOS' 
                        AND LENGTH(c.wlot_lot_number) = 10 
                        AND dy_fv_splt.ori_assy_lot = c.wlot_lot_number[1,9] 
                        AND dy_fv_splt.splt_assy_lot = c.wlot_lot_number
                    )
            )";

                if (delMode)
                {
                    // 模式：直接刪除
                    string deleteSQL = $"DELETE FROM {tableName} WHERE {baseCondition}";
                    int affectedRows = IfxDataAccess.ExecuteNonQuery(tx, deleteSQL);
                    logCallback($"{tableName} 刪除完成，影響筆數: {affectedRows}");
                }
                else
                {
                    // 模式：匯出
                    string selectSQL = $"SELECT * FROM {tableName} WHERE {baseCondition}";
                    DataTable queryResult = IfxDataAccess.ExecuteDataTable(tx, selectSQL);

                    if (queryResult != null && queryResult.Rows.Count > 0)
                    {
                        DataExporter.ExportData(queryResult, targetFileName, $"AR_Del_{ DateTime.Now.ToString("yyyyMM")}",logCallback);
                        logCallback($"{tableName} 資料已匯出至 {targetFileName}，共 {queryResult.Rows.Count} 筆");
                    }
                    else
                    {
                        logCallback($"{tableName} 無資料需匯出。");
                    }
                }
            }
            catch (Exception ex)
            {
                logCallback($"== error == 處理 {tableName} 時發生錯誤: {ex.Message}");
                throw;
            }
        }
    }
}
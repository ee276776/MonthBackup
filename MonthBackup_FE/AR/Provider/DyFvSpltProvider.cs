using CPISData.Data;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MonthBackup_FE.AR.Provider
{
    public static class DyFvSpltProvider
    {
        /// <summary>
        /// 使用子查詢直接查詢所有符合條件的 dy_fv_splt 數據（不分批處理）
        /// </summary>
        /// <param name="tx">資料庫交易物件</param>
        /// <returns>符合條件的 dy_fv_splt 數據</returns>
        public static DataTable GetDyFvSpltDataBySubquery(IFXTransaction tx)
        {
            try
            {
                Console.WriteLine("開始查詢 dy_fv_splt 數據（使用子查詢）...");

                // 方案1: 非 ProMOS 客戶的數據
                string queryNonProMOS = @"
                    SELECT d.*
                    FROM dy_fv_splt d
                    WHERE d.ori_assy_lot IN (
                        SELECT c.wlot_lot_number
                        FROM cchu c
                        INNER JOIN wiplot w ON w.wlot_lot_number = c.wlot_lot_number
                        WHERE w.wlot_crt_dat_al_1 != 'ProMOS'
                    )
                    AND (d.splt_assy_lot = '' OR d.splt_assy_lot IS NULL)
                ";

                // 方案2: ProMOS 客戶的數據 (lot_no 長度為 10)
                string queryProMOS = @"
                    SELECT d.*
                    FROM dy_fv_splt d
                    WHERE EXISTS (
                        SELECT 1
                        FROM cchu c
                        INNER JOIN wiplot w ON w.wlot_lot_number = c.wlot_lot_number
                        WHERE w.wlot_crt_dat_al_1 = 'ProMOS'
                          AND LENGTH(c.wlot_lot_number) = 10
                          AND d.ori_assy_lot = SUBSTRING(c.wlot_lot_number FROM 1 FOR 9)
                          AND d.splt_assy_lot = c.wlot_lot_number
                    )
                ";

                // 合併兩個查詢
                string combinedQuery = queryNonProMOS + " UNION ALL " + queryProMOS;

                var dyFvSpltData = IfxDataAccess.ExecuteDataTable(tx, combinedQuery);

                Console.WriteLine($"dy_fv_splt 數據查詢完成，共 {(dyFvSpltData != null ? dyFvSpltData.Rows.Count : 0)} 筆");
                return dyFvSpltData;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"查詢 dy_fv_splt 數據時發生錯誤: {ex.Message}");
                return null;
            }
        }
    }
}

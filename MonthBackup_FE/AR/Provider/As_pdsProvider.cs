using CPISData.Data;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MonthBackup_FE.AR.Provider
{
    public static class As_pdsProvider
    {

        /// <summary>
        /// 直接通過子查詢查詢 as_pds 數據
        /// </summary>
        /// <param name="tx">資料庫交易物件 (IFXTransaction)</param>
        /// <returns>符合條件的 as_pds 數據 (DataTable)</returns>
        public static DataTable GetAsPdsData(IFXTransaction tx)
        {
            try
            {
                Console.WriteLine("開始查詢 as_pds 數據（使用子查詢）...");

                // 使用子查詢，直接從 as_pds 查詢所有符合 cchu 條件的數據
                //string query = @"
                //    SELECT *
                //    FROM as_pds
                //    WHERE apds_assylot IN (
                //        SELECT wlot_lot_number
                //        FROM cchu
                //    )
                //";
                string query = @"
     SELECT P.*
     FROM as_pds AS P
     INNER JOIN cchu AS C ON P.apds_assylot = C.wlot_lot_number
 ";

                // 執行查詢並返回 DataTabl
                var asPdsData = IfxDataAccess.ExecuteDataTable(tx, query);

                Console.WriteLine("as_pds 數據查詢完成（使用子查詢）。");
                return asPdsData;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"查詢 as_pds 數據時發生錯誤（使用子查詢）: {ex.Message}");
                return null;
            }
        }
    }
}

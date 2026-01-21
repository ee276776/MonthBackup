using CPISData.Data;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MonthBackup_FE.AR.Provider
{
    public static class AbnMvouHistProvider
    {
        /// <summary>
        /// 直接通過子查詢查詢 abn_mvou_hist 數據
        /// </summary>
        /// <param name="tx">資料庫交易物件 (IFXTransaction)</param>
        /// <returns>符合條件的 abn_mvou_hist 數據 (DataTable)</returns>
        public static DataTable GetAbnMvouHistData(IFXTransaction tx)
        {
            try
            {
                Console.WriteLine("unload abn_mvou_hist .....");

                // 對應 4GL 的邏輯：
                // 查詢 abn_mvou_hist 中，其 abnm_lot_number 存在於 cchu.wlot_lot_number 的所有資料
                //    string query = @"
                //    SELECT *
                //    FROM abn_mvou_hist
                //    WHERE abnm_lot_number IN (
                //        SELECT wlot_lot_number
                //        FROM cchu
                //    )
                //";
                string query = @"
     SELECT H.*
     FROM abn_mvou_hist AS H
     INNER JOIN cchu AS C ON H.abnm_lot_number = C.wlot_lot_number
 ";

                // 執行查詢並返回 DataTable (對應 4GL 的 unload 動作)
                var abnMvouData = IfxDataAccess.ExecuteDataTable(tx, query);

                Console.WriteLine("abn_mvou_hist 數據查詢完成。");
                return abnMvouData;
            }
            catch (Exception ex)
            {
                // 對應 4GL 的 display "== error == "
                Console.WriteLine($"查詢 abn_mvou_hist 數據時發生錯誤: {ex.Message}");
                return null;
            }
        }
    }
}

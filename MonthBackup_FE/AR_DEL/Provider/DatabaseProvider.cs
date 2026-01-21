using CPISData.Data;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MonthBackup_FE.AR_DEL.Provider
{
    public static class DatabaseProvider
    {
        public static string connectionString = ConnectionStrings.FromName("comets");
        public static int cchuCount = 0;

        public static void ExecuteDelLoadLogic(IFXTransaction tx, string endDate, Action<string> logCallback)
        {
            try
            {
                logCallback("開始 del_load 處理...");
                logCallback($"END_DATE : {endDate}");

                // 1. 創建臨時表 cchu
                CreateTempCchuTable(tx, logCallback);

                // 2. 執行 A2 部分邏輯
                InsertCchuFromWiplotA2(tx, endDate, logCallback);

                // 3. 執行 A5 部分邏輯
                InsertCchuFromWiplotA5(tx, endDate, logCallback);

                logCallback("del_load 處理完成。");
            }
            catch (Exception ex)
            {
                logCallback($"執行 del_load 時發生錯誤: {ex.Message}");
                throw;
            }
        }

        private static void CreateTempCchuTable(IFXTransaction tx, Action<string> logCallback)
        {
            // 1. 先嘗試刪除已存在的臨時表 (IF EXISTS 確保表不存在時不會報錯)
            // 2. 接著建立新的臨時表
            string sql = @"
        DROP TABLE IF EXISTS cchu;
        CREATE TEMP TABLE cchu (
            wlot_lot_number CHAR(11)
        ) WITH NO LOG;
    ";

            try
            {
                IfxDataAccess.ExecuteNonQuery(tx, sql);
                logCallback("臨時表 cchu 已重置並重新建立...");
                logCallback?.Invoke("臨時表 cchu 已建立。");
            }
            catch (Exception ex)
            {
                // 如果您的 Informix 版本較舊不支援 IF EXISTS，可以分開執行並捕捉錯誤
                logCallback($"建立臨時表時發生錯誤: {ex.Message}");
                throw;
            }
        }

        public static void InsertCchuFromWiplotA2(IFXTransaction tx, string endDate, Action<string> logCallback)
        {
            try
            {
                logCallback("開始處理 A2 條件數據...");

                string query = string.Format(@"
                    SELECT wlot_lot_number, wlot_crt_dat_al_1, wlot_dt_last_trans
                    FROM wiplot
                    WHERE wlot_qty_1 = 0
                      AND wlot_qty_2 = 0
                      AND wlot_crte_category = 'A2'
                      AND wlot_dt_last_trans <= '{0}'
                      AND wlot_lot_number NOT IN (SELECT assy_lot FROM ar_exclude)
                ", endDate);

                var results = IfxDataAccess.ExecuteDataTable(tx, query);
                logCallback($"預計處理 {results.Rows.Count} 筆");

                foreach (DataRow row in results.Rows)
                {
                    string lotNo = row["wlot_lot_number"].ToString().Trim();
                    string customer = row["wlot_crt_dat_al_1"].ToString().Trim();
                    string qryAssyLot = lotNo.Length >= 7 ? lotNo.Substring(0, 7) : lotNo;

                    // 檢查 ship_hist 是否有 post <> 'Y' 的記錄
                    int cnt = CheckShipHistPostNotY(tx, qryAssyLot);
                    if (cnt > 0)
                    {
                        logCallback($"ship_hist(post<>Y): {lotNo} cnt: {cnt}");
                        continue;
                    }

                    logCallback($"lot_no: {lotNo}, customer: {customer}");

                    // 直接插入 cchu (4GL 原本的邏輯)
                    InsertCchu(tx, lotNo, logCallback);
                }

                logCallback("A2 條件數據處理完成...");
            }
            catch (Exception ex)
            {
                logCallback($"處理 A2 條件數據發生錯誤: {ex.Message}");
                throw;
            }
        }

        private static int CheckShipHistPostNotY(IFXTransaction tx, string qryAssyLot)
        {
            string shipHistQuery = string.Format(@"
                SELECT COUNT(*)
                FROM ship_hist
                WHERE assy_lot_no[1,7] = '{0}' 
                  AND trans_del_flag = 'N'
                  AND qty_flag = 'G'
                  AND post <> 'Y'
            ", qryAssyLot);

            DataTable dt = IfxDataAccess.ExecuteDataTable(tx, shipHistQuery);

            int cnt = 0;
            if (dt != null && dt.Rows.Count > 0 && dt.Rows[0][0] != DBNull.Value)
            {
                cnt = Convert.ToInt32(dt.Rows[0][0]);
            }

            return cnt;
        }

        public static void InsertCchuFromWiplotA5(IFXTransaction tx, string endDate, Action<string> logCallback)
        {
            try
            {
                logCallback("開始處理 A5 條件數據...");

                string query = string.Format(@"
                    SELECT wlot_lot_number
                    FROM wiplot
                    WHERE wlot_qty_1 = 0
                      AND wlot_qty_2 = 0
                      AND wlot_crte_category = 'A5'
                      AND wlot_oper < 2070
                      AND wlot_oper >= 2000
                      AND wlot_dt_last_trans <= '{0}'
                ", endDate);

                var results = IfxDataAccess.ExecuteDataTable(tx, query);
                logCallback($"預計處理 {results.Rows.Count} 筆");

                foreach (DataRow row in results.Rows)
                {
                    string lotNo = row["wlot_lot_number"].ToString().Trim();
                    ProcessWiplotLotA5(tx, lotNo, endDate,logCallback);
                }

                logCallback("A5 條件數據處理完成...");
            }
            catch (Exception ex)
            {
                logCallback($"處理 A5 條件數據發生錯誤: {ex.Message}");
                throw;
            }
        }

        private static void ProcessWiplotLotA5(IFXTransaction tx, string lotNo, string endDate,Action<string>logCallback)
        {
            // 檢查 ship_flag (oper = 2063)
            string query = string.Format(@"
                SELECT wflt_ship_flag
                FROM waflot_hist
                WHERE wflt_waflot_no = '{0}'
                  AND wflt_oper = 2063
                  AND wflt_del_flag = 'N'
            ", lotNo);

            DataTable dt = IfxDataAccess.ExecuteDataTable(tx, query);

            string shipFlag = "N";
            if (dt != null && dt.Rows.Count > 0 && dt.Rows[0][0] != DBNull.Value)
            {
                shipFlag = dt.Rows[0][0].ToString().Trim();
            }

            // ship_flag != "Y" 時直接返回
            if (shipFlag != "Y")
            {
                return;
            }

            // 查詢 taplot 相關資料
            string secondQuery = string.Format(@"
                SELECT wflt_taplot_no, wflt_ship_flag, wflt_ship_date
                FROM waflot_hist
                WHERE wflt_waflot_no = '{0}'
                  AND wflt_del_flag = 'N'
            ", lotNo);

            var results = IfxDataAccess.ExecuteDataTable(tx, secondQuery);

            foreach (DataRow row in results.Rows)
            {
                string taplotNo = row["wflt_taplot_no"] != DBNull.Value
                    ? row["wflt_taplot_no"].ToString().Trim()
                    : "";

                string wafShipFlag = row["wflt_ship_flag"] != DBNull.Value
                    ? row["wflt_ship_flag"].ToString().Trim()
                    : "N";

                int taplotShipDate = 0;
                if (row["wflt_ship_date"] != DBNull.Value)
                {
                    taplotShipDate = Convert.ToInt32(row["wflt_ship_date"]);
                }

                if (string.IsNullOrEmpty(taplotNo))
                {
                    continue;
                }

                if (wafShipFlag != "Y")
                {
                    continue;
                }

                // 計算該 taplot_no 的記錄數
                string cntQuery = string.Format(@"
                    SELECT COUNT(*) 
                    FROM waflot_hist
                    WHERE wflt_taplot_no = '{0}'
                      AND wflt_del_flag = 'N'
                ", taplotNo);

                DataTable dt2 = IfxDataAccess.ExecuteDataTable(tx, cntQuery);

                int count = 0;
                if (dt2 != null && dt2.Rows.Count > 0 && dt2.Rows[0][0] != DBNull.Value)
                {
                    count = Convert.ToInt32(dt2.Rows[0][0]);
                }

                // 處理 count = 1 的情況
                if (count == 1 && taplotShipDate <= Convert.ToInt32(endDate))
                {
                    InsertCchu(tx, lotNo, logCallback);
                    InsertCchu(tx, taplotNo, logCallback);
                }

                // 處理 count = 2 的情況
                if (count == 2)
                {
                    string wlot2Query = string.Format(@"
                        SELECT wflt_ship_flag, wflt_ship_date
                        FROM waflot_hist
                        WHERE wflt_waflot_no <> '{0}'
                          AND wflt_taplot_no = '{1}'
                          AND wflt_del_flag = 'N'
                    ", lotNo, taplotNo);

                    DataTable dt3 = IfxDataAccess.ExecuteDataTable(tx, wlot2Query);

                    if (dt3 != null && dt3.Rows.Count > 0)
                    {
                        string wlot2ShipFlag = dt3.Rows[0]["wflt_ship_flag"] != DBNull.Value
                            ? dt3.Rows[0]["wflt_ship_flag"].ToString().Trim()
                            : "N";

                        int wlot2ShipDate = 0;
                        if (dt3.Rows[0]["wflt_ship_date"] != DBNull.Value)
                        {
                            wlot2ShipDate = Convert.ToInt32(dt3.Rows[0]["wflt_ship_date"]);
                        }

                        if (wlot2ShipFlag == "Y" && wlot2ShipDate <= Convert.ToInt32(endDate))
                        {
                            InsertCchu(tx, lotNo,logCallback);
                            InsertCchu(tx, taplotNo, logCallback);
                        }
                    }
                }
            }
        }

        private static void InsertCchu(IFXTransaction tx, string lotNo,Action<string> logCallback)
        {
            string insertSQL = string.Format("INSERT INTO cchu (wlot_lot_number) VALUES ('{0}')", lotNo);
            IfxDataAccess.ExecuteNonQuery(tx, insertSQL);
            cchuCount++;
            logCallback($"插入到 cchu: {lotNo} 第{cchuCount}筆");
        }
    }
}
using CPISData.Data;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace MonthBackup_FE.AR.Provider
{
    public static class DatabaseProvider2
    {
        public static string connectionString = ConnectionStrings.FromName("comets");
        public static int cchuCount = 0;
        public static void ExecuteDelLoadLogic(IFXTransaction tx, string endDate, Action<string> logCallback)
        {
            try
            {
                Console.WriteLine("開始 del_load 處理...");

                // 1. 創建臨時表 cchu
                CreateTempCchuTable(tx,logCallback);

                //// 2. 清空表 ar_exclude
                ClearArExclude(tx, logCallback);

                // 3. 執行 A2 部分邏輯
                InsertCchuFromWiplotA2(tx, endDate, logCallback);

                // 4. 執行 A5 部分邏輯
                InsertCchuFromWiplotA5(tx, endDate, logCallback);

                //建立Index，讓後續備份子查詢更快速
                string createIndexSQL = "CREATE INDEX idx_cchu_lot ON cchu(wlot_lot_number);";
                IfxDataAccess.ExecuteNonQuery(tx, createIndexSQL);

                Console.WriteLine("del_load 處理完成。");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"執行 del_load 時發生錯誤: {ex.Message}");
            }
        }

        private static void CreateTempCchuTable(IFXTransaction tx, Action<string> logCallback)
        {
            string createSQL = @"
                CREATE temp TABLE cchu (
                    wlot_lot_number CHAR(11)
                )
            ";
            IfxDataAccess.ExecuteNonQuery(tx, createSQL);
            logCallback("臨時表 cchu 已建立...");
        }

        private static void ClearArExclude(IFXTransaction tx, Action<string> logCallback)
        {
            string deleteSQL = "DELETE FROM ar_exclude";
            IfxDataAccess.ExecuteNonQuery(tx, deleteSQL);
            logCallback("表 ar_exclude 已清空...");
        }

        private static void ClearCCHU(IFXTransaction tx)
        {
            string deleteSQL = "Drop Table cchu";
            IfxDataAccess.ExecuteNonQuery(tx, deleteSQL);
            Console.WriteLine("表 cchu 已刪除...");
        }

        private static void ClearCCHU_Check(IFXTransaction tx)
        {
            string checkTableSQL = @"
        SELECT COUNT(*) 
        FROM systables 
        WHERE tabname = 'cchu' AND tabtype = 'T'
    ";
            int tableCount = Convert.ToInt32(IfxDataAccess.ExecuteScalar(connectionString, checkTableSQL));
            if (tableCount > 0)
            {
                string deleteSQL = "DROP TABLE cchu";
                IfxDataAccess.ExecuteNonQuery(tx, deleteSQL);
                Console.WriteLine("臨時表 cchu 已刪除...");
            }
            else
            {
                Console.WriteLine("無需刪除，cchu 表並不存在...");
            }
        }
       

        private static void DropTempTableIfExists(IFXTransaction tx, string tableName)
        {
            try
            {
                string dropSQL = $"DROP TABLE {tableName}";
                IfxDataAccess.ExecuteNonQuery(tx, dropSQL);
            }
            catch
            {
                // 暫存表可能不存在，忽略錯誤
            }
        }
        public static void InsertCchuFromWiplotA2(IFXTransaction tx, string endDate, Action<string> logCallback)
        {
            try
            {
                Console.WriteLine("開始處理 A2 條件數據...");

                string query = string.Format(@"
                    SELECT wlot_lot_number, wlot_crt_dat_al_1, wlot_dt_last_trans
                    FROM wiplot
                    WHERE wlot_qty_1 = 0
                      AND wlot_qty_2 = 0
                      AND wlot_crte_category = 'A2'
                      AND wlot_dt_last_trans <= '{0}'", endDate);
                var results = IfxDataAccess.ExecuteDataTable(tx, query);
                logCallback($"預計處理 {results.Rows.Count} 筆");
                foreach (DataRow row in results.Rows)
                {
                    string lotNo = row["wlot_lot_number"].ToString().Trim();
                    string customer = row["wlot_crt_dat_al_1"].ToString();
                    string qryAssyLot = lotNo.Length >= 7 ? lotNo.Substring(0, 7) : lotNo;

                    Console.WriteLine($"lot_no: {lotNo}, customer: {customer}");

                 

                    if (CheckShipHist(tx, qryAssyLot, lotNo,logCallback)) continue;
                    if (CheckPShipHistAndQty(tx, qryAssyLot, lotNo, logCallback)) continue;
                    if (CheckLotNoInWiplot(tx, lotNo, logCallback)) continue;

                    InsertCchu(tx, lotNo, logCallback);
                }

                logCallback("A2 條件數據處理完成...");
            }
            catch (Exception ex)
            {
                logCallback($"處理 A2 條件數據發生錯誤: {ex.Message}");
            }
        }

        private static bool CheckShipHist(IFXTransaction tx, string qryAssyLot, string lotNo,Action<string>logCallback)
        {
            string shipHistQuery = string.Format(@"
                SELECT COUNT(*)
                FROM ship_hist
                WHERE SUBSTRING(assy_lot_no FROM 1 FOR 7) = '{0}' 
                  AND trans_del_flag = 'N'
                  AND qty_flag = 'G'
                  AND post <> 'Y'", qryAssyLot);

            DataTable dt = IfxDataAccess.ExecuteDataTable(tx, shipHistQuery);

            int cnt = 0;
            if (dt != null && dt.Rows.Count > 0 && dt.Rows[0][0] != DBNull.Value)
            {
                cnt = Convert.ToInt32(dt.Rows[0][0]);
            }

            if (cnt > 0)
            {
                Console.WriteLine($"ship_hist(post <> Y): {lotNo}, cnt: {cnt}");
                string insertExclusionSQL = string.Format("INSERT INTO ar_exclude (assy_lot) VALUES ('{0}')", lotNo);
                logCallback(insertExclusionSQL);
                IfxDataAccess.ExecuteNonQuery(tx, insertExclusionSQL);
                return true;
            }
            return false;
        }

        private static bool CheckPShipHistAndQty(IFXTransaction tx, string qryAssyLot, string lotNo, Action<string> logCallback)
        {
            // 修正1: 只取第一筆記錄 (使用 FIRST 1 或處理時只取第一筆)
            string pShipHistQuery = string.Format(@"
                SELECT FIRST 1 part_id, cust_lot_no, SUM(qty) as sum_qty
                FROM ship_hist
                WHERE SUBSTRING(assy_lot_no FROM 1 FOR 7) = '{0}'
                  AND trans_del_flag = 'N'
                  AND qty_flag = 'G'
                GROUP BY part_id, cust_lot_no", qryAssyLot);

            DataTable result = IfxDataAccess.ExecuteDataTable(tx, pShipHistQuery);

            // 只處理第一筆 (如果存在)
            if (result.Rows.Count > 0)
            {
                DataRow row = result.Rows[0];
                string partId = row["part_id"].ToString();
                string custLotNo = row["cust_lot_no"].ToString();

                string imgFileQuery = string.Format(@"
                    SELECT SUM(img10)
                    FROM ds2@d270tips:img_file
                    WHERE img01 = '{0}'
                      AND img02 LIKE 'MS%'
                      AND img03 LIKE 'GOOD%'
                      AND img04 = '{1}'", partId, custLotNo);

                DataTable dt = IfxDataAccess.ExecuteDataTable(tx, imgFileQuery);

                int pShipQty = 0;
                if (dt != null && dt.Rows.Count > 0 && dt.Rows[0][0] != DBNull.Value)
                {
                    pShipQty = Convert.ToInt32(dt.Rows[0][0]);
                }

                if (pShipQty != 0)
                {
                    string insertExclusionSQL = string.Format("INSERT INTO ar_exclude (assy_lot) VALUES ('{0}')", lotNo);
                    IfxDataAccess.ExecuteNonQuery(tx, insertExclusionSQL);
                    logCallback(insertExclusionSQL);
                    return true;
                }
            }

            return false;
        }

        private static bool CheckLotNoInWiplot(IFXTransaction tx, string lotNo,Action<string> logCallback)
        {
            int cnt = 0;

            // 修正2: 根據 lot_no 長度使用不同查詢邏輯
            if (lotNo.Trim().Length == 7)
            {
                string lotNoCheckQuery = string.Format(@"
                    SELECT COUNT(*)
                    FROM wiplot
                    WHERE (wlot_qty_1 != 0 OR wlot_qty_2 != 0)
                      AND SUBSTRING(wlot_lot_number FROM 1 FOR 7) = '{0}'", lotNo);

                DataTable dt = IfxDataAccess.ExecuteDataTable(tx, lotNoCheckQuery);
                if (dt != null && dt.Rows.Count > 0 && dt.Rows[0][0] != DBNull.Value)
                {
                    cnt = Convert.ToInt32(dt.Rows[0][0]);
                }
            }
            else
            {
                string lotNo7 = lotNo.Length >= 7 ? lotNo.Substring(0, 7) : lotNo;
                string lotNoCheckQuery = string.Format(@"
                    SELECT COUNT(*)
                    FROM wiplot
                    WHERE (wlot_qty_1 != 0 OR wlot_qty_2 != 0)
                      AND wlot_lot_number = '{0}'", lotNo7);

                DataTable dt = IfxDataAccess.ExecuteDataTable(tx, lotNoCheckQuery);
                if (dt != null && dt.Rows.Count > 0 && dt.Rows[0][0] != DBNull.Value)
                {
                    cnt = Convert.ToInt32(dt.Rows[0][0]);
                }
            }

            if (cnt != 0)
            {
                string insertExclusionSQL = string.Format("INSERT INTO ar_exclude (assy_lot) VALUES ('{0}')", lotNo);
                IfxDataAccess.ExecuteNonQuery(tx, insertExclusionSQL);
                logCallback(insertExclusionSQL);
                return true;
            }
            return false;
        }

        public static void InsertCchuFromWiplotA5(IFXTransaction tx, string endDate, Action<string> logCallback)
        {
            try
            {
                Console.WriteLine("開始處理 A5 條件數據...");

                string query = string.Format(@"
                    SELECT wlot_lot_number
                    FROM wiplot
                    WHERE wlot_qty_1 = 0
                      AND wlot_qty_2 = 0
                      AND wlot_crte_category = 'A5'
                      AND wlot_oper < 2070
                      AND wlot_oper >= 2000
                      AND wlot_dt_last_trans <= '{0}'", endDate);
                var results = IfxDataAccess.ExecuteDataTable(tx, query);

                foreach (DataRow row in results.Rows)
                {
                    string lotNo = row["wlot_lot_number"].ToString().Trim();
                    ProcessWiplotLot(tx, lotNo, endDate,logCallback);
                }

                logCallback("A5 條件數據處理完成...");
            }
            catch (Exception ex)
            {
                logCallback($"處理 A5 條件數據發生錯誤: {ex.Message}");
            }
        }

        private static void ProcessWiplotLot(IFXTransaction tx, string lotNo, string endDate,Action<string>logCallback)
        {
            // 檢查 ship_flag
            string query = string.Format(@"
                SELECT wflt_ship_flag
                FROM waflot_hist
                WHERE wflt_waflot_no = '{0}'
                  AND wflt_oper = 2063
                  AND wflt_del_flag = 'N'", lotNo);

            DataTable dt = IfxDataAccess.ExecuteDataTable(tx, query);

            string shipFlag = "";
            if (dt != null && dt.Rows.Count > 0 && dt.Rows[0][0] != DBNull.Value)
            {
                shipFlag = dt.Rows[0][0].ToString();
            }

            // 修正3: ship_flag != "Y" 時直接返回,不插入
            if (shipFlag != "Y") return;

            // 修正4: 不在這裡插入 lot_no,等後續條件判斷
            // InsertCchu(tx, lotNo); // ← 移除這行

            // 查詢 taplot 相關資料
            string secondQuery = string.Format(@"
                SELECT wflt_taplot_no, wflt_ship_flag, wflt_ship_date
                FROM waflot_hist
                WHERE wflt_waflot_no = '{0}'
                  AND wflt_del_flag = 'N'", lotNo);
            var results = IfxDataAccess.ExecuteDataTable(tx, secondQuery);

            foreach (DataRow row in results.Rows)
            {
                string taplotNo = row["wflt_taplot_no"] != DBNull.Value ? row["wflt_taplot_no"].ToString() : "";
                string wafShipFlag = row["wflt_ship_flag"] != DBNull.Value ? row["wflt_ship_flag"].ToString() : "N";
                int taplotShipDate = 0;
                if (row["wflt_ship_date"] != DBNull.Value && row["wflt_ship_date"] != null)
                {
                    taplotShipDate = Convert.ToInt32(row["wflt_ship_date"]);
                }

                if (string.IsNullOrEmpty(taplotNo) || wafShipFlag != "Y") continue;

                // 計算該 taplot_no 的記錄數
                string cntQuery = string.Format(@"
                    SELECT COUNT(*) 
                    FROM waflot_hist
                    WHERE wflt_taplot_no = '{0}'
                      AND wflt_del_flag = 'N'", taplotNo);

                DataTable dt2 = IfxDataAccess.ExecuteDataTable(tx, cntQuery);

                int count = 0;
                if (dt2 != null && dt2.Rows.Count > 0 && dt2.Rows[0][0] != DBNull.Value)
                {
                    count = Convert.ToInt32(dt2.Rows[0][0]);
                }

                // 修正5: 處理 count = 1 的情況
                if (count == 1 && taplotShipDate <= Convert.ToInt32(endDate))
                {
                    InsertCchu(tx, lotNo, logCallback);
                    InsertCchu(tx, taplotNo, logCallback);
                }

                // 修正6: 補上 count = 2 的完整邏輯
                if (count == 2)
                {
                    string wlot2Query = string.Format(@"
                        SELECT wflt_ship_flag, wflt_ship_date
                        FROM waflot_hist
                        WHERE wflt_waflot_no <> '{0}'
                          AND wflt_taplot_no = '{1}'
                          AND wflt_del_flag = 'N'", lotNo, taplotNo);

                    DataTable dt3 = IfxDataAccess.ExecuteDataTable(tx, wlot2Query);

                    if (dt3 != null && dt3.Rows.Count > 0)
                    {
                        string wlot2ShipFlag = dt3.Rows[0]["wflt_ship_flag"] != DBNull.Value
                            ? dt3.Rows[0]["wflt_ship_flag"].ToString()
                            : "N";

                        int wlot2ShipDate = 0;
                        if (dt3.Rows[0]["wflt_ship_date"] != DBNull.Value)
                        {
                            wlot2ShipDate = Convert.ToInt32(dt3.Rows[0]["wflt_ship_date"]);
                        }

                        if (wlot2ShipFlag == "Y" && wlot2ShipDate <= Convert.ToInt32(endDate))
                        {
                            InsertCchu(tx, lotNo, logCallback);
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
            logCallback($"插入到 cchu: {lotNo} 第{cchuCount++}筆");
        }


    }
}

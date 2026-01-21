using System;
using System.Data;
using System.IO;
using CPISData.Data;
using MonthBackup_FE.AR_EXTEND.Provider;
using MonthBackup_FE.Helper;

namespace MonthBackup_FE.AR_EXTEND.Service
{
    /// <summary>
    /// 對應 ar_extend.4gl 的 gen_ar_lot、gen_ar_date
    /// </summary>
    public class ArExtendService
    {
        /// <summary>
        /// 對應 4GL：LOAD FROM g.file_name INSERT INTO ar_temp
        /// 這裡用簡單的逐行 INSERT，你可以再換成 bulk load。
        /// </summary>
        public static void LoadArTempFromFile(IFXTransaction tx, string fileName, bool isDelete,Action<string> logCallback)
        {
            // 1. 決定實際檔案路徑
            string folder = isDelete? "AR_Del_" + DateTime.Now.ToString("yyyyMM") : "AR_" + DateTime.Now.ToString("yyyyMM");
            string baseName;

            if (string.IsNullOrEmpty(fileName))
            {
                baseName = "cchu.995";
            }
            else
            {
                baseName = fileName.Trim();
                if (!baseName.EndsWith(".995", StringComparison.OrdinalIgnoreCase))
                {
                    baseName = baseName + ".995";
                }
            }

            string fullPath = System.IO.Path.Combine(folder, baseName);

            if (!File.Exists(fullPath))
                throw new FileNotFoundException("找不到 lot 檔案: " + fullPath, fullPath);

            string[] lines = File.ReadAllLines(fullPath);
            int count = 0;

            // 2. 一次組多條單筆 INSERT，避免 multi-row VALUES 相容性問題
            System.Text.StringBuilder sb = new System.Text.StringBuilder();

            foreach (string line in lines)
            {
                string lot = (line == null) ? string.Empty : line.Trim();
                if (string.IsNullOrEmpty(lot))
                    continue;

                // 處理單引號跳脫
                lot = lot.Replace("'", "''");

                sb.Append("INSERT INTO ar_temp (wlot_lot_number) VALUES ('")
                  .Append(lot)
                  .AppendLine("');");

                count++;
            }

            if (count > 0)
            {
                string sql = sb.ToString();
                IfxDataAccess.ExecuteNonQuery(tx, sql);
            }

            logCallback("已從檔案 " + fullPath + " 匯入 " + count + " 筆 lot 至 ar_temp");
        }

        /// <summary>
        /// 對應 FUNCTION gen_ar_lot()
        /// </summary>
        public static void GenArLot(IFXTransaction tx, Action<string> logCallback)
        {
            string sqlConfig = @"
SELECT db_name, table_name, ar_type, col_name, col_define, unload_flag
  FROM ar_table
 WHERE ar_type = 'LOT'
   AND del_flag = 'N'
   AND ar_extend_flag = 'Y'";

            DataTable config = IfxDataAccess.ExecuteDataTable(tx, sqlConfig);
            bool isUnload = (ArExtendProvider.Flag == "UNLOAD");
            bool isDelete = (ArExtendProvider.Flag == "DELETE");

            foreach (DataRow row in config.Rows)
            {
                string dbName = row["db_name"].ToString().Trim();
                string tableName = row["table_name"].ToString().Trim();
                string colName = row["col_name"].ToString().Trim();
                string colDefine = row["col_define"].ToString().Trim();
                string unloadFlag = row["unload_flag"].ToString().Trim();

                logCallback("Table Name --> " + dbName + ":" + tableName);

                // UNLOAD 模式且這筆不允許 unload → 跳過
                if (isUnload &&
                    string.Equals(unloadFlag, "N", StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                // UNLOAD 先刪除舊 .995 檔 (對應 RUN 'rm db:table.995')
                if (isUnload)
                {
                    TryRemoveExportFile(dbName, tableName, logCallback);
                }

                // SELECT wlot_lot_number[1,7] FROM ar_temp GROUP BY 1
                string sqlLots = @"
SELECT DISTINCT SUBSTR(wlot_lot_number, 1, 7) AS lot_no
  FROM ar_temp";
                DataTable lots = IfxDataAccess.ExecuteDataTable(tx, sqlLots);

                int cnt = 0;
                foreach (DataRow lotRow in lots.Rows)
                {
                    string lotNo = lotRow["lot_no"].ToString().Trim();
                    if (string.IsNullOrEmpty(lotNo))
                    {
                        continue;
                    }

                    cnt++;
                    if (cnt % 1000 == 0)
                    {
                        logCallback(ArExtendProvider.Flag + " Lot Count:" + cnt);
                    }

                    if (isUnload)
                    {
                        string selectSql = BuildLotSelectSql(dbName, tableName, colName, colDefine, lotNo);
                        ExportOrDeleteLot(tx, dbName, tableName, selectSql, false, logCallback);
                    }
                    else if (isDelete)
                    {
                        string deleteSql = BuildLotDeleteSql(dbName, tableName, colName, colDefine, lotNo);
                        ExportOrDeleteLot(tx, dbName, tableName, deleteSql, true, logCallback);
                    }
                }
            }
        }

        /// <summary>
        /// 對應 FUNCTION gen_ar_date()
        /// </summary>
        public static void GenArDate(IFXTransaction tx, Action<string> logCallback)
        {
            string sqlConfig = @"
SELECT db_name, table_name, ar_type, col_name, col_define, keep_month, unload_flag
  FROM ar_table
 WHERE ar_type = 'DATE'
   AND del_flag = 'N'
   AND ar_extend_flag = 'Y'";

            DataTable config = IfxDataAccess.ExecuteDataTable(tx, sqlConfig);
            bool isUnload = (ArExtendProvider.Flag == "UNLOAD");
            bool isDelete = (ArExtendProvider.Flag == "DELETE");

            // 4GL: pa_ar_start_date = '1/1/1997'
            DateTime startDate = new DateTime(1997, 1, 1);

            foreach (DataRow row in config.Rows)
            {
                string dbName = row["db_name"].ToString().Trim();
                string tableName = row["table_name"].ToString().Trim();
                string colName = row["col_name"].ToString().Trim();
                string colDefine = row["col_define"].ToString().Trim();
                string keepMonth = row["keep_month"].ToString().Trim();
                string unloadFlag = row["unload_flag"].ToString().Trim();

                logCallback("Table Name --> " + dbName + ":" + tableName);

                if (isUnload &&
                    string.Equals(unloadFlag, "N", StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                if (isUnload)
                {
                    TryRemoveExportFile(dbName, tableName, logCallback);
                }

                // pa_ar_date = getARDate(keep_month)
                DateTime arDate = ArExtendProvider.GetArDate(keepMonth);
                int days = (int)(arDate - startDate).TotalDays;

                int i;
                for (i = 0; i <= days; i++)
                {
                    if (i > 0)
                    {
                        arDate = arDate.AddDays(-1);
                    }

                    int arDateInt = ArExtendProvider.ToIntYyyymmdd(arDate);

                    if (isUnload)
                    {
                        string selectSql = BuildDateSelectSql(dbName, tableName, colName, colDefine, arDateInt);
                        ExportOrDeleteDate(tx, dbName, tableName, selectSql, false, logCallback);
                    }
                    else if (isDelete)
                    {
                        string deleteSql = BuildDateDeleteSql(dbName, tableName, colName, colDefine, arDateInt);
                        ExportOrDeleteDate(tx, dbName, tableName, deleteSql, true, logCallback);
                    }
                }
            }
        }

        #region LOT SQL 組合 & 執行

        private static string BuildLotSelectSql(
            string dbName,
            string tableName,
            string colName,
            string colDefine,
            string lotNo)
        {
            if (string.Equals(colDefine, "NONE", StringComparison.OrdinalIgnoreCase) ||
                string.IsNullOrEmpty(colDefine))
            {
                return "SELECT * FROM " + dbName + ":" + tableName +
                       " WHERE " + colName + " = '" + lotNo + "'";
            }

            if (colDefine == "*")
            {
                return "SELECT * FROM " + dbName + ":" + tableName +
                       " WHERE " + colName + " MATCHES '" + lotNo + "*'";
            }

            return "SELECT * FROM " + dbName + ":" + tableName +
                   " WHERE " + colName + colDefine + " = '" + lotNo + "'";
        }

        private static string BuildLotDeleteSql(
            string dbName,
            string tableName,
            string colName,
            string colDefine,
            string lotNo)
        {
            if (string.Equals(colDefine, "NONE", StringComparison.OrdinalIgnoreCase) ||
                string.IsNullOrEmpty(colDefine))
            {
                return "DELETE FROM " + dbName + ":" + tableName +
                       " WHERE " + colName + " = '" + lotNo + "'";
            }

            if (colDefine == "*")
            {
                return "DELETE FROM " + dbName + ":" + tableName +
                       " WHERE " + colName + " MATCHES '" + lotNo + "*'";
            }

            return "DELETE FROM " + dbName + ":" + tableName +
                   " WHERE " + colName + colDefine + " = '" + lotNo + "'";
        }

        private static void ExportOrDeleteLot(
            IFXTransaction tx,
            string dbName,
            string tableName,
            string sql,
            bool isDelete,
            Action<string> logCallback)
        {
            if (isDelete)
            {
                int affected = IfxDataAccess.ExecuteNonQuery(tx, sql);
                logCallback("DELETE " + dbName + ":" + tableName + " (LOT) 影響筆數: " + affected);
            }
            else
            {
                DataTable dt = IfxDataAccess.ExecuteDataTable(tx, sql);
                if (dt.Rows.Count == 0)
                {
                    return;
                }

                // 模擬 4GL 的 cat tmp1 >> db:table.995，實際這裡輸出為本機檔案
                string fileName = dbName + "_" + tableName + ".995";
                DataExporter.ExportData_Append(dt, fileName, "AR_EXTEND", logCallback);
            }
        }

        #endregion

        #region DATE SQL 組合 & 執行

        private static string BuildDateSelectSql(
            string dbName,
            string tableName,
            string colName,
            string colDefine,
            int arDateInt)
        {
            string yyyymmdd = ArExtendProvider.ToStringYyyymmdd(arDateInt);
            string yyyy = yyyymmdd.Substring(0, 4);
            string mm = yyyymmdd.Substring(4, 2);
            string dd = yyyymmdd.Substring(6, 2);

            string startStr;
            string endStr;

            if (colDefine == "yyyymmdd")
            {
                return "SELECT * FROM " + dbName + ":" + tableName +
                       " WHERE " + colName + " = '" + yyyymmdd + "'";
            }
            if (colDefine == "yyyy/mm/dd")
            {
                return "SELECT * FROM " + dbName + ":" + tableName +
                       " WHERE " + colName + " = '" + yyyy + "/" + mm + "/" + dd + "'";
            }
            if (colDefine == "yyyy-mm-dd")
            {
                return "SELECT * FROM " + dbName + ":" + tableName +
                       " WHERE " + colName + " = '" + yyyy + "-" + mm + "-" + dd + "'";
            }
            if (colDefine == "mm/dd/yyyy")
            {
                return "SELECT * FROM " + dbName + ":" + tableName +
                       " WHERE " + colName + " = '" + mm + "/" + dd + "/" + yyyy + "'";
            }
            if (colDefine == "yyyy-mm-dd hh:mm:ss")
            {
                startStr = yyyy + "-" + mm + "-" + dd + " 00:00:00";
                endStr = yyyy + "-" + mm + "-" + dd + " 23:59:59";
                return "SELECT * FROM " + dbName + ":" + tableName +
                       " WHERE " + colName + " >= '" + startStr + "'" +
                       "   AND " + colName + " <= '" + endStr + "'";
            }
            if (colDefine == "yyyy/mm/dd hh:mm:ss")
            {
                startStr = yyyy + "/" + mm + "/" + dd + " 00:00:00";
                endStr = yyyy + "/" + mm + "/" + dd + " 23:59:59";
                return "SELECT * FROM " + dbName + ":" + tableName +
                       " WHERE " + colName + " >= '" + startStr + "'" +
                       "   AND " + colName + " <= '" + endStr + "'";
            }
            if (colDefine == "yyyymmddhhmm")
            {
                startStr = yyyy + mm + dd + "0000";
                endStr = yyyy + mm + dd + "2359";
                return "SELECT * FROM " + dbName + ":" + tableName +
                       " WHERE " + colName + " >= '" + startStr + "'" +
                       "   AND " + colName + " <= '" + endStr + "'";
            }

            throw new InvalidOperationException("未定義的 col_define: " + colDefine);
        }

        private static string BuildDateDeleteSql(
            string dbName,
            string tableName,
            string colName,
            string colDefine,
            int arDateInt)
        {
            string yyyymmdd = ArExtendProvider.ToStringYyyymmdd(arDateInt);
            string yyyy = yyyymmdd.Substring(0, 4);
            string mm = yyyymmdd.Substring(4, 2);
            string dd = yyyymmdd.Substring(6, 2);

            string startStr;
            string endStr;

            if (colDefine == "yyyymmdd")
            {
                return "DELETE FROM " + dbName + ":" + tableName +
                       " WHERE " + colName + " = '" + yyyymmdd + "'";
            }
            if (colDefine == "yyyy/mm/dd")
            {
                return "DELETE FROM " + dbName + ":" + tableName +
                       " WHERE " + colName + " = '" + yyyy + "/" + mm + "/" + dd + "'";
            }
            if (colDefine == "yyyy-mm-dd")
            {
                return "DELETE FROM " + dbName + ":" + tableName +
                       " WHERE " + colName + " = '" + yyyy + "-" + mm + "-" + dd + "'";
            }
            if (colDefine == "mm/dd/yyyy")
            {
                return "DELETE FROM " + dbName + ":" + tableName +
                       " WHERE " + colName + " = '" + mm + "/" + dd + "/" + yyyy + "'";
            }
            if (colDefine == "yyyy-mm-dd hh:mm:ss")
            {
                startStr = yyyy + "-" + mm + "-" + dd + " 00:00:00";
                endStr = yyyy + "-" + mm + "-" + dd + " 23:59:59";
                return "DELETE FROM " + dbName + ":" + tableName +
                       " WHERE " + colName + " >= '" + startStr + "'" +
                       "   AND " + colName + " <= '" + endStr + "'";
            }
            if (colDefine == "yyyy/mm/dd hh:mm:ss")
            {
                startStr = yyyy + "/" + mm + "/" + dd + " 00:00:00";
                endStr = yyyy + "/" + mm + "/" + dd + " 23:59:59";
                return "DELETE FROM " + dbName + ":" + tableName +
                       " WHERE " + colName + " >= '" + startStr + "'" +
                       "   AND " + colName + " <= '" + endStr + "'";
            }
            if (colDefine == "yyyymmddhhmm")
            {
                startStr = yyyy + mm + dd + "0000";
                endStr = yyyy + mm + dd + "2359";
                return "DELETE FROM " + dbName + ":" + tableName +
                       " WHERE " + colName + " >= '" + startStr + "'" +
                       "   AND " + colName + " <= '" + endStr + "'";
            }

            throw new InvalidOperationException("未定義的 col_define: " + colDefine);
        }

        private static void ExportOrDeleteDate(
            IFXTransaction tx,
            string dbName,
            string tableName,
            string sql,
            bool isDelete,
            Action<string> logCallback)
        {
            if (isDelete)
            {
                int affected = IfxDataAccess.ExecuteNonQuery(tx, sql);
                logCallback("DELETE " + dbName + ":" + tableName + " (DATE) 影響筆數: " + affected);
            }
            else
            {
                DataTable dt = IfxDataAccess.ExecuteDataTable(tx, sql);
                if (dt.Rows.Count == 0)
                {
                    return;
                }

                string fileName = dbName + "_" + tableName + ".995";
                DataExporter.ExportData_Append(dt, fileName, "AR_EXTEND", logCallback);
            }
        }

        #endregion

        private static void TryRemoveExportFile(string dbName, string tableName, Action<string> logCallback)
        {
            string fileName = dbName + "_" + tableName + ".995";

            try
            {
                if (File.Exists(fileName))
                {
                    File.Delete(fileName);
                    logCallback("刪除舊匯出檔: " + fileName);
                }
            }
            catch (Exception ex)
            {
                logCallback("刪除舊匯出檔失敗 " + fileName + ": " + ex.Message);
            }
        }
    }
}
using CPISData.Data;
using MonthBackup_FE.Service;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Reflection;

namespace MonthBackup_FE.ciMesArchive.Eqp.Provider
{
    /// <summary>
    /// 設備資料歸檔工具 - 使用原生 SQL 實作
    /// </summary>
    public class EquipmentArchiver
    {
        private const string MODULE = "EMS";
        private readonly Action<string> _logCallback;
        private readonly List<string> _archiveSqlList;

        public EquipmentArchiver(Action<string> logCallback = null)
        {
            _logCallback = logCallback ?? (msg => Console.WriteLine(msg));
            _archiveSqlList = new List<string>();
        }

        #region 批次歸檔所有設備

        /// <summary>
        /// 批次處理：抓出所有設備 (informix.mes_eqp_eqp)，逐台設備跑歸檔。
        /// </summary>
        /// <param name="tx">交易物件</param>
        /// <param name="beginTime">開始時間 (YYYY-MM-DD)</param>
        /// <param name="endTime">結束時間 (YYYY-MM-DD)</param>
        /// <param name="deleteFromDB">是否刪除原始資料</param>
        /// <returns>全部設備的歸檔 SQL 語句</returns>
        public List<string> ArchiveAllEquipments(
            IFXTransaction tx,
            string beginTime,
            string endTime,
            bool deleteFromDB)
        {
            _archiveSqlList.Clear();

            Log("========================================");
            Log("開始批次歸檔所有設備");
            Log($"時間範圍: {beginTime} ~ {endTime}");
            Log($"刪除原始資料: {(deleteFromDB ? "是" : "否")}");
            Log("========================================");

            // 設定隔離層級
            IfxDataAccess.ExecuteNonQuery(tx, "SET ISOLATION TO DIRTY READ");
            Log("已設定 ISOLATION TO DIRTY READ");
            Log("");

            // 抓出所有設備
            string selectAllEquipSql = "SELECT * FROM informix.mes_eqp_eqp";
            DataTable eqpTable = IfxDataAccess.ExecuteDataTable(tx, selectAllEquipSql);

            if (eqpTable == null || eqpTable.Rows.Count == 0)
            {
                Log("查無任何設備資料，結束。");
                return new List<string>(_archiveSqlList);
            }

            Log($"共取得 {eqpTable.Rows.Count} 台設備，開始逐台處理...");
            Log("");
            string folderPath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, $"ciMesArchive_EQP_{DateTime.Now:yyyyMM}");
            if (!Directory.Exists(folderPath))
            {
                Directory.CreateDirectory(folderPath);
            }
            string historyId = CreateArchiveHistory(tx);
            int idx = 0;
            foreach (DataRow eqpRow in eqpTable.Rows)
            {
                idx++;

                string eqpEqpSid = eqpRow["eqp_eqp_sid"]?.ToString();
                string equipment = eqpRow["equipment"]?.ToString();

                if (string.IsNullOrEmpty(eqpEqpSid))
                {
                    Log($"[{idx}] 找不到 eqp_eqp_sid，略過一筆設備資料。");
                    continue;
                }
                CreateArchiveHistoryDetail(tx, equipment, historyId);
                Log($"[{idx}/{eqpTable.Rows.Count}] 處理設備 SID = {eqpEqpSid}, EQUIP = {equipment}");

                // 為了每台設備各自有一份 SQL 清單，先記錄目前總數，再計算差額
                int beforeCount = _archiveSqlList.Count;

                // 針對單一設備執行歸檔（會把 SQL 加進 _archiveSqlList）
                ArchiveSingleEquipment(tx, eqpEqpSid, equipment, beginTime, endTime, deleteFromDB);

                int afterCount = _archiveSqlList.Count;
                int added = afterCount - beforeCount;

                if (added > 0)
                {
                    // 取出剛剛為這台設備新增的那一段 SQL
                    List<string> perEquipSqls = _archiveSqlList.GetRange(beforeCount, added);

                    // 檔名：設備名稱.sql（空白或非法字元簡單處理一下）
                    string safeEquipName = string.IsNullOrWhiteSpace(equipment)
                        ? eqpEqpSid
                        : equipment.Trim().Replace(Path.DirectorySeparatorChar, '_')
                                   .Replace(Path.AltDirectorySeparatorChar, '_');
                
                    string filePath = Path.Combine(folderPath, $"{safeEquipName}.sql");
                    ExportToFile(filePath, perEquipSqls);
                }
                else
                {
                    Log($"  - 此設備在指定時間範圍內沒有可歸檔的歷史資料，略過輸出檔案");
                }

                Log(""); // 每台設備之間空一行，便於閱讀 log
            }

            Log("========================================");
            Log($"全部設備歸檔完成！共產生 SQL 筆數: {_archiveSqlList.Count}");
            Log("========================================");

            return new List<string>(_archiveSqlList);
        }

        #endregion

        #region 歸檔單一設備 (原本 ArchiveEquipment 改名)

        /// <summary>
        /// 歸檔「單一設備」資料 (供批次呼叫使用，也可以單獨呼叫測試某台設備)
        /// </summary>
        public List<string> ArchiveSingleEquipment(
            IFXTransaction tx,
            string eqpEqpSid,
            string equipment,
            string beginTime,
            string endTime,
            bool deleteFromDB)
        {
            // 注意：這裡不 Clear，全域 _archiveSqlList 由批次方法管理
            Log("----------------------------------------");
            Log($"開始歸檔設備: {equipment} (SID: {eqpEqpSid})");
            Log($"時間範圍: {beginTime} ~ {endTime}");
            Log($"刪除原始資料: {(deleteFromDB ? "是" : "否")}");
            Log("----------------------------------------");

            try
            {

           

                // Step 1: 歸檔設備主表
                ArchiveEquipmentMaster(tx, eqpEqpSid, deleteFromDB);

                // Step 2: 歸檔設備歷史及關聯資料
                ArchiveEquipmentHistory(tx, eqpEqpSid, beginTime, endTime, deleteFromDB);

                // Step 3: 歸檔時間範圍相關的歷史表
                ArchiveTimeRangeRelatedTables(tx, eqpEqpSid, beginTime, endTime, deleteFromDB);

                Log($"設備 {equipment} (SID: {eqpEqpSid}) 歸檔完成。");
                return new List<string>(_archiveSqlList);
            }
            catch (Exception ex)
            {
                Log($"設備 {equipment} (SID: {eqpEqpSid}) 歸檔發生錯誤: {ex.Message}");
                throw;
            }
        }

        #endregion

        #region Step 1: 歸檔設備主表

        private void ArchiveEquipmentMaster(IFXTransaction tx, string eqpEqpSid, bool deleteFromDB)
        {
            Log("Step 1: 歸檔設備主表 (mes_eqp_eqp)");

            string selectSql = $@"
                SELECT * 
                FROM informix.mes_eqp_eqp 
                WHERE eqp_eqp_sid = '{eqpEqpSid}'";

            DataTable dt = IfxDataAccess.ExecuteDataTable(tx, selectSql);

            if (dt != null && dt.Rows.Count > 0)
            {
                foreach (DataRow row in dt.Rows)
                {
                    string insertSql = GenerateInsertSql("mes_eqp_eqp", row);
                    _archiveSqlList.Add(insertSql);
                }
                Log($"  - 已歸檔 {dt.Rows.Count} 筆設備主表資料");

                //if (deleteFromDB)
                //{
                //    string deleteSql = $"DELETE FROM informix.mes_eqp_eqp WHERE eqp_eqp_sid = '{eqpEqpSid}'";
                //    IfxDataAccess.ExecuteNonQuery(tx, deleteSql);
                //    Log("  - 已刪除設備主表資料");
                //}
            }
            else
            {
                Log("  - 查無設備主表資料");
            }

            Log("");
        }

        #endregion

        #region Step 2: 歸檔設備歷史及關聯資料

        private void ArchiveEquipmentHistory(
            IFXTransaction tx,
            string eqpEqpSid,
            string beginTime,
            string endTime,
            bool deleteFromDB)
        {
            Log("Step 2: 歸檔設備歷史及關聯資料 (mes_eqp_hist)");

            // 查詢設備歷史
            string histWhere = $"eqp_eqp_sid = '{eqpEqpSid}'";
            if (!string.IsNullOrEmpty(beginTime) && !string.IsNullOrEmpty(endTime))
            {
                histWhere += $" AND rectime >= '{beginTime}' AND rectime <= '{endTime} 23:59:59'";
            }

            string selectHistSql = $"SELECT * FROM mes_eqp_hist WHERE {histWhere}";
            DataTable histDt = IfxDataAccess.ExecuteDataTable(tx, selectHistSql);

            if (histDt == null || histDt.Rows.Count == 0)
            {
                Log("  - 查無設備歷史資料");
                Log("");
                return;
            }

            Log($"  - 找到 {histDt.Rows.Count} 筆設備歷史資料");

            int histCount = 0;
            int cancelCount = 0;
            int triggerCount = 0;
            int subcommCount = 0;
            int extendCount = 0;
            int edcInfoCount = 0;
            int edcDataCount = 0;

            foreach (DataRow histRow in histDt.Rows)
            {
                string eqpHistSid = histRow["eqp_hist_sid"].ToString();

                // 2.1 歸檔歷史記錄本身
                string insertHistSql = GenerateInsertSql("mes_eqp_hist", histRow);
                _archiveSqlList.Add(insertHistSql);
                histCount++;

                // 2.2 歸檔 Cancel History
                cancelCount += ArchiveRelatedTable(tx, "mes_eqp_hist_cancel", "eqp_hist_sid", eqpHistSid, deleteFromDB);

                // 2.3 歸檔 Event Trigger History
                triggerCount += ArchiveRelatedTable(tx, "mes_eqp_event_trigger_hist", "eqp_hist_sid", eqpHistSid, deleteFromDB);

                // 2.4 歸檔 Subcomm (你已確認存在)
                subcommCount += ArchiveRelatedTable(tx, "mes_eqp_subcomm", "eqp_hist_sid", eqpHistSid, deleteFromDB);

                // 2.5 歸檔 Extend Attr History (你已確認存在)
                extendCount += ArchiveRelatedTable(tx, "mes_eqp_extend_attr_hist", "eqp_hist_sid", eqpHistSid, deleteFromDB);

                // 2.6 歸檔 EDC 資料 (mes_edc_eqpinfo + mes_edc_eqpdata)
                var edcCounts = ArchiveEDCData(tx, eqpHistSid, deleteFromDB);
                edcInfoCount += edcCounts.Item1;
                edcDataCount += edcCounts.Item2;

                // 2.7 刪除歷史記錄本身
                if (deleteFromDB)
                {
                    string deleteHistSql = $"DELETE FROM mes_eqp_hist WHERE eqp_hist_sid = '{eqpHistSid}'";
                    IfxDataAccess.ExecuteNonQuery(tx, deleteHistSql);
                }
            }

            Log($"  - 已歸檔 {histCount} 筆設備歷史");
            if (cancelCount > 0) Log($"  - 已歸檔 {cancelCount} 筆取消歷史 (mes_eqp_hist_cancel)");
            if (triggerCount > 0) Log($"  - 已歸檔 {triggerCount} 筆事件觸發歷史 (mes_eqp_event_trigger_hist)");
            if (subcommCount > 0) Log($"  - 已歸檔 {subcommCount} 筆子通訊資料 (mes_eqp_subcomm)");
            if (extendCount > 0) Log($"  - 已歸檔 {extendCount} 筆擴展屬性 (mes_eqp_extend_attr_hist)");
            if (edcInfoCount > 0) Log($"  - 已歸檔 {edcInfoCount} 筆 EDC 資訊 (mes_edc_eqpinfo)");
            if (edcDataCount > 0) Log($"  - 已歸檔 {edcDataCount} 筆 EDC 數據 (mes_edc_eqpdata)");

            if (deleteFromDB)
            {
                Log($"  - 已刪除 {histCount} 筆設備歷史及其關聯資料");
            }

            Log("");
        }

        private int ArchiveRelatedTable(
            IFXTransaction tx,
            string tableName,
            string keyColumn,
            string keyValue,
            bool deleteFromDB)
        {
            string selectSql = $"SELECT * FROM {tableName} WHERE {keyColumn} = '{keyValue}'";

            try
            {
                DataTable dt = IfxDataAccess.ExecuteDataTable(tx, selectSql);

                if (dt != null && dt.Rows.Count > 0)
                {
                    foreach (DataRow row in dt.Rows)
                    {
                        string insertSql = GenerateInsertSql(tableName, row);
                        _archiveSqlList.Add(insertSql);
                    }

                    if (deleteFromDB)
                    {
                        string deleteSql = $"DELETE FROM {tableName} WHERE {keyColumn} = '{keyValue}'";
                        IfxDataAccess.ExecuteNonQuery(tx, deleteSql);
                    }

                    return dt.Rows.Count;
                }
            }
            catch
            {
                // 表不存在或其它錯誤時略過
            }

            return 0;
        }

        private Tuple<int, int> ArchiveEDCData(IFXTransaction tx, string eqpHistSid, bool deleteFromDB)
        {
            int edcInfoCount = 0;
            int edcDataCount = 0;

            // 查詢 EDC Info
            string selectEdcInfoSql = $"SELECT * FROM mes_edc_eqpinfo WHERE eqp_hist_sid = '{eqpHistSid}'";
            DataTable edcInfoDt = IfxDataAccess.ExecuteDataTable(tx, selectEdcInfoSql);

            if (edcInfoDt != null && edcInfoDt.Rows.Count > 0)
            {
                foreach (DataRow infoRow in edcInfoDt.Rows)
                {
                    string edcEqpinfoSid = infoRow["edc_eqpinfo_sid"].ToString();

                    // 歸檔 EDC Info
                    string insertInfoSql = GenerateInsertSql("mes_edc_eqpinfo", infoRow);
                    _archiveSqlList.Add(insertInfoSql);
                    edcInfoCount++;

                    // 查詢並歸檔 EDC Data
                    string selectEdcDataSql = $"SELECT * FROM mes_edc_eqpdata WHERE edc_eqpinfo_sid = '{edcEqpinfoSid}'";
                    DataTable edcDataDt = IfxDataAccess.ExecuteDataTable(tx, selectEdcDataSql);

                    if (edcDataDt != null && edcDataDt.Rows.Count > 0)
                    {
                        foreach (DataRow dataRow in edcDataDt.Rows)
                        {
                            string insertDataSql = GenerateInsertSql("mes_edc_eqpdata", dataRow);
                            _archiveSqlList.Add(insertDataSql);
                            edcDataCount++;
                        }

                        if (deleteFromDB)
                        {
                            string deleteDataSql = $"DELETE FROM mes_edc_eqpdata WHERE edc_eqpinfo_sid = '{edcEqpinfoSid}'";
                            IfxDataAccess.ExecuteNonQuery(tx, deleteDataSql);
                        }
                    }

                    if (deleteFromDB)
                    {
                        string deleteInfoSql = $"DELETE FROM mes_edc_eqpinfo WHERE edc_eqpinfo_sid = '{edcEqpinfoSid}'";
                        IfxDataAccess.ExecuteNonQuery(tx, deleteInfoSql);
                    }
                }
            }

            return Tuple.Create(edcInfoCount, edcDataCount);
        }

        #endregion

        #region Step 3: 歸檔時間範圍相關的歷史表

        private void ArchiveTimeRangeRelatedTables(
            IFXTransaction tx,
            string eqpEqpSid,
            string beginTime,
            string endTime,
            bool deleteFromDB)
        {
            Log("Step 3: 歸檔時間範圍相關的歷史表");

            if (string.IsNullOrEmpty(beginTime) || string.IsNullOrEmpty(endTime))
            {
                Log("  - 未指定時間範圍，跳過");
                Log("");
                return;
            }

            // 定義需要處理的表格 (表名, 關聯欄位, 時間欄位)
            var tables = new[]
            {
                // TODO: 請確認這些表與欄位實際存在，否則會被 try-catch 略過
                new { Table = "mes_eqp_eqp_hist",       KeyCol = "eqp_eqp_sid", TimeCol = "rectime" },
                new { Table = "mes_eqp_event_wip_log",  KeyCol = "resourcesid", TimeCol = "rectime" },
                new { Table = "mes_eqp_oper_assign_hist", KeyCol = "eqp_eqp_sid", TimeCol = "rectime" },
                new { Table = "mes_eqp_oper_except_hist", KeyCol = "eqp_eqp_sid", TimeCol = "rectime" },
                new { Table = "mes_eqp_recipe_hist",   KeyCol = "eqp_eqp_sid", TimeCol = "rectime" },
                new { Table = "mes_eqp_subeqp_hist",   KeyCol = "eqp_eqp_sid", TimeCol = "rectime" },
                new { Table = "mes_eqp_tool_hist",     KeyCol = "eqp_eqp_sid", TimeCol = "rectime" }
            };

            int totalCount = 0;

            foreach (var tbl in tables)
            {
                int count = ArchiveTimeRangeTable(
                    tx,
                    tbl.Table,
                    tbl.KeyCol,
                    eqpEqpSid,
                    tbl.TimeCol,
                    beginTime,
                    endTime,
                    deleteFromDB);

                if (count > 0)
                {
                    Log($"  - {tbl.Table}: 已歸檔 {count} 筆");
                    totalCount += count;
                }
            }

            if (totalCount == 0)
            {
                Log("  - 查無時間範圍內的其它歷史資料");
            }

            Log("");
        }

        private int ArchiveTimeRangeTable(
            IFXTransaction tx,
            string tableName,
            string keyColumn,
            string keyValue,
            string timeColumn,
            string beginTime,
            string endTime,
            bool deleteFromDB)
        {
            string selectSql = $@"
                SELECT * 
                FROM {tableName} 
                WHERE {keyColumn} = '{keyValue}' 
                  AND {timeColumn} >= '{beginTime}' 
                  AND {timeColumn} <= '{endTime} 23:59:59'";

            try
            {
                DataTable dt = IfxDataAccess.ExecuteDataTable(tx, selectSql);

                if (dt != null && dt.Rows.Count > 0)
                {
                    foreach (DataRow row in dt.Rows)
                    {
                        string insertSql = GenerateInsertSql(tableName, row);
                        _archiveSqlList.Add(insertSql);
                    }

                    if (deleteFromDB)
                    {
                        string deleteSql = $@"
                            DELETE FROM {tableName} 
                            WHERE {keyColumn} = '{keyValue}' 
                              AND {timeColumn} >= '{beginTime}' 
                              AND {timeColumn} <= '{endTime} 23:59:59'";
                        IfxDataAccess.ExecuteNonQuery(tx, deleteSql);
                    }

                    return dt.Rows.Count;
                }
            }
            catch
            {
                // 表不存在或其它錯誤時略過
            }

            return 0;
        }

        #endregion

        #region 輔助方法

        /// <summary>
        /// 產生 INSERT SQL 語句 (用於備份)
        /// </summary>
        private string GenerateInsertSql(string tableName, DataRow row)
        {
            var columns = new List<string>();
            var values = new List<string>();

            foreach (DataColumn col in row.Table.Columns)
            {
                columns.Add(col.ColumnName);

                object value = row[col];
                if (value == null || value == DBNull.Value)
                {
                    values.Add("NULL");
                }
                else
                {
                    string strValue = value.ToString().Replace("'", "''"); // 跳脫單引號
                    values.Add($"'{strValue}'");
                }
            }

            return $"INSERT INTO {tableName} ({string.Join(", ", columns)}) VALUES ({string.Join(", ", values)});";
        }

        private void Log(string message)
        {
            _logCallback?.Invoke(message);
        }

        #endregion

        #region 使用範例

        public static class ArchiveUsageExample
        {
            /// <summary>
            /// 範例：批次把所有設備 (3000 多台) 做歸檔
            /// </summary>
            public static void ArchiveAll()
            {
                using (UserDefineTableService service = new UserDefineTableService())
                using (IFXTransaction tx = service.BeginTransaction())
                {
                    try
                    {
                        var archiver = new EquipmentArchiver(msg => Console.WriteLine(msg));

                        // 例如：歸檔 2022/01/01 ~ 2022/12/31 的歷史
                        string beginTime = "2022-01-01";
                        string endTime = "2022-12-31";

                        List<string> sqls = archiver.ArchiveAllEquipments(
                            tx: tx,
                            beginTime: beginTime,
                            endTime: endTime,
                             GlobalSettings.IsDeleteMode // 先不要刪資料，確認備份正確再改 true
                        );

                        // 依需要匯出成檔案
                        string outputPath = $@"C:\Archive\AllEquip_{DateTime.Now:yyyyMMdd_HHmmss}.sql";
                        archiver.ExportToFile(outputPath, sqls);

                        tx.Commit();
                        Console.WriteLine($"批次歸檔完成，總共 {sqls.Count} 筆 INSERT SQL，已輸出至: {outputPath}");
                    }
                    catch (Exception ex)
                    {
                        tx.Rollback();
                        Console.WriteLine($"批次歸檔失敗: {ex.Message}");
                        throw;
                    }
                }
            }

            /// <summary>
            /// 範例：只測試單一設備 (例如 BN110 對應的 SID 200902161340088247)
            /// </summary>
            public static void ArchiveSingleBN110()
            {
                using (UserDefineTableService service = new UserDefineTableService())
                using (IFXTransaction tx = service.BeginTransaction())
                {
                    try
                    {
                        var archiver = new EquipmentArchiver(msg => Console.WriteLine(msg));

                        string eqpEqpSid = "200902161340088247"; // BN110 的 SID
                        string equipment = "BN110";

                        List<string> sqls = archiver.ArchiveSingleEquipment(
                            tx: tx,
                            eqpEqpSid: eqpEqpSid,
                            equipment: equipment,
                            beginTime: "2022-03-01",
                            endTime: "2022-03-31",
                            deleteFromDB: false
                        );

                        string outputPath = $@"C:\Archive\Equipment_{equipment}_{DateTime.Now:yyyyMMdd_HHmmss}.sql";
                        archiver.ExportToFile(outputPath, sqls);

                        tx.Commit();
                        Console.WriteLine($"設備 {equipment} 歸檔完成，SQL 筆數: {sqls.Count}");
                    }
                    catch (Exception ex)
                    {
                        tx.Rollback();
                        Console.WriteLine($"設備歸檔失敗: {ex.Message}");
                        throw;
                    }
                }
            }
        }

        #endregion

        #region 匯出歸檔結果

        /// <summary>
        /// 將歸檔 SQL 匯出到檔案
        /// </summary>
        public void ExportToFile(string filePath, List<string> sqlList)
        {
            using (var writer = new System.IO.StreamWriter(filePath, false, Encoding.UTF8))
            {
                writer.WriteLine("-- Equipment Archive Export");
                writer.WriteLine($"-- Generated at: {DateTime.Now:yyyy-MM-dd HH:mm:ss}");
                writer.WriteLine($"-- Total records: {sqlList.Count}");
                writer.WriteLine();

                foreach (string sql in sqlList)
                {
                    writer.WriteLine(sql);
                }
            }

            Log($"已匯出歸檔 SQL 到: {filePath}");
        }

        #endregion


        #region 建立ArchiveHist
        private string CreateArchiveHistory(IFXTransaction tx)
        {
            Log("Step 3: 建立歷史記錄");

            string historyId = GenerateHistoryId();
            string now = DateTime.Now.ToString("yyyy/MM/dd HH:mm:ss");
            string archivePath = Path.Combine(
                AppDomain.CurrentDomain.BaseDirectory,
                $"ciMesArchive_LOT_{DateTime.Now:yyyyMM}");

            string sql = $@"
                INSERT INTO MES_ARCHIVE_HIST
                (archive_history_sid, module, archivepath, updatetime, userid)
                VALUES 
                ('{historyId}', '{MODULE}', '{archivePath}', '{now}', 'Admin')";

            IfxDataAccess.ExecuteNonQuery(tx, sql);

            return historyId;
        }

        private void CreateArchiveHistoryDetail(IFXTransaction tx, string obj, string histID)
        {
            Log("建立歷史記錄詳細 ");

            string detailId = GenerateHistoryId();
            string now = DateTime.Now.ToString("yyyy/MM/dd HH:mm:ss");


            string sql = $@"
                INSERT INTO MES_ARCHIVE_HIST_DETAIL
                (archive_hist_detail_sid,archive_hist_sid, module, objectid, updatetime, userid)
                VALUES 
                ('{detailId}','{histID}', '{MODULE}', '{obj}', '{now}', 'Admin')";

            IfxDataAccess.ExecuteNonQuery(tx, sql);

        }
        private string GenerateHistoryId()
        {
            return DateTime.Now.ToString("yyyyMMddHHmmssffff");
        }
        #endregion

    }
}

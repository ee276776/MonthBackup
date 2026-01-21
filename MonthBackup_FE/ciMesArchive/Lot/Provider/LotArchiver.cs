using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.IO;
using CPISData.Data;
using MonthBackup_FE.Service;

namespace MonthBackup_FE.ciMesArchive.Lot.Provider
{
    /// <summary>
    /// Lot 歸檔工具 - 根據時間範圍歸檔批號資料
    /// 模擬 WIPTransaction.ArchiveLot 的邏輯
    /// </summary>
    public class LotArchiver
    {
        private readonly Action<string> _logCallback;
        private readonly List<string> _archiveSqlList;
        private const string MODULE = "WIP";

        public LotArchiver(Action<string> logCallback = null)
        {
            _logCallback = logCallback ?? (msg => Console.WriteLine(msg));
            _archiveSqlList = new List<string>();
        }

        #region 主要歸檔方法

        /// <summary>
        /// 批次歸檔符合條件的所有 Lot
        /// </summary>
        public List<string> ArchiveAllLots(
            IFXTransaction tx,
            string beginTime,
            string endTime,
            bool deleteFromDB)
        {
            _archiveSqlList.Clear();

            Log("========================================");
            Log("開始批次歸檔 Lot 資料");
            Log($"時間範圍: {beginTime} ~ {endTime}");
            Log($"刪除原始資料: {(deleteFromDB ? "是" : "否")}");
            Log("========================================");

            IfxDataAccess.ExecuteNonQuery(tx, "SET ISOLATION TO DIRTY READ");
            Log("已設定 ISOLATION TO DIRTY READ");
            Log("");

            // Step 1: 查詢符合條件的 Lot
            DataTable lotTable = QueryLotsToArchive(tx, beginTime, endTime);

            if (lotTable == null || lotTable.Rows.Count == 0)
            {
                Log("查無符合條件的 Lot 資料，結束。");
                return new List<string>(_archiveSqlList);
            }

            Log($"共查詢到 {lotTable.Rows.Count} 筆符合條件的 Lot");
            Log("");

            // Step 2: 取得使用者自訂的歸檔設定
            List<ArchiveConfig> userConfigs = GetArchiveConfigs(tx);
            Log($"已載入 {userConfigs.Count} 筆使用者自訂歸檔設定");
            Log("");

            // Step 3: 建立歸檔歷史記錄
            string historyId = CreateArchiveHistory(tx);
            Log($"已建立歷史記錄 ID: {historyId}");
            Log("");

            // Step 4: 建立輸出資料夾
            string folderPath = Path.Combine(
                AppDomain.CurrentDomain.BaseDirectory,
                $"ciMesArchive_LOT_{DateTime.Now:yyyyMM}");

            if (!Directory.Exists(folderPath))
            {
                Directory.CreateDirectory(folderPath);
            }
            Log($"輸出資料夾: {folderPath}");
            Log("");

            // Step 5: 逐筆處理 Lot
            int successCount = 0;
            int errorCount = 0;

            foreach (DataRow lotRow in lotTable.Rows)
            {
                string lot = lotRow["lot"]?.ToString();
                if (string.IsNullOrEmpty(lot))
                {
                    continue;
                }

                try
                {
                    Log($"[{successCount + errorCount + 1}/{lotTable.Rows.Count}] 處理 Lot: {lot}");

                    int beforeCount = _archiveSqlList.Count;

                    CreateArchiveHistoryDetail(tx, lot, historyId);


                    // 歸檔單一 Lot (核心邏輯)
                    ArchiveSingleLot(tx, lotRow, userConfigs, historyId, deleteFromDB);

                    int afterCount = _archiveSqlList.Count;
                    int added = afterCount - beforeCount;

                    if (added > 0)
                    {
                        // 輸出該 Lot 的 SQL 檔案
                        List<string> perLotSqls = _archiveSqlList.GetRange(beforeCount, added);
                        string filePath = Path.Combine(folderPath, $"{lot}.sql");
                        ExportToFile(filePath, perLotSqls);

                        Log($"  ✓ 成功歸檔，產生 {added} 筆 SQL");
                        successCount++;
                    }
                    else
                    {
                        Log($"  - 無可歸檔資料");
                    }
                }
                catch (Exception ex)
                {
                    Log($"  ✗ 歸檔失敗: {ex.Message}");
                    errorCount++;

                    RecordArchiveError(tx, historyId, lot, ex);
                }

                Log("");
            }

            Log("========================================");
            Log($"歸檔完成！");
            Log($"  成功: {successCount} 筆");
            Log($"  失敗: {errorCount} 筆");
            Log($"  總 SQL 數: {_archiveSqlList.Count}");
            Log("========================================");

            return new List<string>(_archiveSqlList);
        }

        #endregion

        #region Step 1: 查詢要歸檔的 Lot

        private DataTable QueryLotsToArchive(IFXTransaction tx, string beginTime, string endTime)
        {
            Log("Step 1: 查詢要歸檔的 Lot (使用 LEFT JOIN 優化)");

            string sql = $@"
        SELECT l.* 
        FROM mes_wip_lot l
        LEFT JOIN wiplot w ON l.lot = w.wlot_lot_number
        WHERE l.quantity = 0 
          AND l.squantity = 0 
          AND l.lasttranstime BETWEEN '{beginTime} 00:00:00' AND '{endTime} 23:59:59'
          AND w.wlot_lot_number IS NULL";

            return IfxDataAccess.ExecuteDataTable(tx, sql);
        }

        #endregion

        #region Step 2: 取得歸檔設定

        private List<ArchiveConfig> GetArchiveConfigs(IFXTransaction tx)
        {
            Log("Step 2: 取得使用者自訂歸檔設定");

            string sql = $@"
                SELECT * 
                FROM mes_archive_config 
                WHERE module = '{MODULE}'";

            DataTable dt = IfxDataAccess.ExecuteDataTable(tx, sql);

            var configs = new List<ArchiveConfig>();
            if (dt != null)
            {
                foreach (DataRow row in dt.Rows)
                {
                    configs.Add(new ArchiveConfig
                    {
                        Module = row["module"]?.ToString(),
                        ArchiveTable = row["archivetable"]?.ToString(),
                        ArchiveColumn = row["archivecolumn"]?.ToString(),
                        SystemTable = row["systable"]?.ToString(),
                        SystemColumn = row["syscolumn"]?.ToString()
                    });
                }
            }

            return configs;
        }

        #endregion

        #region Step 3: 建立歷史記錄

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
                (archive_hist_sid, module, archivepath, updatetime, userid)
                VALUES 
                ('{historyId}', '{MODULE}', '{archivePath}', '{now}', 'Admin')";

            IfxDataAccess.ExecuteNonQuery(tx, sql);

            return historyId;
        }

        private void CreateArchiveHistoryDetail(IFXTransaction tx,string obj,string histID)
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

        #region Step 4: 歸檔單一 Lot (核心邏輯 - 模擬 WIPTransaction.ArchiveLot)

        /// <summary>
        /// 歸檔單一 Lot 的所有相關資料
        /// 此方法模擬 WIPTransaction.ArchiveLot 的完整邏輯
        /// </summary>
        private void ArchiveSingleLot(
            IFXTransaction tx,
            DataRow lotRow,
            List<ArchiveConfig> userConfigs,
            string historyId,
            bool deleteFromDB)
        {
            string lot = lotRow["lot"]?.ToString();
            string wipLotSid = lotRow["wip_lot_sid"]?.ToString();
            string startTime = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");

            try
            {
                // 驗證 Lot 資料
                ValidateLotData(tx, lot);

                // 合併系統預設配置 + 使用者自訂配置
                List<ArchiveConfig> allConfigs = BuildAllConfigs(userConfigs);

                // === 1. 歸檔 MES_WIP_LOT 主表 ===
                _archiveSqlList.Add(GenerateInsertSql("mes_wip_lot", lotRow));

                // === 2. 處理 SYSTABLE = MES_WIP_LOT 的配置 ===
                var lotConfigs = allConfigs.Where(c =>
                    c.SystemTable.Equals("mes_wip_lot", StringComparison.OrdinalIgnoreCase)).ToList();

                foreach (var config in lotConfigs)
                {
                    ProcessArchiveConfig(tx, config, lot, wipLotSid, deleteFromDB);
                }

                // === 3. 刪除 MES_WIP_LOT 主表 ===
                if (deleteFromDB)
                {
                    string deleteLotSql = $"DELETE FROM mes_wip_lot WHERE lot = '{lot}'";
                    IfxDataAccess.ExecuteNonQuery(tx, deleteLotSql);
                }

                // === 4. 處理 MES_WIP_HIST (歷史記錄) === 
                //捨棄config，因config內沒有systable為hist的資料
                ArchiveLotHistory(tx, lot, wipLotSid,  deleteFromDB);

                // === 5. 處理 MES_WIP_COMP (元件資料) ===
                //ArchiveLotComponents(tx, lot, allConfigs, deleteFromDB);
                var lotCompConfigs = allConfigs.Where(c =>
                 c.SystemTable.Equals("mes_wip_comp", StringComparison.OrdinalIgnoreCase)).ToList();

                foreach (var config in lotCompConfigs)
                {
                    ProcessArchiveConfig(tx, config, lot, wipLotSid, deleteFromDB);
                }

                // 記錄成功
                string endTime = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");
                RecordArchiveSuccess(tx, historyId, lot, startTime, endTime);
            }
            catch (Exception ex)
            {
                throw new Exception($"Lot {lot} 歸檔失敗: {ex.Message}", ex);
            }
        }

        /// <summary>
        /// 建立完整的配置清單 (系統預設 + 使用者自訂)
        /// </summary>
        private List<ArchiveConfig> BuildAllConfigs(List<ArchiveConfig> userConfigs)
        {
            var allConfigs = new List<ArchiveConfig>(userConfigs);

            // 系統預設的表格配置 (對應原程式的 CreateVirtualLotConfigInfo)
            var defaultTables = new[]
            {
                // SYSTABLE = MES_WIP_LOT
                new { Table = "mes_cms_lot", Column = "lot", SysTable = "mes_wip_lot" },
                new { Table = "mes_edc_lotdata", Column = "lot", SysTable = "mes_wip_lot" },
                new { Table = "mes_edc_lotinfo", Column = "lot", SysTable = "mes_wip_lot" },
                new { Table = "mes_edc_lotpara", Column = "lot", SysTable = "mes_wip_lot" },
                new { Table = "mes_eqp_lot", Column = "lot", SysTable = "mes_wip_lot" },
                new { Table = "mes_eqp_subeqp_lot", Column = "lot", SysTable = "mes_wip_lot" },
                new { Table = "mes_prc_oper_skip_hist", Column = "lot", SysTable = "mes_wip_lot" },
                new { Table = "mes_spc_comment", Column = "lot", SysTable = "mes_wip_lot" },
                new { Table = "mes_spc_hist", Column = "lot", SysTable = "mes_wip_lot" },
                new { Table = "mes_spc_rawdata", Column = "lot", SysTable = "mes_wip_lot" },
                new { Table = "mes_wip_attr", Column = "lot", SysTable = "mes_wip_lot" },
                new { Table = "mes_wip_attr_modify_hist", Column = "lot", SysTable = "mes_wip_lot" },
                new { Table = "mes_wip_bin", Column = "wip_lot_sid", SysTable = "mes_wip_lot" },
                new { Table = "mes_wip_bonus", Column = "lot", SysTable = "mes_wip_lot" },
                new { Table = "mes_wip_comp_hist", Column = "lot", SysTable = "mes_wip_lot" },
                new { Table = "mes_wip_current_hold", Column = "lot", SysTable = "mes_wip_lot" },
                new { Table = "mes_wip_defect", Column = "lot", SysTable = "mes_wip_lot" },
                new { Table = "mes_wip_extend_attr", Column = "lot", SysTable = "mes_wip_lot" },
                new { Table = "mes_wip_extend_attr_hist", Column = "lot", SysTable = "mes_wip_lot" },
                new { Table = "mes_wip_end_shift", Column = "lot", SysTable = "mes_wip_lot" },
                new { Table = "mes_wip_futureaction", Column = "lot", SysTable = "mes_wip_lot" },
                new { Table = "mes_wip_hist_cancel", Column = "wip_lot_sid", SysTable = "mes_wip_lot" },
                new { Table = "mes_wip_hold", Column = "lot", SysTable = "mes_wip_lot" },
                new { Table = "mes_wip_issue", Column = "lot", SysTable = "mes_wip_lot" },
                new { Table = "mes_wip_lot_create", Column = "lot", SysTable = "mes_wip_lot" },
                new { Table = "mes_wip_lot_nonactive", Column = "lot", SysTable = "mes_wip_lot" },
                new { Table = "mes_wip_lot_rule", Column = "lot", SysTable = "mes_wip_lot" },
                new { Table = "mes_wip_move_eqp", Column = "lot", SysTable = "mes_wip_lot" },
                new { Table = "mes_wip_parallel_info", Column = "lot", SysTable = "mes_wip_lot" },
                new { Table = "mes_wip_qtime", Column = "lot", SysTable = "mes_wip_lot" },
                new { Table = "mes_wip_redo", Column = "lot", SysTable = "mes_wip_lot" },
                new { Table = "mes_wip_release", Column = "lot", SysTable = "mes_wip_lot" },
                new { Table = "mes_wip_rework", Column = "lot", SysTable = "mes_wip_lot" },
                new { Table = "mes_wip_scrap", Column = "lot", SysTable = "mes_wip_lot" },
                new { Table = "mes_wip_time_ctrl", Column = "lot", SysTable = "mes_wip_lot" },
                new { Table = "mes_wip_wtime", Column = "lot", SysTable = "mes_wip_lot" },
                new { Table = "mes_wip_split", Column = "parentlot", SysTable = "mes_wip_lot" },
                new { Table = "mes_wip_transfer", Column = "parentlot", SysTable = "mes_wip_lot" },
                new { Table = "mes_wip_consume", Column = "parentlot", SysTable = "mes_wip_lot" },
                new { Table = "mes_wip_merge", Column = "parentlot", SysTable = "mes_wip_lot" },
                
              // SYSTABLE = MES_WIP_COMP
                new { Table = "mes_wip_subcomp_hist", Column = "comp", SysTable = "mes_wip_comp" },
                

                new { Table = "mes_wip_subcomp", Column = "currentcomp", SysTable = "mes_wip_comp" }
            };

            foreach (var def in defaultTables)
            {
                // 避免重複加入
                if (!allConfigs.Any(c =>
                    c.ArchiveTable.Equals(def.Table, StringComparison.OrdinalIgnoreCase)))
                {
                    allConfigs.Add(new ArchiveConfig
                    {
                        Module = MODULE,
                        ArchiveTable = def.Table,
                        ArchiveColumn = def.Column,
                        SystemTable = def.SysTable,
                        SystemColumn = def.Column == "wip_lot_sid" ? "wip_lot_sid" : "lot"
                    });
                }
            }

            return allConfigs;
        }

        /// <summary>
        /// 處理單一歸檔配置
        /// </summary>
        private void ProcessArchiveConfig(
            IFXTransaction tx,
            ArchiveConfig config,
            string lot,
            string wipLotSid,
            bool deleteFromDB)
        {
            try
            {
                // 決定查詢條件的值 (lot 或 wip_lot_sid)
                string keyValue = config.ArchiveColumn.Equals("wip_lot_sid", StringComparison.OrdinalIgnoreCase)
                    ? wipLotSid
                    : lot;

                if (string.IsNullOrEmpty(keyValue))
                {
                    return;
                }

                string selectSql = $@"
                    SELECT * 
                    FROM {config.ArchiveTable} 
                    WHERE {config.ArchiveColumn} = '{keyValue}'";

                DataTable dt = IfxDataAccess.ExecuteDataTable(tx, selectSql);

                if (dt != null && dt.Rows.Count > 0)
                {
                    foreach (DataRow row in dt.Rows)
                    {
                        _archiveSqlList.Add(GenerateInsertSql(config.ArchiveTable, row));
                    }

                    if (deleteFromDB)
                    {
                        string deleteSql = $@"
                            DELETE FROM {config.ArchiveTable} 
                            WHERE {config.ArchiveColumn} = '{keyValue}'";

                        IfxDataAccess.ExecuteNonQuery(tx, deleteSql);
                    }
                }
            }
            catch
            {
                Log($"{config.ArchiveTable}找不到{config.ArchiveColumn}欄位");
                // 表格可能不存在，略過
            }
        }

        ///// <summary>
        ///// 歸檔 Lot 歷史記錄 (MES_WIP_HIST)
        ///// </summary>
        //private void ArchiveLotHistory(
        //    IFXTransaction tx,
        //    string lot,
        //    string wipLotSid,
        //    List<ArchiveConfig> allConfigs,
        //    bool deleteFromDB)
        //{
        //    string selectHistSql = $@"
        //        SELECT * 
        //        FROM mes_wip_hist 
        //        WHERE lot = '{lot}'";

        //    DataTable histDt = IfxDataAccess.ExecuteDataTable(tx, selectHistSql);

        //    if (histDt == null || histDt.Rows.Count == 0)
        //    {
        //        return;
        //    }

        //    var histConfigs = allConfigs.Where(c =>
        //        c.SystemTable.Equals("mes_wip_hist", StringComparison.OrdinalIgnoreCase)).ToList();

        //    foreach (DataRow histRow in histDt.Rows)
        //    {
        //        string wipHistSid = histRow["wip_hist_sid"]?.ToString();
        //        string wipCommentSid = histRow["wip_comm_sid"]?.ToString();

        //        // 1. 歸檔歷史本身
        //        _archiveSqlList.Add(GenerateInsertSql("mes_wip_hist", histRow));

        //        // 2. 歸檔關聯的 Comment
        //        if (!string.IsNullOrEmpty(wipCommentSid))
        //        {
        //            ArchiveWipComment(tx, wipCommentSid, deleteFromDB);
        //        }

        //        // 3. 處理 SYSTABLE = MES_WIP_LOT 的配置
        //        foreach (var config in histConfigs)
        //        {
        //            ProcessArchiveConfigByHistSid(tx, config, wipHistSid, deleteFromDB);
        //        }

        //        // 4. 刪除歷史記錄
        //        if (deleteFromDB)
        //        {
        //            string deleteHistSql = $"DELETE FROM mes_wip_hist WHERE wip_hist_sid = '{wipHistSid}'";
        //            IfxDataAccess.ExecuteNonQuery(tx, deleteHistSql);
        //        }
        //    }
        //}

        private void ArchiveLotHistory(
    IFXTransaction tx,
    string lot,         // lot number
    string wipLotSid,   // 對應 MES_WIP_HIST.WIP_LOT_SID，用來對齊原本邏輯
    bool deleteFromDB)
        {
            // 1. 撈出此 lot 相關的 WIP_HIST（建議使用 WIP_LOT_SID + CANCELFLAG = 'N'）
            string selectHistSql = $@"
        SELECT *
        FROM mes_wip_hist
        WHERE wip_lot_sid = '{wipLotSid}'
          AND cancelflag = 'N'
        ORDER BY wip_hist_sid";   // 如需特定方向再加 ASC/DESC
                                  // 如果你的實際欄位名不同，請對應調整

            DataTable histDt = IfxDataAccess.ExecuteDataTable(tx, selectHistSql);

            if (histDt == null || histDt.Rows.Count == 0)
            {
                return;
            }

            foreach (DataRow histRow in histDt.Rows)
            {
                string wipHistSid = histRow["wip_hist_sid"]?.ToString();
                string wipCommentSid = histRow["wip_comm_sid"]?.ToString();

                // 1. 歸檔 WIP_HISTORY 本身
                _archiveSqlList.Add(GenerateInsertSql("mes_wip_hist", histRow));

                // 2. 歸檔關聯的 Comment（如果有）
                if (!string.IsNullOrEmpty(wipCommentSid))
                {
                    ArchiveWipComment(tx, wipCommentSid, deleteFromDB);
                }

                // 3. 刪除 WIP_HISTORY 記錄（若需要）
                if (deleteFromDB && !string.IsNullOrEmpty(wipHistSid))
                {
                    string deleteHistSql = $@"
                DELETE FROM mes_wip_hist
                WHERE wip_hist_sid = '{wipHistSid}'";

                    IfxDataAccess.ExecuteNonQuery(tx, deleteHistSql);
                }
            }
        }


        /// <summary>
        /// 歸檔 WIP Comment
        /// </summary>
        private void ArchiveWipComment(IFXTransaction tx, string wipCommentSid, bool deleteFromDB)
        {
            string selectSql = $"SELECT * FROM MES_WIP_COMM WHERE wip_comm_sid = '{wipCommentSid}'";
            DataTable dt = IfxDataAccess.ExecuteDataTable(tx, selectSql);

            if (dt != null && dt.Rows.Count > 0)
            {
                foreach (DataRow row in dt.Rows)
                {
                    _archiveSqlList.Add(GenerateInsertSql("mes_wip_comment", row));
                }

                if (deleteFromDB)
                {
                    string deleteSql = $"DELETE FROM mes_wip_comment WHERE wip_comment_sid = '{wipCommentSid}'";
                    IfxDataAccess.ExecuteNonQuery(tx, deleteSql);
                }
            }
        }

        /// <summary>
        /// 處理基於 wip_hist_sid 的配置
        /// </summary>
        private void ProcessArchiveConfigByHistSid(
            IFXTransaction tx,
            ArchiveConfig config,
            string wipHistSid,
            bool deleteFromDB)
        {
            try
            {
                string selectSql = $@"
                    SELECT * 
                    FROM {config.ArchiveTable} 
                    WHERE wip_hist_sid = '{wipHistSid}'";

                DataTable dt = IfxDataAccess.ExecuteDataTable(tx, selectSql);

                if (dt != null && dt.Rows.Count > 0)
                {
                    foreach (DataRow row in dt.Rows)
                    {
                        _archiveSqlList.Add(GenerateInsertSql(config.ArchiveTable, row));
                    }

                    if (deleteFromDB)
                    {
                        string deleteSql = $@"
                            DELETE FROM {config.ArchiveTable} 
                            WHERE wip_hist_sid = '{wipHistSid}'";

                        IfxDataAccess.ExecuteNonQuery(tx, deleteSql);
                    }
                }
            }
            catch
            {
                // 表格可能不存在，略過
            }
        }

        /// <summary>
        /// 歸檔 Lot 元件資料 (MES_WIP_COMP)
        /// </summary>
        private void ArchiveLotComponents(
            IFXTransaction tx,
            string lot,
            List<ArchiveConfig> allConfigs,
            bool deleteFromDB)
        {
            string selectCompSql = $@"
                SELECT * 
                FROM mes_wip_comp 
                WHERE lot = '{lot}'";

            DataTable compDt = IfxDataAccess.ExecuteDataTable(tx, selectCompSql);

            if (compDt == null || compDt.Rows.Count == 0)
            {
                return;
            }

            var compConfigs = allConfigs.Where(c =>
                c.SystemTable.Equals("mes_wip_comp", StringComparison.OrdinalIgnoreCase)).ToList();

            foreach (DataRow compRow in compDt.Rows)
            {
                string comp = compRow["comp"]?.ToString();

                // 1. 歸檔元件本身
                _archiveSqlList.Add(GenerateInsertSql("mes_wip_comp", compRow));

                // 2. 處理 SYSTABLE = MES_WIP_COMP 的配置
                foreach (var config in compConfigs)
                {
                    ProcessArchiveConfigByComp(tx, config, comp, deleteFromDB);
                }

                // 3. 刪除元件記錄
                if (deleteFromDB)
                {
                    string deleteCompSql = $"DELETE FROM mes_wip_comp WHERE comp = '{comp}'";
                    IfxDataAccess.ExecuteNonQuery(tx, deleteCompSql);
                }
            }

            // 歸檔 ComponentCustomAttribute (如果有)
            ArchiveComponentCustomAttributes(tx, lot, deleteFromDB);
        }

        /// <summary>
        /// 處理基於 comp 的配置
        /// </summary>
        private void ProcessArchiveConfigByComp(
            IFXTransaction tx,
            ArchiveConfig config,
            string comp,
            bool deleteFromDB)
        {
            try
            {
                string selectSql = $@"
                    SELECT * 
                    FROM {config.ArchiveTable} 
                    WHERE {config.ArchiveColumn} = '{comp}'";

                DataTable dt = IfxDataAccess.ExecuteDataTable(tx, selectSql);

                if (dt != null && dt.Rows.Count > 0)
                {
                    foreach (DataRow row in dt.Rows)
                    {
                        _archiveSqlList.Add(GenerateInsertSql(config.ArchiveTable, row));
                    }

                    if (deleteFromDB)
                    {
                        string deleteSql = $@"
                            DELETE FROM {config.ArchiveTable} 
                            WHERE {config.ArchiveColumn} = '{comp}'";

                        IfxDataAccess.ExecuteNonQuery(tx, deleteSql);
                    }
                }
            }
            catch
            {
                // 表格可能不存在，略過
            }
        }

        /// <summary>
        /// 歸檔元件自訂屬性
        /// </summary>
        private void ArchiveComponentCustomAttributes(IFXTransaction tx, string lot, bool deleteFromDB)
        {
            try
            {
                string selectSql = $@"
                    SELECT ca.* 
                    FROM mes_wip_comp_custom_attr ca
                    INNER JOIN mes_wip_comp c ON ca.comp = c.comp
                    WHERE c.lot = '{lot}'";

                DataTable dt = IfxDataAccess.ExecuteDataTable(tx, selectSql);

                if (dt != null && dt.Rows.Count > 0)
                {
                    foreach (DataRow row in dt.Rows)
                    {
                        _archiveSqlList.Add(GenerateInsertSql("mes_wip_comp_custom_attr", row));
                    }

                    if (deleteFromDB)
                    {
                        string deleteSql = $@"
                            DELETE FROM mes_wip_comp_custom_attr 
                            WHERE comp IN (
                                SELECT comp FROM mes_wip_comp WHERE lot = '{lot}'
                            )";

                        IfxDataAccess.ExecuteNonQuery(tx, deleteSql);
                    }
                }
            }
            catch
            {
                // 表格可能不存在，略過
            }
        }

        #endregion

        #region 資料驗證

        private void ValidateLotData(IFXTransaction tx, string lot)
        {
            // 1. 檢查數量必須為 0
            string checkQtySql = $@"
                SELECT quantity, squantity 
                FROM mes_wip_lot 
                WHERE lot = '{lot}'";

            DataTable qtDt = IfxDataAccess.ExecuteDataTable(tx, checkQtySql);
            if (qtDt != null && qtDt.Rows.Count > 0)
            {
                int qty = Convert.ToInt32(qtDt.Rows[0]["quantity"]);
                int sqty = Convert.ToInt32(qtDt.Rows[0]["squantity"]);

                if (qty > 0 || sqty > 0)
                {
                    throw new Exception($"批號數量不為零 (qty={qty}, sqty={sqty})");
                }
            }

            // 2. 檢查 ship_hist
            string lotPrefix = lot.Length >= 7 ? lot.Substring(0, 7) : lot;
            if (lot.StartsWith("F") || lot.StartsWith("W"))
            {
                lotPrefix = lot.Length >= 10 ? lot.Substring(0, 10) : lot;
            }

            string checkShipSql = $@"
                SELECT * 
                FROM ship_hist 
                WHERE assy_lot_no LIKE '{lotPrefix}%'";

            DataTable shipDt = IfxDataAccess.ExecuteDataTable(tx, checkShipSql);
            if (shipDt != null && shipDt.Rows.Count > 0)
            {
                foreach (DataRow row in shipDt.Rows)
                {
                    string delFlag = row["trans_del_flag"]?.ToString();
                    string post = row["post"]?.ToString();

                    if (delFlag == "N" && post != "Y")
                    {
                        string assyLot = row["assy_lot_no"]?.ToString();
                        throw new Exception($"assy_lot_no: {assyLot} 尚未確認入庫");
                    }
                }
            }

            // 3. 檢查是否有子批號仍在處理中
            string checkChildSql = $@"
                SELECT lot, quantity, squantity 
                FROM mes_wip_lot 
                WHERE lot LIKE '{lot}%' 
                  AND lot <> '{lot}'";

            DataTable childDt = IfxDataAccess.ExecuteDataTable(tx, checkChildSql);
            if (childDt != null && childDt.Rows.Count > 0)
            {
                foreach (DataRow row in childDt.Rows)
                {
                    int childQty = Convert.ToInt32(row["quantity"]);
                    int childSqty = Convert.ToInt32(row["squantity"]);

                    if (childQty > 0 || childSqty > 0)
                    {
                        string childLot = row["lot"]?.ToString();
                        throw new Exception($"parent lot {lot} 的子批: {childLot} 仍在 WIP 處理中");
                    }
                }
            }
        }

        #endregion

        #region 歷史記錄

        private void RecordArchiveSuccess(
            IFXTransaction tx,
            string historyId,
            string lot,
            string startTime,
            string endTime)
        {
            string detailId = GenerateDetailId();

            string sql = $@"
                INSERT INTO mes_archive_history_detail 
                (archive_history_detail_sid, module, archive_history_sid, objectid, 
                 starttime, endtime, errmessage, errstacktrace, updatetime, userid)
                VALUES 
                ('{detailId}', '{MODULE}', '{historyId}', '{lot}',
                 '{startTime}', '{endTime}', '', '', '{endTime}', 'SYSTEM')";

            IfxDataAccess.ExecuteNonQuery(tx, sql);
        }

        private void RecordArchiveError(
            IFXTransaction tx,
            string historyId,
            string lot,
            Exception ex)
        {
            string detailId = GenerateDetailId();
            string now = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");

            string errMsg = ex.Message.Replace("'", "''");
            string stackTrace = (ex.StackTrace ?? "").Replace("'", "''");

            string sql = $@"
                INSERT INTO mes_archive_history_detail 
                (archive_history_detail_sid, module, archive_history_sid, objectid, 
                 starttime, endtime, errmessage, errstacktrace, updatetime, userid)
                VALUES 
                ('{detailId}', '{MODULE}', '{historyId}', '{lot}',
                 '{now}', '{now}', '{errMsg}', '{stackTrace}', '{now}', 'SYSTEM')";

            try
            {
                IfxDataAccess.ExecuteNonQuery(tx, sql);
            }
            catch
            {
            }
        }

        private string GenerateDetailId()
        {
            return DateTime.Now.ToString("yyyyMMddHHmmssfff") +
                   new Random().Next(1000, 9999).ToString();
        }

        #endregion

        #region 輔助方法

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
                    string strValue = value.ToString().Replace("'", "''");
                    values.Add($"'{strValue}'");
                }
            }

            return $"INSERT INTO {tableName} ({string.Join(", ", columns)}) VALUES ({string.Join(", ", values)});";
        }

        private void Log(string message)
        {
            _logCallback?.Invoke(message);
        }

        public void ExportToFile(string filePath, List<string> sqlList)
        {
            using (var writer = new StreamWriter(filePath, false, Encoding.UTF8))
            {
                writer.WriteLine("-- Lot Archive Export");
                writer.WriteLine($"-- Generated at: {DateTime.Now:yyyy-MM-dd HH:mm:ss}");
                writer.WriteLine($"-- Total records: {sqlList.Count}");
                writer.WriteLine();

                foreach (string sql in sqlList)
                {
                    writer.WriteLine(sql);
                }
            }
        }

        #endregion

        #region 內部類別

        private class ArchiveConfig
        {
            public string Module { get; set; }
            public string ArchiveTable { get; set; }
            public string ArchiveColumn { get; set; }
            public string SystemTable { get; set; }
            public string SystemColumn { get; set; }
        }

        #endregion
    }
}
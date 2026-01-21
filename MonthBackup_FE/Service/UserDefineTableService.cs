using CPISData.Data;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MonthBackup_FE.Service
{
    //public class UserDefineTableService : IDisposable
    //{
    //    private static readonly string dbName = "comets"; // 資料庫名稱
    //    private readonly string connectionString; // 資料庫連線字串
    //    public IFXTransaction Transaction { get; private set; }

    //    public UserDefineTableService(bool isDecrypt = false)
    //    {

    //        connectionString = ConnectionStrings.FromName(dbName);

    //    }

    //    /// <summary>
    //    /// 開啟資料庫交易
    //    /// </summary>
    //    /// <returns>IFXTransaction</returns>
    //    public IFXTransaction BeginTransaction()
    //    {
    //        Transaction = new IFXTransaction(connectionString, IsolationLevel.ReadUncommitted);
    //        return Transaction;
    //    }

    //    /// <summary>
    //    /// 資源清理
    //    /// </summary>
    //    public void Dispose()
    //    {
    //        Transaction?.Dispose();
    //    }
    //}
    public class UserDefineTableService : IDisposable
    {
        // 預設資料庫名稱還是 comets
        private static readonly string DefaultDbName = "comets";

        private readonly string _dbName;              // 實際使用的資料庫名稱
        private readonly string connectionString;     // 資料庫連線字串

        public IFXTransaction Transaction { get; private set; }

        /// <summary>
        /// 預設建構子：使用 comets 資料庫
        /// </summary>
        public UserDefineTableService(bool isDecrypt = false)
            : this(null, isDecrypt)
        {
        }

        /// <summary>
        /// 可指定資料庫名稱的建構子。
        /// dbName 為 null 或空字串時，仍使用預設 comets。
        /// 例如：new UserDefineTableService("cimes") 會連到 cimes。
        /// </summary>
        public UserDefineTableService(string dbName, bool isDecrypt = false)
        {
            _dbName = string.IsNullOrWhiteSpace(dbName) ? DefaultDbName : dbName;
            connectionString = ConnectionStrings.FromName(_dbName);
        }

        /// <summary>
        /// 開啟資料庫交易
        /// </summary>
        /// <returns>IFXTransaction</returns>
        public IFXTransaction BeginTransaction()
        {
            Transaction = new IFXTransaction(connectionString, IsolationLevel.ReadUncommitted);
            return Transaction;
        }

        /// <summary>
        /// 資源清理
        /// </summary>
        public void Dispose()
        {
            Transaction?.Dispose();
        }
    }
}

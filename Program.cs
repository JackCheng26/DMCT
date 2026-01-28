using System;
using System.Configuration;
using System.Threading;
using System.Threading.Tasks;
using Oracle.ManagedDataAccess.Client;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using System.Reflection;
using System.IO;
using System.Collections.Concurrent;
using System.Text;
using System.Windows.Forms;  
public static class DictionaryExtensions
{
    public static TValue GetValueOrDefault<TKey, TValue>(this Dictionary<TKey, TValue> dictionary, TKey key, TValue defaultValue = default(TValue))
    {
        if (dictionary == null) throw new ArgumentNullException(nameof(dictionary));
        return dictionary.ContainsKey(key) ? dictionary[key] : defaultValue;
    }
}

namespace DataSyncService
{
    class Program
    {
        private static bool _isRunning = true;
        private static readonly string LogDirectory = "Logs";
        private static readonly string LogFileName = $"DataSync_{DateTime.Now:yyyyMMdd}.log";
        private static readonly string LogFilePath = Path.Combine(LogDirectory, LogFileName);
        private static ConcurrentDictionary<string, string> _taskLogFiles = new ConcurrentDictionary<string, string>();
        private static ConcurrentDictionary<string, (DateTime eventTime, string taskCounter, decimal oxygenFlow, decimal ca13Wgt, string msgId, bool isActive)> _latestDataCache =
            new ConcurrentDictionary<string, (DateTime, string, decimal, decimal, string, bool)>();
        private static ConcurrentDictionary<string, bool> _taskActiveStatus = new ConcurrentDictionary<string, bool>();
        private static ConcurrentDictionary<long, (string taskCounter, DateTime markTime)> _pendingUpdates =
            new ConcurrentDictionary<long, (string, DateTime)>();
        private static ConcurrentDictionary<string, decimal> _slagHeatEffectCache = new ConcurrentDictionary<string, decimal>();
        private static ConcurrentDictionary<string, bool> _tappingStartedCache = new ConcurrentDictionary<string, bool>();
        private static ConcurrentDictionary<string, WaitingCoolingProcess> _waitingCoolingCache = new ConcurrentDictionary<string, WaitingCoolingProcess>();
        public static ConcurrentDictionary<string, bool> _chartWindowsShown = new ConcurrentDictionary<string, bool>();
        private static ConcurrentDictionary<string, DateTime?> _firstBlowingTimeCache = new ConcurrentDictionary<string, DateTime?>();
        private static DateTime _lastWriteTime = DateTime.MinValue;
        private static readonly TimeSpan WriteInterval = TimeSpan.FromSeconds(5);
        private static string _currentSchema;
        private static decimal _oxygenFlowThreshold = 200m;
        private static decimal _lanceHeightThreshold = 400m;
        private static int _temperatureMeasurementDelay = 1;
        private static decimal _tiltAngleThreshold = 85m;

        private static async Task PreloadSlagHeatEffects(OracleConnection connection, OracleTransaction transaction)
        {
            try
            {
                foreach (var scrapType in ScrapMaterialMap.Keys)
                {
                    decimal heatEffect = await GetSlagHeatEffect(connection, transaction, scrapType);
                    _slagHeatEffectCache[scrapType] = heatEffect;
                }
                LogInfo($"预加载炉渣热效应值完成，共{_slagHeatEffectCache.Count}种废钢类型");
            }
            catch (Exception ex)
            {
                LogError($"预加载炉渣热效应值失败: {ex.Message}");
            }
        }

        private readonly struct CoolantMaterialInfo
        {
            public string EffectChar { get; }
            public string MeltingRateChar { get; }

            public CoolantMaterialInfo(string effectChar, string meltingRateChar)
            {
                EffectChar = effectChar;
                MeltingRateChar = meltingRateChar;
            }
        }

        private readonly struct PresetMaterialInfo
        {
            public string MaterialType { get; }
            public int SdmIdx { get; }

            public PresetMaterialInfo(string materialType, int sdmIdx)
            {
                MaterialType = materialType;
                SdmIdx = sdmIdx;
            }
        }

        private readonly struct ScrapMaterialInfo
        {
            public string MaterialId { get; }
            public decimal MeltingRate { get; }
            public string WeightColumn { get; }

            public ScrapMaterialInfo(string materialId, decimal meltingRate, string weightColumn)
            {
                MaterialId = materialId;
                MeltingRate = meltingRate;
                WeightColumn = weightColumn;
            }
        }

        private static readonly Dictionary<int, CoolantMaterialInfo> CoolantMaterialMap =
            new Dictionary<int, CoolantMaterialInfo>
        {
            {101, new CoolantMaterialInfo("SHSBL", "DisrBlim")},
            {103, new CoolantMaterialInfo("SHSDL", "DisrDoli")},
            {105, new CoolantMaterialInfo("SHSLS", "DisrList")},
            {106, new CoolantMaterialInfo("SHSRD", "DisrRdol")},
            {107, new CoolantMaterialInfo("SHSRS", "DisrSlag")},
            {108, new CoolantMaterialInfo("SHSOR", "DisrOre")},
            {109, new CoolantMaterialInfo("SHSMO", "DisrMnor")},
            {112, new CoolantMaterialInfo("SHSR1", "DisrRes1")},
            {113, new CoolantMaterialInfo("SHSR2", "DisrRes2")}
        };

        private static readonly Dictionary<int, PresetMaterialInfo> PresetToMaterialMap = new Dictionary<int, PresetMaterialInfo>
        {
            {0, new PresetMaterialInfo("SHSOR", 108)},
            {1, new PresetMaterialInfo("SHSMO", 109)},
            {2, new PresetMaterialInfo("SHSRD", 106)},
            {3, new PresetMaterialInfo("SHSLS", 105)},
            {4, new PresetMaterialInfo("SHSR1", 112)},
            {5, new PresetMaterialInfo("SHSR2", 113)}
        };

        private static readonly Dictionary<string, ScrapMaterialInfo> ScrapMaterialMap = new Dictionary<string, ScrapMaterialInfo>
{
    {"SC1", new ScrapMaterialInfo("SC1", GetScrapMeltingRate("SC1"), TableConfig.ScrapWeight1Column)},
    {"SC2", new ScrapMaterialInfo("SC2", GetScrapMeltingRate("SC2"), TableConfig.ScrapWeight2Column)},
    {"SC3", new ScrapMaterialInfo("SC3", GetScrapMeltingRate("SC3"), TableConfig.ScrapWeight3Column)},
    {"SC4", new ScrapMaterialInfo("SC4", GetScrapMeltingRate("SC4"), TableConfig.ScrapWeight4Column)},
    {"SC5", new ScrapMaterialInfo("SC5", GetScrapMeltingRate("SC5"), TableConfig.ScrapWeight5Column)},
    {"SC6", new ScrapMaterialInfo("SC6", GetScrapMeltingRate("SC6"), TableConfig.ScrapWeight6Column)},
    {"SC7", new ScrapMaterialInfo("SC7", GetScrapMeltingRate("SC7"), TableConfig.ScrapWeight7Column)},
    {"SC8", new ScrapMaterialInfo("SC8", GetScrapMeltingRate("SC8"), TableConfig.ScrapWeight8Column)},
    {"SC9", new ScrapMaterialInfo("SC9", GetScrapMeltingRate("SC9"), TableConfig.ScrapWeight9Column)},
    {"SC10", new ScrapMaterialInfo("SC10", GetScrapMeltingRate("SC10"), TableConfig.ScrapWeight10Column)},
    {"SC11", new ScrapMaterialInfo("SC11", GetScrapMeltingRate("SC11"), TableConfig.ScrapWeight11Column)},
    {"SC12", new ScrapMaterialInfo("SC12", GetScrapMeltingRate("SC12"), TableConfig.ScrapWeight12Column)},
    {"SC13", new ScrapMaterialInfo("SC13", GetScrapMeltingRate("SC13"), TableConfig.ScrapWeight13Column)},
    {"SC14", new ScrapMaterialInfo("SC14", GetScrapMeltingRate("SC14"), TableConfig.ScrapWeight14Column)},
    {"SC15", new ScrapMaterialInfo("SC15", GetScrapMeltingRate("SC15"), TableConfig.ScrapWeight15Column)}
};

        private static decimal GetScrapMeltingRate(string scrapType)
        {
            try
            {
                var furnaceSettings = ConfigurationManager.GetSection("furnaceSettings") as NameValueCollection;
                if (furnaceSettings == null)
                {
                    return 50m;
                }

                string key = $"{scrapType}_MeltingRate";
                string value = furnaceSettings[key];

                if (!string.IsNullOrEmpty(value) && decimal.TryParse(value, out decimal meltingRate))
                {
                    return meltingRate;
                }

                string defaultValue = furnaceSettings["DefaultScrapMeltingRate"];
                if (!string.IsNullOrEmpty(defaultValue) && decimal.TryParse(defaultValue, out decimal defaultRate))
                {
                    return defaultRate;
                }

                return 50m;
            }
            catch (Exception ex)
            {
                LogError($"获取废钢{scrapType}熔化速率失败: {ex.Message}，使用默认值50kg/s");
                return 50m;
            }
        }

        private static ConcurrentDictionary<string, LowTempCoolingProcess> _lowTempProcessCache = new ConcurrentDictionary<string, LowTempCoolingProcess>();
        private static ConcurrentDictionary<string, HighTempProcess> _highTempProcessCache = new ConcurrentDictionary<string, HighTempProcess>();

        private static decimal _waitingCoolingCoefficient = 2.0m;
        private static decimal _tiltCoolingCoefficient = 10.0m;
        private static decimal _steelSpecificHeat = 0.715m;
        private static decimal _slagSpecificHeat = 1.191m;
        private static bool _configCacheInitialized = false;

        private class WaitingCoolingProcess
        {
            public string TaskCounter { get; set; }
            public DateTime WaitingStartTime { get; set; }
            public DateTime LastCalculationTime { get; set; }
            public decimal TotalWaitingCooling { get; set; } = 0m;
            public bool IsInWaitingPeriod { get; set; } = false;
            public decimal WaitingCoolingCoefficient { get; set; } = 0m;
            public decimal LastTemperature { get; set; } = 0m;
            public bool TiltCoolingCalculated { get; set; } = false;
            public decimal TiltCooling { get; set; } = 0m;
            public decimal LastScrapCoolingBeforeWaiting { get; set; } = 0m;
            public decimal WaitingEndTemperature { get; set; } = 0m;
            public bool HasResumedFromWaiting { get; set; } = false;
                                 
            
                    }

        private class LowTempCoolingProcess
        {
            public string TaskCounter { get; set; }
            public decimal BaseTemperature { get; set; }
            public DateTime StartTime { get; set; }

                        
                        public decimal InProgressScrapCooling { get; set; } = 0m;
            public decimal InProgressCoolantCooling { get; set; } = 0m;
            public decimal InProgressOtherCooling { get; set; } = 0m;

                        public decimal TotalCompletedCoolantCooling { get; set; } = 0m;
            public decimal TotalCompletedOtherCooling { get; set; } = 0m;

            public decimal TotalCompletedCooling { get; set; } = 0m;
            public decimal TotalProcessedWeight { get; set; }
            public List<CoolantBatch> CoolantBatches { get; set; } = new List<CoolantBatch>();
            public List<MaterialBatch> OtherMaterialBatches { get; set; } = new List<MaterialBatch>();
            public List<ScrapBatch> ScrapBatches { get; set; } = new List<ScrapBatch>();
            public Dictionary<string, decimal> MaterialTotalWeights { get; set; } = new Dictionary<string, decimal>();
            public Dictionary<string, decimal> ScrapTotalWeights { get; set; } = new Dictionary<string, decimal>();
            public decimal TotalScrapCooling { get; set; } = 0m;          }

        private class CoolantBatch
        {
            public string MaterialId { get; set; }
            public decimal IncrementWeight { get; set; }
            public decimal TotalCooling { get; set; }
            public decimal Duration { get; set; }
            public DateTime StartTime { get; set; }
            public bool IsCompleted => (DateTime.Now - StartTime).TotalSeconds >= (double)Duration;
        }

        private class HighTempProcess
        {
            public string TaskCounter { get; set; }
            public decimal BaseTemperature { get; set; }
            public DateTime StartTime { get; set; }
            public List<MaterialBatch> MaterialBatches { get; set; } = new List<MaterialBatch>();
            public decimal TotalProcessedWeight { get; set; } = 0m;
            public decimal TotalCompletedCooling { get; set; } = 0m;

                        public decimal LastOxygenHeatingCoefficient { get; set; } = 0m;
            public DateTime LastCoefficientCalcTime { get; set; } = DateTime.MinValue;
            public bool HasValidCoefficient => LastOxygenHeatingCoefficient > 0 &&
                                              (DateTime.Now - LastCoefficientCalcTime).TotalMinutes < 30;
                    }

        private class MaterialBatch
        {
            public string MaterialId { get; set; }
            public decimal IncrementWeight { get; set; }
            public decimal TotalCooling { get; set; }
            public decimal Duration { get; set; }
            public DateTime StartTime { get; set; }
            public bool IsCompleted => (DateTime.Now - StartTime).TotalSeconds >= (double)Duration;
        }

        private class ScrapBatch
        {
            public string MaterialId { get; set; }
            public decimal Weight { get; set; }
            public decimal TotalCooling { get; set; }                  public decimal Duration { get; set; }                      public DateTime StartTime { get; set; }        
            public decimal GetProgressRatio(DateTime currentTime)
            {
                double elapsedSeconds = Math.Max(0, (currentTime - StartTime).TotalSeconds);
                double durationSeconds = (double)Duration;

                if (durationSeconds <= 0) return 0m;

                double progressRatio = Math.Min(elapsedSeconds / durationSeconds, 1.0);
                return (decimal)progressRatio;
            }

            public decimal GetCurrentActualCooling(DateTime currentTime)
            {
                decimal progress = GetProgressRatio(currentTime);
                return TotalCooling * progress;
            }

                        
                        public bool IsCompleted => false;         }

        private class MaterialData
        {
            public string MaterialId { get; set; }
            public string MaterialName { get; set; }
            public string MaterialType { get; set; }
            public decimal CalcWeight { get; set; }
            public decimal ActualWeight { get; set; }
            public decimal CalcCooling { get; set; }
            public decimal ActualCooling { get; set; }
            public decimal MeltingRate { get; set; }
        }

        private class AllMaterialData
        {
            public MaterialData MainCoolantData { get; set; }
            public List<MaterialData> OtherMaterials { get; set; } = new List<MaterialData>();
        }
        static void Main(string[] args)
        {
                        Application.EnableVisualStyles();
            Application.SetCompatibleTextRenderingDefault(false);

                        FixConsoleEncoding();

                        InitializeLogging();

                        LogInfo("程序启动，执行初始日志清理...");
            CleanupOldLogFiles();

                        AppDomain.CurrentDomain.AssemblyResolve += CurrentDomain_AssemblyResolve;

            Console.WriteLine("数据同步服务启动...");
            Console.WriteLine("按 'q' 键停止服务");
            Console.WriteLine($"日志保留策略: 自动清理3天前的日志文件");

                        var stopEvent = new ManualResetEvent(false);

            try
            {
                                if (!InitializeFurnaceConfiguration())
                {
                    LogError("炉座配置初始化失败！");
                    Console.WriteLine("炉座配置初始化失败！");
                    Console.WriteLine("按任意键退出...");
                    Console.ReadKey();
                    return;
                }

                                if (!DatabaseConfig.ValidateConfiguration())
                {
                    LogError("数据库连接配置无效，请检查配置文件！");
                    Console.WriteLine("数据库连接配置无效，请检查配置文件！");
                    Console.WriteLine("按任意键退出...");
                    Console.ReadKey();
                    return;
                }

                                _ = Task.Run(PreloadConfigurationCache);

                                var syncTask = Task.Run(async () => await StartRealTimeSync());

                                var logCleanupTask = Task.Run(async () =>
                {
                    while (_isRunning)
                    {
                        try
                        {
                                                        DateTime now = DateTime.Now;
                            if (now.Hour == 3 && now.Minute == 0)
                            {
                                LogInfo("定时任务：开始执行每日日志清理");
                                CleanupOldLogFiles();

                                                                await Task.Delay(TimeSpan.FromHours(1));
                            }
                            else
                            {
                                                                await Task.Delay(TimeSpan.FromHours(1));
                            }
                        }
                        catch (Exception ex)
                        {
                            LogError($"日志清理任务失败: {ex.Message}", ex);
                            await Task.Delay(TimeSpan.FromMinutes(5));                         }
                    }
                });

                                var messageLoopTask = Task.Run(() =>
                {
                                        using (var hiddenForm = new Form())
                    {
                        hiddenForm.ShowInTaskbar = false;
                        hiddenForm.WindowState = FormWindowState.Minimized;
                        hiddenForm.Visible = false;

                        Application.Run(hiddenForm);
                    }
                });

                Console.WriteLine("服务运行中，按 'q' 键停止...");
                Console.WriteLine($"当前炉座: {_currentSchema}");
                Console.WriteLine($"氧气阈值: {_oxygenFlowThreshold}m³/h, 枪位阈值: {_lanceHeightThreshold}mm");
                Console.WriteLine($"日志目录: {Path.GetFullPath(LogDirectory)}");

                                Task.Run(() =>
                {
                    while (_isRunning)
                    {
                        if (Console.KeyAvailable)
                        {
                            var key = Console.ReadKey(intercept: true);
                            if (key.KeyChar == 'q' || key.KeyChar == 'Q')
                            {
                                _isRunning = false;
                                Console.WriteLine("正在停止服务...");

                                                                try
                                {
                                    ChartFormManager.CloseAllForms();
                                    Console.WriteLine("已关闭所有监控弹窗");
                                }
                                catch (Exception ex)
                                {
                                    Console.WriteLine($"关闭弹窗时出错: {ex.Message}");
                                }

                                                                Application.Exit();

                                                                stopEvent.Set();
                                break;
                            }
                        }
                        Thread.Sleep(100);
                    }
                });

                                stopEvent.WaitOne();

                                Console.WriteLine("正在停止数据同步任务...");
                syncTask.Wait(TimeSpan.FromSeconds(10));

                Console.WriteLine("正在停止日志清理任务...");
                logCleanupTask.Wait(TimeSpan.FromSeconds(5));

                Console.WriteLine("正在停止消息循环...");
                messageLoopTask.Wait(TimeSpan.FromSeconds(5));
            }
            catch (Exception ex)
            {
                LogError($"程序启动失败: {ex.Message}", ex);
                Console.WriteLine($"程序启动失败: {ex.Message}");
                Console.WriteLine("按任意键退出...");
                Console.ReadKey();
            }

                        try
            {
                _taskLogFiles.Clear();
                LogInfo("=== 数据同步服务停止 ===");
            }
            catch
            {
                            }

            Console.WriteLine("数据同步服务已停止");
            Console.WriteLine("按任意键退出...");
            Console.ReadKey();
        }

        private static async Task PreloadConfigurationCache()
        {
            OracleConnection connection = null;
            try
            {
                connection = new OracleConnection(DatabaseConfig.GetConnectionString());
                await connection.OpenAsync();

                using (var transaction = connection.BeginTransaction())
                {
                    _waitingCoolingCoefficient = await GetWaitingCoolingCoefficient(connection, transaction);
                    _tiltCoolingCoefficient = await GetTiltCoolingCoefficient(connection, transaction);
                    _steelSpecificHeat = await GetSteelSpecificHeat(connection, transaction);
                    _slagSpecificHeat = await GetSlagSpecificHeat(connection, transaction);

                    await PreloadSlagHeatEffects(connection, transaction);

                    transaction.Commit();
                    _configCacheInitialized = true;
                                        
                }
            }
            catch (Exception ex)
            {
                LogError($"预加载配置缓存失败: {ex.Message}");
            }
            finally
            {
                connection?.Close();
                connection?.Dispose();
            }
        }

        private static async Task<decimal> GetCachedWaitingCoolingCoefficient(OracleConnection connection, OracleTransaction transaction)
        {
            if (_configCacheInitialized) return _waitingCoolingCoefficient;
            return await GetWaitingCoolingCoefficient(connection, transaction);
        }

        private static async Task<decimal> GetCachedTiltCoolingCoefficient(OracleConnection connection, OracleTransaction transaction)
        {
            if (_configCacheInitialized) return _tiltCoolingCoefficient;
            return await GetTiltCoolingCoefficient(connection, transaction);
        }

        private static async Task StartRealTimeSync()
        {
            string connectionString = DatabaseConfig.GetConnectionString();

            while (_isRunning)
            {
                try
                {
                    await SyncCurrentTimeData(connectionString);
                    await Task.Delay(1000);
                }
                catch (Exception ex)
                {
                    LogError($"同步过程中发生错误: {ex.Message}", ex);
                    await Task.Delay(5000);
                }
            }
        }
        private static async Task<decimal> GetWaitingCoolingCoefficient(OracleConnection connection, OracleTransaction transaction)
        {
            try
            {
                string sql = $@"
                    SELECT {TableConfig.FloatValueColumn} 
                    FROM {GetTableName(TableConfig.AuxValuesTable)} 
                    WHERE {TableConfig.CharValueColumn} = 'TLWT0'";

                using (var command = new OracleCommand(sql, connection))
                {
                    command.Transaction = transaction;

                    var result = await command.ExecuteScalarAsync();
                    if (result != null && result != DBNull.Value)
                    {
                        decimal coefficient = Convert.ToDecimal(result);
                        return coefficient;
                    }

                    LogWarning("未找到等待温降系数配置，使用默认值 2.0℃/分钟");
                    return 2.0m;
                }
            }
            catch (Exception ex)
            {
                LogError($"获取等待温降系数失败: {ex.Message}");
                return 2.0m;
            }
        }

        private static async Task<decimal> GetTiltCoolingCoefficient(OracleConnection connection, OracleTransaction transaction)
        {
            try
            {
                string sql = $@"
                    SELECT {TableConfig.FloatValueColumn} 
                    FROM {GetTableName(TableConfig.AuxValuesTable)} 
                    WHERE {TableConfig.CharValueColumn} = 'TLTILT'";

                using (var command = new OracleCommand(sql, connection))
                {
                    command.Transaction = transaction;

                    var result = await command.ExecuteScalarAsync();
                    if (result != null && result != DBNull.Value)
                    {
                        decimal coefficient = Convert.ToDecimal(result);
                        return coefficient;
                    }

                    LogWarning("未找到倒炉温降系数配置，使用默认值 10.0℃");
                    return 10.0m;
                }
            }
            catch (Exception ex)
            {
                LogError($"获取倒炉温降系数失败: {ex.Message}");
                return 10.0m;
            }
        }

        private static async Task<decimal> GetCurrentLanceHeight(OracleConnection connection, OracleTransaction transaction, string taskCounter)
        {
            try
            {
                string sql = $@"
                    SELECT {TableConfig.LanceHeightColumn} 
                    FROM {GetTableName(TableConfig.TrendsDataTable)} 
                    WHERE {TableConfig.TaskCounterColumn} = :taskCounter 
                      AND {TableConfig.LanceHeightColumn} IS NOT NULL 
                    ORDER BY {TableConfig.EventTimeColumn} DESC 
                    FETCH FIRST 1 ROW ONLY";

                using (var command = new OracleCommand(sql, connection))
                {
                    command.Transaction = transaction;
                    command.Parameters.Add(new OracleParameter("taskCounter", taskCounter));

                    var result = await command.ExecuteScalarAsync();
                    return result != null && result != DBNull.Value ? Convert.ToDecimal(result) : 0m;
                }
            }
            catch (Exception ex)
            {
                LogError($"获取枪位高度失败: {ex.Message}");
                return 0m;
            }
        }

        private static async Task<decimal> GetCurrentConverterAngle(OracleConnection connection, OracleTransaction transaction, string taskCounter)
        {
            try
            {
                string sql = $@"
                    SELECT {TableConfig.ConverterAngleColumn} 
                    FROM {GetTableName(TableConfig.TrendsDataTable)} 
                    WHERE {TableConfig.TaskCounterColumn} = :taskCounter 
                      AND {TableConfig.ConverterAngleColumn} IS NOT NULL 
                    ORDER BY {TableConfig.EventTimeColumn} DESC 
                    FETCH FIRST 1 ROW ONLY";

                using (var command = new OracleCommand(sql, connection))
                {
                    command.Transaction = transaction;
                    command.Parameters.Add(new OracleParameter("taskCounter", taskCounter));

                    var result = await command.ExecuteScalarAsync();
                    return result != null && result != DBNull.Value ? Convert.ToDecimal(result) : 0m;
                }
            }
            catch (Exception ex)
            {
                LogError($"获取转炉角度失败: {ex.Message}");
                return 0m;
            }
        }

        private static async Task<bool> CheckWaitingPeriod(
    OracleConnection connection, OracleTransaction transaction,
    string taskCounter, decimal currentOxygenFlow)
        {
            try
            {
                                decimal latestOxygenFlow = await GetCurrentOxygenFlow(connection, transaction, taskCounter);

                if (latestOxygenFlow >= _oxygenFlowThreshold)
                {
                    LogInfo($"检查等待期: 任务={taskCounter}, 最新氧流量={latestOxygenFlow:F1}≥{_oxygenFlowThreshold}, 不是等待期", taskCounter);
                    return false;
                }

                decimal lanceHeight = await GetCurrentLanceHeight(connection, transaction, taskCounter);
                bool isLanceHigh = lanceHeight > _lanceHeightThreshold;

                if (isLanceHigh)
                {
                    LogInfo($"检查等待期: 任务={taskCounter}, 最新氧流量={latestOxygenFlow:F1}<{_oxygenFlowThreshold}, 枪位={lanceHeight:F1}>{_lanceHeightThreshold}, 是等待期", taskCounter);
                    return true;
                }

                LogInfo($"检查等待期: 任务={taskCounter}, 最新氧流量={latestOxygenFlow:F1}<{_oxygenFlowThreshold}, 枪位={lanceHeight:F1}≤{_lanceHeightThreshold}, 不是等待期", taskCounter);
                return false;
            }
            catch (Exception ex)
            {
                LogError($"检查等待期条件失败: {ex.Message}");
                return false;
            }
        }

        private static async Task<decimal> GetCurrentOxygenFlow(OracleConnection connection, OracleTransaction transaction, string taskCounter)
        {
            try
            {
                string sql = $@"
            SELECT {TableConfig.OxygenFlowColumn} 
            FROM {GetTableName(TableConfig.TrendsDataTable)} 
            WHERE {TableConfig.TaskCounterColumn} = :taskCounter 
              AND {TableConfig.OxygenFlowColumn} IS NOT NULL 
            ORDER BY {TableConfig.EventTimeColumn} DESC 
            FETCH FIRST 1 ROW ONLY";

                using (var command = new OracleCommand(sql, connection))
                {
                    command.Transaction = transaction;
                    command.Parameters.Add(new OracleParameter("taskCounter", taskCounter));

                    var result = await command.ExecuteScalarAsync();
                    return result != null && result != DBNull.Value ? Convert.ToDecimal(result) : 0m;
                }
            }
            catch (Exception ex)
            {
                LogError($"获取最新氧流量失败: {ex.Message}");
                return 0m;
            }
        }

        private static async Task<bool> CheckTiltEvent(
    OracleConnection connection, OracleTransaction transaction,
    string taskCounter, WaitingCoolingProcess waitingProcess)
        {
            try
            {
                                if (!waitingProcess.IsInWaitingPeriod)
                {
                    return false;
                }

                if (waitingProcess.TiltCoolingCalculated)
                {
                    return false;
                }

                decimal currentAngle = await GetCurrentConverterAngle(connection, transaction, taskCounter);

                if (currentAngle >= _tiltAngleThreshold)
                {
                    decimal tiltCoolingCoefficient = await GetCachedTiltCoolingCoefficient(connection, transaction);
                    waitingProcess.TiltCooling = tiltCoolingCoefficient;
                    waitingProcess.TiltCoolingCalculated = true;

                    LogInfo($"检测到倒炉事件: 任务={taskCounter}, 角度={currentAngle:F2}°≥{_tiltAngleThreshold}°, 倒炉温降={tiltCoolingCoefficient:F2}℃");
                    return true;
                }

                return false;
            }
            catch (Exception ex)
            {
                LogError($"检查倒炉事件失败: {ex.Message}");
                return false;
            }
        }
        private static async Task<(decimal intervalCooling, decimal tiltCooling, decimal totalWaitingCooling)> ProcessWaitingAndTiltCooling(
    OracleConnection connection, OracleTransaction transaction,
    string taskCounter)
        {
            try
            {
                                decimal waitingCoefficient = await GetWaitingCoolingCoefficient(connection, transaction);

                if (!_waitingCoolingCache.TryGetValue(taskCounter, out var waitingProcess))
                {
                    waitingProcess = new WaitingCoolingProcess
                    {
                        TaskCounter = taskCounter,
                        WaitingStartTime = DateTime.Now,
                        LastCalculationTime = DateTime.Now,
                        WaitingCoolingCoefficient = waitingCoefficient,
                        IsInWaitingPeriod = true,
                        LastTemperature = 0m,
                        TiltCoolingCalculated = false,
                        TiltCooling = 0m
                    };
                    _waitingCoolingCache[taskCounter] = waitingProcess;

                    LogInfo($"开始等待温降计算: 任务={taskCounter}, 系数={waitingCoefficient:F2}℃/分钟", taskCounter);
                    return (0m, 0m, 0m);
                }

                if (!waitingProcess.IsInWaitingPeriod)
                {
                    waitingProcess.WaitingStartTime = DateTime.Now;
                    waitingProcess.LastCalculationTime = DateTime.Now;
                    waitingProcess.IsInWaitingPeriod = true;
                    waitingProcess.LastTemperature = 0m;
                    waitingProcess.WaitingCoolingCoefficient = waitingCoefficient;

                    LogInfo($"重新开始等待温降计算: 任务={taskCounter}, 保留倒炉温降={waitingProcess.TiltCooling:F2}℃", taskCounter);
                }

                decimal intervalCooling = 0m;
                decimal tiltCooling = 0m;

                DateTime currentTime = DateTime.Now;
                double intervalMinutes = (currentTime - waitingProcess.LastCalculationTime).TotalMinutes;

                intervalCooling = waitingProcess.WaitingCoolingCoefficient * (decimal)intervalMinutes;

                if (intervalCooling > 0)
                {
                    decimal previousTotal = waitingProcess.TotalWaitingCooling;
                    waitingProcess.TotalWaitingCooling += intervalCooling;

                    LogInfo($"等待温降增加: 任务={taskCounter}, 时间间隔={intervalMinutes:F2}分钟, " +
                           $"本次温降={intervalCooling:F2}℃, 累计温降={waitingProcess.TotalWaitingCooling:F2}℃ " +
                           $"(前次累计={previousTotal:F2}℃)", taskCounter);
                }

                waitingProcess.LastCalculationTime = currentTime;

                                if (!waitingProcess.TiltCoolingCalculated && waitingProcess.IsInWaitingPeriod)
                {
                    bool hasTiltEvent = await CheckTiltEvent(connection, transaction, taskCounter, waitingProcess);
                    if (hasTiltEvent)
                    {
                        tiltCooling = waitingProcess.TiltCooling;
                        LogInfo($"倒炉温降增加: 任务={taskCounter}, 倒炉温降={tiltCooling:F2}℃", taskCounter);
                    }
                }
                else if (waitingProcess.TiltCoolingCalculated)
                {
                                        tiltCooling = waitingProcess.TiltCooling;
                    LogInfo($"使用已计算的倒炉温降: {tiltCooling:F2}℃", taskCounter);
                }

                return (intervalCooling, tiltCooling, waitingProcess.TotalWaitingCooling);
            }
            catch (Exception ex)
            {
                LogError($"处理等待温降失败: {ex.Message}");
                return (0m, 0m, 0m);
            }
        }

        private static void EndWaitingPeriod(string taskCounter, decimal currentTemperature = 0m)
        {
            if (_waitingCoolingCache.TryGetValue(taskCounter, out var waitingProcess) && waitingProcess.IsInWaitingPeriod)
            {
                waitingProcess.IsInWaitingPeriod = false;

                                if (currentTemperature > 0)
                {
                    waitingProcess.WaitingEndTemperature = currentTemperature;
                    LogInfo($"结束等待期: 任务={taskCounter}, 结束温度={currentTemperature:F1}℃, " +
                           $"累计等待温降={waitingProcess.TotalWaitingCooling:F2}℃");
                }
                else
                {
                    LogInfo($"结束等待期: 任务={taskCounter}, 未记录结束温度, " +
                           $"累计等待温降={waitingProcess.TotalWaitingCooling:F2}℃");
                }
            }
        }

        private static bool ShouldStopCalculation(string taskCounter)
        {
            if (_tappingStartedCache.ContainsKey(taskCounter))
            {
                return true;
            }

            if (_taskActiveStatus.TryGetValue(taskCounter, out bool isActive) && !isActive)
            {
                bool hasCache = _lowTempProcessCache.ContainsKey(taskCounter) || _highTempProcessCache.ContainsKey(taskCounter);
                return !hasCache;
            }

            return false;
        }

        private static async Task<decimal> GetTotalScrapCooling(
    OracleConnection connection, OracleTransaction transaction, string taskCounter)
        {
            try
            {
                decimal totalScrapCooling = 0m;

                                LowTempCoolingProcess process = null;
                if (_lowTempProcessCache.TryGetValue(taskCounter, out var tempProcess))
                {
                    process = tempProcess;
                }

                foreach (var scrapType in ScrapMaterialMap.Keys)
                {
                    decimal scrapCooling = await CalculateScrapCoolingEffect(
                        connection, transaction, taskCounter, scrapType);
                    totalScrapCooling += scrapCooling;

                                        if (process != null)
                    {
                        var scrapBatch = process.ScrapBatches
                            .FirstOrDefault(b => b.MaterialId == scrapType);

                        if (scrapBatch != null)
                        {
                            decimal oldTotalCooling = scrapBatch.TotalCooling;
                            scrapBatch.TotalCooling = scrapCooling;

                            if (Math.Abs(oldTotalCooling - scrapCooling) > 0.001m)
                            {
                                LogInfo($"废钢{scrapType}总温降更新: {oldTotalCooling:F3}→{scrapCooling:F3}℃", taskCounter);
                            }
                        }
                    }
                }

                return totalScrapCooling;
            }
            catch (Exception ex)
            {
                LogError($"获取废钢总温降失败: {ex.Message}");
                return 0m;
            }
        }

        private static async Task<decimal> GetScrapCoolingEffect(
    OracleConnection connection, OracleTransaction transaction,
    string taskCounter, string scrapType)
        {
            try
            {
                if (_slagHeatEffectCache.TryGetValue(scrapType, out decimal cachedHeatEffect))
                {
                }

                decimal calculatedEffect = await CalculateScrapCoolingEffect(connection, transaction, taskCounter, scrapType);
                if (calculatedEffect > 0)
                {
                    return calculatedEffect;
                }

                return 3100m;
            }
            catch (Exception ex)
            {
                LogError($"获取废钢{scrapType}冷却效应失败: {ex.Message}, 使用默认值 3100");
                return 3100m;
            }
        }

        private static async Task<(decimal slagPct, decimal slagHeatCapacity)> GetScrapProperties(
    OracleConnection connection, OracleTransaction transaction, string scrapType)
        {
            string sql = "";
            try
            {
                sql = $@"
            SELECT 
                T2.{TableConfig.ScrapSlagPctColumn},
                T3.{TableConfig.FloatValueColumn} as slag_heat_capacity
            FROM {TableConfig.ScrapMaterialTable} T2
            LEFT JOIN {GetTableName(TableConfig.AuxValuesTable)} T3 ON 
                T3.{TableConfig.CharValueColumn} = 'SHSLGSCR' || SUBSTR(T2.{TableConfig.ScrapMatIdColumn}, 3)
            WHERE T2.{TableConfig.ScrapMatIdColumn} = :scrapType";

                using (var command = new OracleCommand(sql, connection))
                {
                    command.Transaction = transaction;
                    command.Parameters.Add(new OracleParameter("scrapType", scrapType));

                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        if (await reader.ReadAsync())
                        {
                            decimal slagPct = reader.IsDBNull(0) ? 10m : reader.GetDecimal(0);
                            decimal slagHeatCapacity = reader.IsDBNull(1) ? 0m : reader.GetDecimal(1);
                            return (slagPct, slagHeatCapacity);
                        }
                    }
                }
                return (10m, 0m);
            }
            catch (Exception ex)
            {
                return (10m, 0m);
            }
        }

        private static async Task<decimal> GetTotalHeatCapacity(
    OracleConnection connection, OracleTransaction transaction, string taskCounter)
        {
            try
            {
                string sql = $@"
            SELECT 
                {TableConfig.SteelWeight1Column} * (SELECT {TableConfig.FloatValueColumn} FROM {GetTableName(TableConfig.AuxValuesTable)} WHERE {TableConfig.CharValueColumn}='SPHST') +
                {TableConfig.SlagWeight1Column} * (SELECT {TableConfig.FloatValueColumn} FROM {GetTableName(TableConfig.AuxValuesTable)} WHERE {TableConfig.CharValueColumn}='SPHSL')
            FROM {GetTableName(TableConfig.LogDataTable)} 
            WHERE {TableConfig.TaskCounterColumn} = :taskCounter 
              AND {TableConfig.SdmFunctionColumn} = 'BLOCAL' 
              AND {TableConfig.StepColumn} = 1";

                using (var command = new OracleCommand(sql, connection))
                {
                    command.Transaction = transaction;
                    command.Parameters.Add(new OracleParameter("taskCounter", taskCounter));

                    var result = await command.ExecuteScalarAsync();
                    if (result != null && result != DBNull.Value)
                    {
                        return Convert.ToDecimal(result);
                    }
                    return 1000m;
                }
            }
            catch (Exception ex)
            {
                return 1000m;
            }
        }

        private static async Task<decimal> CalculateScrapCoolingEffect(
    OracleConnection connection, OracleTransaction transaction,
    string taskCounter, string scrapType)
        {
            try
            {
                decimal scrapWeightTon = await GetScrapWeightByType(connection, transaction, taskCounter, scrapType);
                if (scrapWeightTon <= 0)
                {
                    return 0m;
                }

                (decimal slagPct, decimal slagHeatCapacity) = await GetScrapProperties(connection, transaction, scrapType);

                decimal slagHeatEffect = await GetSlagHeatEffect(connection, transaction, scrapType);

                decimal targetTemperature = await GetTargetTemperature(connection, transaction, taskCounter);
                decimal tempFactor = targetTemperature - 25m;

                decimal steelSpecificHeat = await GetSteelSpecificHeat(connection, transaction);

                decimal totalHeatCapacity = await GetTotalHeatCapacity(connection, transaction, taskCounter);

                decimal metalCoolingEffect = (100 - slagPct) / 100 * scrapWeightTon * steelSpecificHeat * tempFactor;

                decimal slagCoolingEffect = slagPct / 100 * scrapWeightTon * slagHeatEffect;

                decimal scrapCooling = (metalCoolingEffect + slagCoolingEffect) / totalHeatCapacity;

                LogInfo($"废钢{scrapType}温降计算: 重量={scrapWeightTon:F3}吨, 渣子比例={slagPct:F1}%, " +
                       $"金属效应={metalCoolingEffect:F1}kJ, 渣子效应={slagCoolingEffect:F1}kJ, " +
                       $"总热容={totalHeatCapacity:F1}kJ/℃, 温降={scrapCooling:F3}℃");

                return scrapCooling;
            }
            catch (Exception ex)
            {
                LogError($"计算废钢{scrapType}冷却效应失败: {ex.Message}");
                return 0m;
            }
        }

        private static async Task<decimal> GetScrapWeightByType(
    OracleConnection connection, OracleTransaction transaction,
    string taskCounter, string scrapType)
        {
            string sql = "";
            try
            {
                string weightColumn = scrapType switch
                {
                    "SC1" => TableConfig.ScrapWeight1Column,
                    "SC2" => TableConfig.ScrapWeight2Column,
                    "SC3" => TableConfig.ScrapWeight3Column,
                    "SC4" => TableConfig.ScrapWeight4Column,
                    "SC5" => TableConfig.ScrapWeight5Column,
                    "SC6" => TableConfig.ScrapWeight6Column,
                    "SC7" => TableConfig.ScrapWeight7Column,
                    "SC8" => TableConfig.ScrapWeight8Column,
                    "SC9" => TableConfig.ScrapWeight9Column,
                    "SC10" => TableConfig.ScrapWeight10Column,
                    "SC11" => TableConfig.ScrapWeight11Column,
                    "SC12" => TableConfig.ScrapWeight12Column,
                    "SC13" => TableConfig.ScrapWeight13Column,
                    "SC14" => TableConfig.ScrapWeight14Column,
                    "SC15" => TableConfig.ScrapWeight15Column,
                    _ => TableConfig.ScrapWeight1Column
                };

                sql = $@"
            SELECT NVL({weightColumn}, 0) 
            FROM {GetTableName(TableConfig.ScrapDataTable)}
            WHERE {TableConfig.TaskCounterColumn} = :taskCounter 
              AND {TableConfig.SdmFunctionColumn} = 'BLOCAL' 
              AND {TableConfig.StepColumn} = 1";

                using (var command = new OracleCommand(sql, connection))
                {
                    command.Transaction = transaction;
                    command.Parameters.Add(new OracleParameter("taskCounter", taskCounter));

                    var result = await command.ExecuteScalarAsync();
                    return result != null && result != DBNull.Value ? Convert.ToDecimal(result) : 0m;
                }
            }
            catch (Exception ex)
            {
                return 0m;
            }
        }

        
        private static async Task<Dictionary<string, decimal>> GetScrapWeights(
    OracleConnection connection, OracleTransaction transaction, string taskCounter)
        {
            var scrapWeights = new Dictionary<string, decimal>();

            try
            {
                string sql = $@"
            SELECT 
                {TableConfig.ScrapWeight1Column}, {TableConfig.ScrapWeight2Column}, {TableConfig.ScrapWeight3Column},
                {TableConfig.ScrapWeight4Column}, {TableConfig.ScrapWeight5Column}, {TableConfig.ScrapWeight6Column},
                {TableConfig.ScrapWeight7Column}, {TableConfig.ScrapWeight8Column}, {TableConfig.ScrapWeight9Column},
                {TableConfig.ScrapWeight10Column}, {TableConfig.ScrapWeight11Column}, {TableConfig.ScrapWeight12Column},
                {TableConfig.ScrapWeight13Column}, {TableConfig.ScrapWeight14Column}, {TableConfig.ScrapWeight15Column}
            FROM {GetTableName(TableConfig.ScrapDataTable)}
            WHERE {TableConfig.TaskCounterColumn} = :taskCounter 
              AND {TableConfig.SdmFunctionColumn} = 'BLOCAL' 
              AND {TableConfig.StepColumn} = 1";

                using (var command = new OracleCommand(sql, connection))
                {
                    command.Transaction = transaction;
                    command.Parameters.Add(new OracleParameter("taskCounter", taskCounter));

                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        if (await reader.ReadAsync())
                        {
                            for (int i = 1; i <= 15; i++)
                            {
                                string scrapKey = $"SC{i}";
                                decimal weightTon = reader.IsDBNull(i - 1) ? 0m : reader.GetDecimal(i - 1);
                                decimal weightKg = weightTon * 1000m;
                                scrapWeights[scrapKey] = weightKg;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
            }

            return scrapWeights;
        }

        private static async Task ProcessScrapIncrement(
    OracleConnection connection, OracleTransaction transaction,
    string taskCounter, LowTempCoolingProcess process, bool isInWaitingPeriod = false)
        {
            try
            {
                var currentScrapWeights = await GetScrapWeights(connection, transaction, taskCounter);
                decimal currentTotalScrapWeight = currentScrapWeights.Values.Sum();
                decimal previousTotalScrapWeight = process.ScrapTotalWeights.Values.Sum();

                LogInfo($"废钢重量检查 - 任务={taskCounter}: 当前={currentTotalScrapWeight:F1}kg, 之前={previousTotalScrapWeight:F1}kg", taskCounter);

                                bool needsReinitialization = false;
                string changeReason = "";

                                if (previousTotalScrapWeight == 0 && currentTotalScrapWeight > 0)
                {
                    needsReinitialization = true;
                    changeReason = $"首次初始化 (0kg → {currentTotalScrapWeight:F1}kg)";
                }
                                else if (previousTotalScrapWeight > 0 && currentTotalScrapWeight == 0)
                {
                    needsReinitialization = true;
                    changeReason = $"重量清零 ({previousTotalScrapWeight:F1}kg → 0kg)";
                }
                                else if (Math.Abs(currentTotalScrapWeight - previousTotalScrapWeight) > 100)
                {
                    needsReinitialization = true;
                    changeReason = $"显著变化 ({previousTotalScrapWeight:F1}kg → {currentTotalScrapWeight:F1}kg)";
                }

                if (needsReinitialization)
                {
                                        process.ScrapBatches.Clear();
                    process.ScrapTotalWeights.Clear();

                    if (currentTotalScrapWeight > 0)
                    {
                                                DateTime blowingStartTime = await GetBlowingStartTime(connection, transaction, taskCounter);
                        await ReinitializeScrapBatchesWithProgress(connection, transaction, taskCounter, process,
                            currentScrapWeights, blowingStartTime, DateTime.Now);
                    }
                }
                else if (process.ScrapBatches.Count == 0 && currentTotalScrapWeight > 0)
                {
                                        LogInfo($"废钢批次为空但重量>0，重新初始化", taskCounter);
                    await InitializeScrapBatches(connection, transaction, taskCounter, process, currentScrapWeights);
                }

                                if (currentTotalScrapWeight > 0)
                {
                                        if (process.TotalScrapCooling <= 0)
                    {
                        process.TotalScrapCooling = await GetTotalScrapCooling(connection, transaction, taskCounter);
                        LogInfo($"更新废钢总计算温降: {process.TotalScrapCooling:F3}℃", taskCounter);
                    }
                }
            }
            catch (Exception ex)
            {
                string periodType = isInWaitingPeriod ? "等待期" : "正常期";
                LogError($"{periodType}处理废钢增量失败: {ex.Message}", null, taskCounter);
            }
        }

        
        private static async Task ReinitializeScrapBatchesWithProgress(
    OracleConnection connection, OracleTransaction transaction,
    string taskCounter, LowTempCoolingProcess process,
    Dictionary<string, decimal> currentScrapWeights, DateTime blowingStartTime, DateTime currentTime)
        {
            LogInfo($"基于进度重新初始化废钢批次 - 任务={taskCounter}:", taskCounter);
            LogInfo($"  当前废钢总重: {currentScrapWeights.Values.Sum():F1}kg", taskCounter);
            LogInfo($"  开吹时间: {blowingStartTime:HH:mm:ss}", taskCounter);
            LogInfo($"  当前时间: {currentTime:HH:mm:ss}", taskCounter);
            LogInfo($"  已过时间: {(currentTime - blowingStartTime).TotalMinutes:F1}分钟", taskCounter);

                        
            foreach (var scrapItem in currentScrapWeights.Where(kv => kv.Value > 0))
            {
                string scrapKey = scrapItem.Key;
                decimal currentWeightKg = scrapItem.Value;

                decimal meltingRate = ScrapMaterialMap[scrapKey].MeltingRate;
                decimal totalDuration = currentWeightKg / meltingRate;
                decimal totalScrapCoolingEffect = await GetScrapCoolingEffect(connection, transaction, taskCounter, scrapKey);

                var batch = new ScrapBatch
                {
                    MaterialId = scrapKey,
                    Weight = currentWeightKg,
                    TotalCooling = totalScrapCoolingEffect,
                    Duration = totalDuration,
                    StartTime = blowingStartTime                  };

                process.ScrapBatches.Add(batch);
                process.ScrapTotalWeights[scrapKey] = currentWeightKg;

                                decimal currentProgress = batch.GetProgressRatio(currentTime);
                decimal currentActualCooling = batch.GetCurrentActualCooling(currentTime);

                LogInfo($"废钢批次[{scrapKey}]重新初始化:", taskCounter);
                LogInfo($"  - 重量: {currentWeightKg:F1}kg, 总温降: {totalScrapCoolingEffect:F3}℃", taskCounter);
                LogInfo($"  - 熔化速率: {meltingRate:F1}kg/s, 总时长: {totalDuration:F1}秒", taskCounter);
                LogInfo($"  - 当前进度: {currentProgress:P1}, 已过时间: {(currentTime - blowingStartTime).TotalSeconds:F1}秒", taskCounter);
                LogInfo($"  - 当前实际温降: {currentActualCooling:F3}℃", taskCounter);
            }

            process.TotalScrapCooling = await GetTotalScrapCooling(connection, transaction, taskCounter);
            LogInfo($"废钢批次重新初始化完成:", taskCounter);
            LogInfo($"  - 总批次: {process.ScrapBatches.Count}", taskCounter);
            LogInfo($"  - 废钢总计算温降: {process.TotalScrapCooling:F3}℃", taskCounter);
            LogInfo($"  - 当前总实际温降: {await CalculateCurrentScrapCooling(process, currentTime):F3}℃", taskCounter);
        }

        private static async Task InitializeScrapBatches(
    OracleConnection connection, OracleTransaction transaction,
    string taskCounter, LowTempCoolingProcess process, Dictionary<string, decimal> currentScrapWeights)
        {
            DateTime blowingStartTime = await GetBlowingStartTime(connection, transaction, taskCounter);
            DateTime currentTime = DateTime.Now;

            LogInfo($"=== 初始化废钢批次 - 任务={taskCounter} ===", taskCounter);
            LogInfo($"开吹时间: {blowingStartTime:HH:mm:ss}", taskCounter);
            LogInfo($"当前时间: {currentTime:HH:mm:ss}", taskCounter);
            LogInfo($"总废钢重量: {currentScrapWeights.Values.Sum():F1}kg", taskCounter);

            foreach (var scrapItem in currentScrapWeights.Where(kv => kv.Value > 0))
            {
                string scrapKey = scrapItem.Key;
                decimal currentWeightKg = scrapItem.Value;

                decimal meltingRate = ScrapMaterialMap[scrapKey].MeltingRate;
                decimal totalDuration = currentWeightKg / meltingRate;
                decimal totalScrapCoolingEffect = await GetScrapCoolingEffect(connection, transaction, taskCounter, scrapKey);

                var batch = new ScrapBatch
                {
                    MaterialId = scrapKey,
                    Weight = currentWeightKg,
                    TotalCooling = totalScrapCoolingEffect,
                    Duration = totalDuration,
                    StartTime = blowingStartTime                  };

                process.ScrapBatches.Add(batch);
                process.ScrapTotalWeights[scrapKey] = currentWeightKg;

                                decimal initialProgress = batch.GetProgressRatio(currentTime);

                LogInfo($"废钢批次[{scrapKey}]初始化:", taskCounter);
                LogInfo($"  - 重量: {currentWeightKg:F1}kg, 熔化速率: {meltingRate:F1}kg/s", taskCounter);
                LogInfo($"  - 总时长: {totalDuration:F1}秒, 总温降: {totalScrapCoolingEffect:F3}℃", taskCounter);
                LogInfo($"  - 初始进度: {initialProgress:P1}, 初始温降: {batch.GetCurrentActualCooling(currentTime):F3}℃", taskCounter);
            }

                        process.TotalScrapCooling = await GetTotalScrapCooling(connection, transaction, taskCounter);

            LogInfo($"废钢批次初始化完成:", taskCounter);
            LogInfo($"  - 批次数量: {process.ScrapBatches.Count}", taskCounter);
            LogInfo($"  - 废钢总计算温降: {process.TotalScrapCooling:F3}℃", taskCounter);
            LogInfo($"  - 当前总实际温降: {await CalculateCurrentScrapCooling(process, currentTime):F3}℃", taskCounter);
            LogInfo($"=== 废钢批次初始化结束 ===", taskCounter);
        }

        private static async Task<DateTime> GetBlowingStartTime(OracleConnection connection, OracleTransaction transaction, string taskCounter)
        {
            try
            {
                                string trendSql = $@"
            SELECT MIN({TableConfig.EventTimeColumn})
            FROM {GetTableName(TableConfig.TrendsDataTable)}
            WHERE {TableConfig.TaskCounterColumn} = :taskCounter
            AND {TableConfig.OxygenFlowColumn} >= {_oxygenFlowThreshold}";

                using (var command = new OracleCommand(trendSql, connection))
                {
                    command.Transaction = transaction;
                    command.Parameters.Add(new OracleParameter("taskCounter", taskCounter));

                    var result = await command.ExecuteScalarAsync();
                    if (result != null && result != DBNull.Value)
                    {
                        DateTime blowingStartTime = Convert.ToDateTime(result);
                        LogInfo($"获取到开吹时间 - 任务={taskCounter}, 时间={blowingStartTime:HH:mm:ss}");
                        return blowingStartTime;
                    }
                }

                                if (_lowTempProcessCache.TryGetValue(taskCounter, out var process))
                {
                    LogWarning($"使用低温过程开始时间作为开吹时间 - 任务={taskCounter}, 时间={process.StartTime:HH:mm:ss}");
                    return process.StartTime;
                }

                LogWarning($"无法获取开吹时间，使用当前时间估算 - 任务={taskCounter}");
                return DateTime.Now;
            }
            catch (Exception ex)
            {
                LogError($"获取开吹时间失败: {ex.Message}");

                                return DateTime.Now;
            }
        }

        private static async Task<decimal> CalculateLowTempTemperature(
    OracleConnection connection, OracleTransaction transaction,
    string taskCounter, decimal currentOxygenFlow, decimal currentTotalOxygen)
        {
            try
            {
                if (ShouldStopCalculation(taskCounter))
                {
                    EndWaitingPeriod(taskCounter);
                    return 0;
                }

                bool isInWaitingPeriod = await CheckWaitingPeriod(connection, transaction, taskCounter, currentOxygenFlow);

                if (!_waitingCoolingCache.TryGetValue(taskCounter, out var waitingProcess))
                {
                    waitingProcess = new WaitingCoolingProcess
                    {
                        TaskCounter = taskCounter,
                        WaitingStartTime = DateTime.Now,
                        LastCalculationTime = DateTime.Now,
                        TotalWaitingCooling = 0m,
                        IsInWaitingPeriod = false,
                        WaitingCoolingCoefficient = 0m,
                        LastTemperature = 0m,
                        TiltCoolingCalculated = false,
                        TiltCooling = 0m,
                        WaitingEndTemperature = 0m,
                        HasResumedFromWaiting = false
                    };
                    _waitingCoolingCache[taskCounter] = waitingProcess;
                }

                decimal waitingCooling = 0m;
                decimal tiltCooling = 0m;
                decimal totalWaitingCooling = waitingProcess.TotalWaitingCooling;

                if (isInWaitingPeriod)
                {
                    var (intervalCooling, currentTiltCooling, accumulatedWaitingCooling) =
                        await ProcessWaitingAndTiltCooling(connection, transaction, taskCounter);
                    waitingCooling = intervalCooling;
                    tiltCooling = currentTiltCooling;
                    totalWaitingCooling = accumulatedWaitingCooling;

                    LogInfo($"低温阶段等待期温降累计: 任务={taskCounter}, 累计等待温降={totalWaitingCooling:F2}℃", taskCounter);
                }
                else
                {
                    if (waitingProcess.IsInWaitingPeriod)
                    {
                        waitingProcess.IsInWaitingPeriod = false;
                        tiltCooling = waitingProcess.TiltCooling;                         LogInfo($"低温阶段离开等待期: 任务={taskCounter}, 累计等待温降={waitingProcess.TotalWaitingCooling:F2}℃, " +
                               $"倒炉温降保持不变={tiltCooling:F2}℃", taskCounter);
                    }
                    else
                    {
                        tiltCooling = waitingProcess.TiltCooling;
                        LogInfo($"低温阶段非等待期，使用累计等待温降: {totalWaitingCooling:F2}℃, " +
                               $"倒炉温降: {tiltCooling:F2}℃", taskCounter);
                    }
                }

                decimal baseTemperature = await GetBaseTemperature(connection, transaction, taskCounter);
                decimal targetTempLow = await GetTargetTemperature(connection, transaction, taskCounter);
                decimal calcOxygenVolume = await GetCalcOxygenVolume(connection, transaction, taskCounter);
                decimal actualOxygenVolume = currentTotalOxygen;

                decimal firstBathTemp = await GetFirstBathTemp(connection, transaction, taskCounter);

                                if (firstBathTemp >= 1450 && !_mbTemperatureOxygenCache.ContainsKey(taskCounter))
                {
                    decimal currentTotalOxygenAtMB = await GetCurrentTotalOxygen(connection, transaction);
                    _mbTemperatureOxygenCache[taskCounter] = (DateTime.Now, currentTotalOxygenAtMB);

                                        if (_lowTempProcessCache.ContainsKey(taskCounter))
                    {
                        _lowTempProcessCache.TryRemove(taskCounter, out _);
                        LogInfo($"清理低温阶段缓存，准备进入高温阶段", taskCounter);
                    }

                                        if (waitingProcess != null)
                    {
                        decimal previousWaitingCooling = waitingProcess.TotalWaitingCooling;
                        decimal previousTiltCooling = waitingProcess.TiltCooling;

                        waitingProcess.TotalWaitingCooling = 0m;
                        waitingProcess.TiltCooling = 0m;
                        waitingProcess.TiltCoolingCalculated = false;
                        waitingProcess.WaitingStartTime = DateTime.Now;
                        waitingProcess.LastCalculationTime = DateTime.Now;
                        waitingProcess.IsInWaitingPeriod = false;

                        LogInfo($"=== MB表有有效温度重置等待温降 - 任务={taskCounter} ===", taskCounter);
                        LogInfo($"MB表温度: {firstBathTemp:F1}℃ ≥ 1450℃", taskCounter);
                        LogInfo($"记录时刻总氧量: {currentTotalOxygenAtMB:F1} m³", taskCounter);
                        LogInfo($"记录时间: {DateTime.Now:HH:mm:ss}", taskCounter);
                        LogInfo($"等待温降重置: {previousWaitingCooling:F2}℃ → 0℃", taskCounter);
                        LogInfo($"倒炉温降重置: {previousTiltCooling:F2}℃ → 0℃", taskCounter);
                        LogInfo($"后续计算将以此温度为基准", taskCounter);
                        LogInfo($"=== MB表有温度重置完成 ===", taskCounter);
                    }

                    return 0;                 }

                                if (_mbTemperatureOxygenCache.ContainsKey(taskCounter))
                {
                    LogInfo($"已存在MB表温度记录，跳过低温计算", taskCounter);
                    return 0;
                }

                if (firstBathTemp >= 1450 && _lowTempProcessCache.ContainsKey(taskCounter))
                {
                    _lowTempProcessCache.TryRemove(taskCounter, out _);
                    return 0;
                }

                var allMaterialData = await GetAllMaterialDataForLowTemp(connection, transaction, taskCounter);
                decimal totalScrapCooling = await GetTotalScrapCooling(connection, transaction, taskCounter);
                decimal mainCoolantCalcCooling = allMaterialData.MainCoolantData.CalcCooling;
                decimal otherMaterialsCalcCooling = allMaterialData.OtherMaterials.Sum(m => m.CalcCooling);

                LogInfo($"=== 低温阶段氧气升温系数计算过程 - 任务={taskCounter} ===", taskCounter);
                LogInfo($"基础数据:", taskCounter);
                LogInfo($"  - 基准温度: {baseTemperature:F1}℃", taskCounter);
                LogInfo($"  - 目标温度: {targetTempLow:F1}℃", taskCounter);
                LogInfo($"  - 计算氧量: {calcOxygenVolume:F1}m³", taskCounter);
                LogInfo($"  - 实际氧量: {actualOxygenVolume:F1}m³", taskCounter);

                LogInfo($"物料温降数据:", taskCounter);
                LogInfo($"  - 主冷却剂计算温降: {mainCoolantCalcCooling:F3}℃", taskCounter);
                LogInfo($"  - 其他物料计算温降: {otherMaterialsCalcCooling:F3}℃", taskCounter);
                LogInfo($"  - 废钢总计算温降: {totalScrapCooling:F3}℃", taskCounter);

                decimal temperatureDifference = targetTempLow - baseTemperature;
                decimal numerator = temperatureDifference + mainCoolantCalcCooling + otherMaterialsCalcCooling + totalScrapCooling;
                decimal oxygenHeatingCoefficient = numerator / calcOxygenVolume;
                decimal oxygenHeatingEffect = oxygenHeatingCoefficient * actualOxygenVolume;

                LogInfo($"氧气升温系数计算:", taskCounter);
                LogInfo($"  - 温度差: {targetTempLow:F1} - {baseTemperature:F1} = {temperatureDifference:F1}℃", taskCounter);
                LogInfo($"  - 分子计算: {temperatureDifference:F1} + {mainCoolantCalcCooling:F3} + {otherMaterialsCalcCooling:F3} + {totalScrapCooling:F3} = {numerator:F3}℃", taskCounter);
                LogInfo($"  - 分母(计算氧量): {calcOxygenVolume:F1}m³", taskCounter);
                LogInfo($"  - 氧气升温系数: {numerator:F3} ÷ {calcOxygenVolume:F1} = {oxygenHeatingCoefficient:F4}℃/m³", taskCounter);
                LogInfo($"  - 氧气升温效应: {oxygenHeatingCoefficient:F4} × {actualOxygenVolume:F1} = {oxygenHeatingEffect:F1}℃", taskCounter);
                LogInfo($"公式说明: 氧气升温系数 = (目标温度 - 基准温度 + 主冷却剂温降 + 其他物料温降 + 废钢总温降) ÷ 计算氧量", taskCounter);
                LogInfo($"=== 低温阶段氧气升温系数计算完成 ===", taskCounter);

                                var existingProcess = _lowTempProcessCache.AddOrUpdate(taskCounter,
                    key => new LowTempCoolingProcess
                    {
                        TaskCounter = key,
                        BaseTemperature = baseTemperature,
                        StartTime = DateTime.Now,
                        TotalCompletedCooling = 0m,
                        TotalProcessedWeight = 0m,
                        TotalScrapCooling = totalScrapCooling,
                        InProgressScrapCooling = 0m,
                        TotalCompletedCoolantCooling = 0m,
                        InProgressCoolantCooling = 0m,
                        TotalCompletedOtherCooling = 0m,
                        InProgressOtherCooling = 0m
                    },
                    (key, existing) =>
                    {
                        existing.TotalScrapCooling = totalScrapCooling;
                        return existing;
                    });

                DateTime currentTime = DateTime.Now;

                var coolantMaterials = await GetAllCoolantMaterials(connection, transaction);

                                if (allMaterialData.MainCoolantData != null && allMaterialData.MainCoolantData.ActualWeight > 0)
                {
                    await ProcessMaterialIncrement(connection, transaction, taskCounter, existingProcess,
                        allMaterialData.MainCoolantData, "COOLANT", coolantMaterials);
                }

                                foreach (var otherMaterial in allMaterialData.OtherMaterials)
                {
                    if (otherMaterial.ActualWeight > 0)
                    {
                        await ProcessMaterialIncrement(connection, transaction, taskCounter, existingProcess,
                            otherMaterial, "OTHER", coolantMaterials);
                    }
                }

                                await ProcessScrapIncrement(connection, transaction, taskCounter, existingProcess, isInWaitingPeriod);

                                var currentScrapWeights = await GetScrapWeights(connection, transaction, taskCounter);
                decimal currentTotalScrapWeight = currentScrapWeights.Values.Sum();

                if (currentTotalScrapWeight > 0 && existingProcess.ScrapBatches.Count == 0)
                {
                    LogInfo($"=== 检测到废钢有重量但批次为空 - 任务={taskCounter} ===", taskCounter);
                    LogInfo($"当前废钢总重: {currentTotalScrapWeight:F1}kg", taskCounter);
                    LogInfo($"当前批次数量: {existingProcess.ScrapBatches.Count}", taskCounter);
                    LogInfo($"废钢总计算温降: {existingProcess.TotalScrapCooling:F3}℃", taskCounter);

                                        await InitializeScrapBatches(connection, transaction, taskCounter, existingProcess, currentScrapWeights);

                    LogInfo($"重新初始化后批次数量: {existingProcess.ScrapBatches.Count}", taskCounter);
                    LogInfo($"=== 废钢批次重新初始化完成 ===", taskCounter);
                }
                else if (existingProcess.ScrapBatches.Count > 0)
                {
                    LogInfo($"废钢批次正常: 批次数量={existingProcess.ScrapBatches.Count}, 总重={currentTotalScrapWeight:F1}kg", taskCounter);
                }
                else
                {
                    LogInfo($"废钢批次为空且无重量: 总重={currentTotalScrapWeight:F1}kg", taskCounter);
                }
                
                                decimal currentScrapCooling = await CalculateCurrentScrapCooling(existingProcess, currentTime);

                                decimal currentCoolantCooling = await CalculateCurrentCoolantCooling(existingProcess, currentTime);
                decimal currentOtherCooling = await CalculateCurrentOtherCooling(existingProcess, currentTime);

                                decimal totalCoolantCooling = existingProcess.TotalCompletedCoolantCooling + currentCoolantCooling;
                decimal totalOtherCooling = existingProcess.TotalCompletedOtherCooling + currentOtherCooling;
                decimal totalMaterialCooling = totalCoolantCooling + totalOtherCooling;

                                decimal finalTemperature = baseTemperature
                         + oxygenHeatingEffect
                         - totalWaitingCooling
                         - tiltCooling
                         - currentScrapCooling                                   - totalMaterialCooling;

                finalTemperature = Math.Round(finalTemperature, 2, MidpointRounding.AwayFromZero);

                                existingProcess.InProgressScrapCooling = currentScrapCooling;
                existingProcess.InProgressCoolantCooling = currentCoolantCooling;
                existingProcess.InProgressOtherCooling = currentOtherCooling;

                string periodType = isInWaitingPeriod ? "等待期" : "正常期";
                LogInfo($"低温阶段{periodType}计算汇总 - 任务={taskCounter}:", taskCounter);
                LogInfo($"  - 基准温度: {baseTemperature:F1}℃", taskCounter);
                LogInfo($"  - 氧气升温: {oxygenHeatingEffect:F1}℃ (系数={oxygenHeatingCoefficient:F4}℃/m³)", taskCounter);
                LogInfo($"  - 累计等待温降: {totalWaitingCooling:F2}℃", taskCounter);
                LogInfo($"  - 累计倒炉温降: {tiltCooling:F2}℃", taskCounter);
                LogInfo($"  - 废钢当前实际温降: {currentScrapCooling:F3}℃", taskCounter);                  LogInfo($"  - 累计物料温降: {totalMaterialCooling:F3}℃", taskCounter);
                LogInfo($"  - 最终温度: {finalTemperature:F1}℃", taskCounter);

                                if (existingProcess.ScrapBatches.Any())
                {
                    LogInfo($"废钢温降详细计算 - 任务={taskCounter}:", taskCounter);
                    decimal totalBatchCooling = 0m;
                    foreach (var batch in existingProcess.ScrapBatches)
                    {
                        double elapsedSeconds = (currentTime - batch.StartTime).TotalSeconds;
                        decimal progress = batch.GetProgressRatio(currentTime);
                        decimal batchCurrentCooling = batch.GetCurrentActualCooling(currentTime);
                        totalBatchCooling += batchCurrentCooling;

                        LogInfo($"  - 批次[{batch.MaterialId}]: " +
                               $"重量={batch.Weight:F1}kg, " +
                               $"总温降={batch.TotalCooling:F3}℃, " +
                               $"已过时间={elapsedSeconds:F1}/{batch.Duration:F1}秒, " +
                               $"进度={progress:P1}, " +
                               $"当前温降={batchCurrentCooling:F3}℃", taskCounter);
                    }
                    LogInfo($"  - 废钢批次合计温降: {totalBatchCooling:F3}℃", taskCounter);
                    LogInfo($"  - 废钢当前实际温降(方法): {currentScrapCooling:F3}℃", taskCounter);
                }
                else if (currentScrapCooling > 0)
                {
                    LogInfo($"警告: 废钢当前实际温降={currentScrapCooling:F3}℃, 但批次为空", taskCounter);
                }

                                if (currentScrapCooling == 0 && currentTotalScrapWeight > 0 && existingProcess.TotalScrapCooling > 0)
                {
                    LogInfo($"警告: 有废钢重量({currentTotalScrapWeight:F1}kg)和计算温降({existingProcess.TotalScrapCooling:F3}℃)，但实际温降为0", taskCounter);
                    LogInfo($"建议检查废钢批次初始化逻辑", taskCounter);
                }

                if (currentScrapCooling > existingProcess.TotalScrapCooling)
                {
                    LogInfo($"警告: 废钢当前实际温降({currentScrapCooling:F3}℃)大于总计算温降({existingProcess.TotalScrapCooling:F3}℃)", taskCounter);
                    LogInfo($"建议检查废钢进度计算逻辑", taskCounter);
                }
                
                return finalTemperature;
            }
            catch (Exception ex)
            {
                LogError($"低温阶段温度计算失败: {ex.Message}", ex, taskCounter);
                return 0;
            }
        }

               

        private static async Task<decimal> GetFirstBathTempSB(OracleConnection connection, OracleTransaction transaction, string taskCounter)
        {
            try
            {
                string sql = $@"
                    SELECT {TableConfig.FirstBathTempSBColumn} 
                    FROM {GetTableName(TableConfig.PeriodDataSBTable)} 
                    WHERE {TableConfig.TaskCounterColumn} = :taskCounter";

                using (var command = new OracleCommand(sql, connection))
                {
                    command.Transaction = transaction;
                    command.Parameters.Add(new OracleParameter("taskCounter", taskCounter));

                    var result = await command.ExecuteScalarAsync();
                    if (result != null && result != DBNull.Value)
                    {
                        decimal firstBathTemp = Convert.ToDecimal(result);
                        if (firstBathTemp > 0)
                        {
                            return firstBathTemp;
                        }
                    }
                }
                return 0;
            }
            catch (Exception ex)
            {
                LogError($"获取SB表首次浴温失败: {ex.Message}");
                return 0;
            }
        }


        private static async Task<decimal> CalculateHighTempTemperature(
    OracleConnection connection, OracleTransaction transaction,
    string taskCounter, decimal currentOxygenFlow, decimal currentTotalOxygen)
        {
            try
            {
                LogInfo($"=== 开始高温阶段计算 - 任务={taskCounter} ===", taskCounter);
                LogInfo($"当前时间: {DateTime.Now:HH:mm:ss}", taskCounter);
                LogInfo($"缓存状态检查:", taskCounter);
                LogInfo($"  - MB温度氧量缓存: {_mbTemperatureOxygenCache.ContainsKey(taskCounter)}", taskCounter);
                LogInfo($"  - SB温度状态缓存: {_sbTemperatureStateCache.ContainsKey(taskCounter)}", taskCounter);

                                decimal sbFirstBathTemp = await GetFirstBathTempSB(connection, transaction, taskCounter);
                if (sbFirstBathTemp > 0)
                {
                    await CheckAndResetForSBFirstBathTemp(connection, transaction, taskCounter);
                }

                decimal mbFirstBathTemp = await GetFirstBathTemp(connection, transaction, taskCounter);
                bool hasMbTemperatureRecord = _mbTemperatureOxygenCache.ContainsKey(taskCounter);

                if (mbFirstBathTemp >= 1450 && !hasMbTemperatureRecord)
                {
                    LogInfo($"检测到MB表有温度({mbFirstBathTemp:F1}℃)，记录并重置等待温降", taskCounter);

                    _mbTemperatureOxygenCache[taskCounter] = (DateTime.Now, currentTotalOxygen);

                    if (_waitingCoolingCache.ContainsKey(taskCounter))
                    {
                        var process = _waitingCoolingCache[taskCounter];
                        decimal previousWaitingCooling = process.TotalWaitingCooling;
                        decimal previousTiltCooling = process.TiltCooling;

                        process.TotalWaitingCooling = 0m;
                        process.TiltCooling = 0m;
                        process.TiltCoolingCalculated = false;
                        process.WaitingStartTime = DateTime.Now;
                        process.LastCalculationTime = DateTime.Now;
                        process.IsInWaitingPeriod = false;

                        LogInfo($"=== 高温阶段入口重置等待温降 - 任务={taskCounter} ===", taskCounter);
                        LogInfo($"MB表温度: {mbFirstBathTemp:F1}℃ ≥ 1450℃", taskCounter);
                        LogInfo($"记录时刻总氧量: {currentTotalOxygen:F1} m³", taskCounter);
                        LogInfo($"记录时间: {DateTime.Now:HH:mm:ss}", taskCounter);
                        LogInfo($"等待温降重置: {previousWaitingCooling:F2}℃ → 0℃", taskCounter);
                        LogInfo($"倒炉温降重置: {previousTiltCooling:F2}℃ → 0℃", taskCounter);
                        LogInfo($"后续计算将以此温度为基准", taskCounter);
                        LogInfo($"=== 高温阶段入口重置完成 ===", taskCounter);
                    }
                    else
                    {
                        _waitingCoolingCache[taskCounter] = new WaitingCoolingProcess
                        {
                            TaskCounter = taskCounter,
                            WaitingStartTime = DateTime.Now,
                            LastCalculationTime = DateTime.Now,
                            TotalWaitingCooling = 0m,
                            IsInWaitingPeriod = false,
                            WaitingCoolingCoefficient = 0m,
                            LastTemperature = 0m,
                            TiltCoolingCalculated = false,
                            TiltCooling = 0m,
                            WaitingEndTemperature = 0m,
                            HasResumedFromWaiting = false
                        };
                        LogInfo($"创建新的等待温降缓存: 任务={taskCounter}", taskCounter);
                    }
                    Program.LogInfo($"进入高温阶段，准备显示温度监控弹窗", taskCounter);
                }

                if (ShouldStopCalculation(taskCounter))
                {
                    EndWaitingPeriod(taskCounter);
                    return 0;
                }

                bool isInWaitingPeriod = await CheckWaitingPeriod(connection, transaction, taskCounter, currentOxygenFlow);
                LogInfo($"高温阶段等待期检查: 任务={taskCounter}, 氧流量={currentOxygenFlow:F1}, 是否等待期={isInWaitingPeriod}", taskCounter);

                if (!_waitingCoolingCache.TryGetValue(taskCounter, out var waitingProcess))
                {
                    waitingProcess = new WaitingCoolingProcess
                    {
                        TaskCounter = taskCounter,
                        WaitingStartTime = DateTime.Now,
                        LastCalculationTime = DateTime.Now,
                        TotalWaitingCooling = 0m,
                        IsInWaitingPeriod = false,
                        WaitingCoolingCoefficient = 0m,
                        LastTemperature = 0m,
                        TiltCoolingCalculated = false,
                        TiltCooling = 0m,
                        WaitingEndTemperature = 0m,
                        HasResumedFromWaiting = false
                    };
                    _waitingCoolingCache[taskCounter] = waitingProcess;
                }

                decimal waitingCooling = 0m;
                decimal tiltCooling = 0m;
                decimal totalWaitingCooling = waitingProcess.TotalWaitingCooling;

                if (isInWaitingPeriod)
                {
                    if (!waitingProcess.IsInWaitingPeriod)
                    {
                        waitingProcess.IsInWaitingPeriod = true;
                        waitingProcess.WaitingStartTime = DateTime.Now;
                        waitingProcess.LastCalculationTime = DateTime.Now;
                        waitingProcess.WaitingCoolingCoefficient = await GetWaitingCoolingCoefficient(connection, transaction);
                        LogInfo($"高温阶段进入等待期: 任务={taskCounter}, 开始时间={DateTime.Now:HH:mm:ss}", taskCounter);
                    }

                    var (intervalCooling, currentTiltCooling, accumulatedWaitingCooling) =
                        await ProcessWaitingAndTiltCooling(connection, transaction, taskCounter);
                    waitingCooling = intervalCooling;
                    tiltCooling = currentTiltCooling;
                    totalWaitingCooling = accumulatedWaitingCooling;

                    LogInfo($"高温阶段等待期温降累计: 任务={taskCounter}, 累计等待温降={totalWaitingCooling:F2}℃", taskCounter);
                }
                else
                {
                    if (waitingProcess.IsInWaitingPeriod)
                    {
                        waitingProcess.IsInWaitingPeriod = false;
                        LogInfo($"高温阶段离开等待期: 任务={taskCounter}, 累计等待温降={waitingProcess.TotalWaitingCooling:F2}℃", taskCounter);
                    }

                    totalWaitingCooling = waitingProcess.TotalWaitingCooling;
                    tiltCooling = waitingProcess.TiltCooling;

                    LogInfo($"高温阶段非等待期，使用累计等待温降: {totalWaitingCooling:F2}℃, 倒炉温降: {tiltCooling:F2}℃", taskCounter);
                }

                if (ShouldStopCalculation(taskCounter))
                {
                    EndWaitingPeriod(taskCounter);
                    return 0;
                }

                (decimal baseTemperature, decimal targetTemperature, decimal calcOxygenVolume, decimal actualOxygenVolume) =
                    await GetHighTempBaseData(connection, transaction, taskCounter);

                if (ShouldStopCalculation(taskCounter))
                {
                    EndWaitingPeriod(taskCounter);
                    return 0;
                }

                if (baseTemperature <= 0)
                {
                    LogInfo($"高温阶段基准温度无效: {baseTemperature:F1}℃, 跳过计算", taskCounter);
                    return 0;
                }

                var allMaterialData = await GetAllMaterialData(connection, transaction, taskCounter);

                if (ShouldStopCalculation(taskCounter))
                {
                    EndWaitingPeriod(taskCounter);
                    return 0;
                }

                decimal mainCoolantCalcCooling = allMaterialData?.MainCoolantData?.CalcCooling ?? 0m;
                decimal otherMaterialsCalcCooling = allMaterialData?.OtherMaterials?.Sum(m => m.CalcCooling) ?? 0m;
                decimal totalMaterialCalcCooling = mainCoolantCalcCooling + otherMaterialsCalcCooling;

                decimal totalMaterialActualCooling =
                    (allMaterialData?.MainCoolantData?.ActualCooling ?? 0m) +
                    allMaterialData?.OtherMaterials?.Sum(m => m.ActualCooling) ?? 0m;

                decimal waitingCoolingCoefficient = await GetWaitingCoolingCoefficient(connection, transaction);
                decimal measurementCoolingCompensation = waitingCoolingCoefficient * _temperatureMeasurementDelay;

                decimal oxygenHeatingCoefficient = 0m;
                decimal oxygenHeatingEffect = 0m;

                bool isAfterSBTemperature = _sbTemperatureStateCache.TryGetValue(taskCounter, out var sbState) &&
                                           sbState.IsInitialized;

                if (isAfterSBTemperature)
                {
                    if (_highTempProcessCache.TryGetValue(taskCounter, out var highTempProcess))
                    {
                        if (highTempProcess.HasValidCoefficient &&
                            highTempProcess.LastCoefficientCalcTime < sbState.SBTemperatureTime)
                        {
                            oxygenHeatingCoefficient = highTempProcess.LastOxygenHeatingCoefficient;
                            oxygenHeatingEffect = oxygenHeatingCoefficient * actualOxygenVolume;

                            LogInfo($"SB表有温度后使用保存的氧气升温系数: " +
                                   $"系数={oxygenHeatingCoefficient:F4}℃/m³ (保存于{highTempProcess.LastCoefficientCalcTime:HH:mm:ss}), " +
                                   $"实际增量氧量={actualOxygenVolume:F1}m³, " +
                                   $"氧气升温效应={oxygenHeatingEffect:F1}℃", taskCounter);

                            goto SkipNormalCalculation;
                        }
                        else if (highTempProcess.LastOxygenHeatingCoefficient > 0)
                        {
                            oxygenHeatingCoefficient = highTempProcess.LastOxygenHeatingCoefficient;
                            oxygenHeatingEffect = oxygenHeatingCoefficient * actualOxygenVolume;

                            LogInfo($"SB表有温度后使用最近的氧气升温系数: " +
                                   $"系数={oxygenHeatingCoefficient:F4}℃/m³, " +
                                   $"实际增量氧量={actualOxygenVolume:F1}m³, " +
                                   $"氧气升温效应={oxygenHeatingEffect:F1}℃", taskCounter);

                            goto SkipNormalCalculation;
                        }
                        else
                        {
                            oxygenHeatingCoefficient = await CalculateAndSaveOxygenCoefficient(
                                connection, transaction, taskCounter, baseTemperature, targetTemperature,
                                calcOxygenVolume, totalMaterialCalcCooling, measurementCoolingCompensation,
                                highTempProcess);
                            oxygenHeatingEffect = oxygenHeatingCoefficient * actualOxygenVolume;

                            LogInfo($"SB表有温度后重新计算氧气升温系数: {oxygenHeatingCoefficient:F4}℃/m³", taskCounter);
                        }
                    }
                    else
                    {
                        oxygenHeatingCoefficient = await CalculateOxygenCoefficientNormally(
                            connection, transaction, taskCounter, baseTemperature, targetTemperature,
                            calcOxygenVolume, totalMaterialCalcCooling, measurementCoolingCompensation);
                        oxygenHeatingEffect = oxygenHeatingCoefficient * actualOxygenVolume;
                    }
                }
                else
                {
                    oxygenHeatingCoefficient = await CalculateOxygenCoefficientNormally(
                        connection, transaction, taskCounter, baseTemperature, targetTemperature,
                        calcOxygenVolume, totalMaterialCalcCooling, measurementCoolingCompensation);
                    oxygenHeatingEffect = oxygenHeatingCoefficient * actualOxygenVolume;

                    LogInfo($"SB表无温度，计算氧气升温系数: {oxygenHeatingCoefficient:F4}℃/m³", taskCounter);
                }

            SkipNormalCalculation:

                var highTempProcessInstance = _highTempProcessCache.AddOrUpdate(taskCounter,
                    key => new HighTempProcess
                    {
                        TaskCounter = key,
                        BaseTemperature = baseTemperature,
                        StartTime = DateTime.Now,
                        TotalCompletedCooling = 0m,
                        TotalProcessedWeight = 0m,
                        MaterialBatches = new List<MaterialBatch>()
                    },
                    (key, existing) => existing);

                decimal totalMaterialCooling = await ProcessAllMaterialBatches(
                    connection, transaction, taskCounter, highTempProcessInstance, allMaterialData);

                LogInfo($"高温阶段累计物料温降 - 任务={taskCounter}: " +
                       $"累计完成={highTempProcessInstance.TotalCompletedCooling:F3}℃, " +
                       $"本次增量={totalMaterialCooling:F3}℃",
                       taskCounter);

                if (ShouldStopCalculation(taskCounter))
                {
                    EndWaitingPeriod(taskCounter);
                    return 0;
                }

                decimal finalTemperature = baseTemperature
                         + oxygenHeatingEffect
                         - totalWaitingCooling
                         - tiltCooling
                         - totalMaterialActualCooling;

                finalTemperature = Math.Round(finalTemperature, 2, MidpointRounding.AwayFromZero);

                string periodType = isInWaitingPeriod ? "等待期" : "正常期";
                LogInfo($"高温阶段{periodType}计算汇总 - 任务={taskCounter}:", taskCounter);
                LogInfo($"  - 基准温度: {baseTemperature:F1}℃", taskCounter);
                LogInfo($"  - 氧气升温: {oxygenHeatingEffect:F1}℃ (系数={oxygenHeatingCoefficient:F4}℃/m³ × 氧量={actualOxygenVolume:F1}m³)", taskCounter);
                LogInfo($"  - 累计等待温降: {totalWaitingCooling:F2}℃", taskCounter);
                LogInfo($"  - 累计倒炉温降: {tiltCooling:F2}℃", taskCounter);
                LogInfo($"  - 累计物料温降: {totalMaterialActualCooling:F3}℃ (计算值={totalMaterialCalcCooling:F3}℃)", taskCounter);
                LogInfo($"  - 最终温度: {finalTemperature:F1}℃", taskCounter);

                return finalTemperature;
            }
            catch (Exception ex)
            {
                LogError($"高温阶段温度计算失败: {ex.Message}", ex, taskCounter);
                return 0;
            }
        }

        private static async Task<decimal> CalculateOxygenCoefficientNormally(
    OracleConnection connection, OracleTransaction transaction,
    string taskCounter, decimal baseTemperature, decimal targetTemperature,
    decimal calcOxygenVolume, decimal totalMaterialCalcCooling, decimal measurementCoolingCompensation)
        {
            try
            {
                                decimal temperatureDifference = targetTemperature - baseTemperature;

                                decimal numerator = temperatureDifference + totalMaterialCalcCooling + measurementCoolingCompensation;

                if (calcOxygenVolume <= 0)
                {
                    LogError($"计算氧量无效: {calcOxygenVolume:F1}m³", null, taskCounter);
                    return 0m;
                }

                decimal coefficient = numerator / calcOxygenVolume;

                                if (_highTempProcessCache.TryGetValue(taskCounter, out var highTempProcess))
                {
                    highTempProcess.LastOxygenHeatingCoefficient = coefficient;
                    highTempProcess.LastCoefficientCalcTime = DateTime.Now;
                    LogInfo($"保存氧气升温系数到缓存: {coefficient:F4}℃/m³", taskCounter);
                }

                return coefficient;
            }
            catch (Exception ex)
            {
                LogError($"计算氧气升温系数失败: {ex.Message}", ex, taskCounter);
                return 0m;
            }
        }

                private static async Task<decimal> CalculateAndSaveOxygenCoefficient(
    OracleConnection connection, OracleTransaction transaction,
    string taskCounter, decimal baseTemperature, decimal targetTemperature,
    decimal calcOxygenVolume, decimal totalMaterialCalcCooling, decimal measurementCoolingCompensation,
    HighTempProcess highTempProcess)
        {
            try
            {
                                decimal temperatureDifference = targetTemperature - baseTemperature;

                                decimal numerator = temperatureDifference + totalMaterialCalcCooling + measurementCoolingCompensation;

                if (calcOxygenVolume <= 0)
                {
                    LogError($"计算氧量无效: {calcOxygenVolume:F1}m³", null, taskCounter);
                    return 0m;
                }

                decimal coefficient = numerator / calcOxygenVolume;

                                highTempProcess.LastOxygenHeatingCoefficient = coefficient;
                highTempProcess.LastCoefficientCalcTime = DateTime.Now;
                LogInfo($"计算并保存氧气升温系数: {coefficient:F4}℃/m³", taskCounter);

                return coefficient;
            }
            catch (Exception ex)
            {
                LogError($"计算氧气升温系数失败: {ex.Message}", ex, taskCounter);
                return 0m;
            }
        }

        private static async Task<(decimal BaseTemp, decimal TargetTemp, decimal CalcOxygen, decimal ActualOxygen)>
GetHighTempBaseData(OracleConnection connection, OracleTransaction transaction, string taskCounter)
        {
            decimal baseTemperature = await GetFirstBathTempSB(connection, transaction, taskCounter);

                        if (baseTemperature > 0)
            {
                await CheckAndResetForSBFirstBathTemp(connection, transaction, taskCounter);
            }

            if (baseTemperature <= 0)
            {
                baseTemperature = await GetFirstBathTemp(connection, transaction, taskCounter);
            }

            string sql = $@"
    SELECT {TableConfig.ExpBathTempColumn} 
    FROM {GetTableName(TableConfig.PeriodDataSBTable)} 
    WHERE {TableConfig.TaskCounterColumn} = :taskCounter";

            decimal targetTemperature = 0;
            using (var command = new OracleCommand(sql, connection))
            {
                command.Transaction = transaction;
                command.Parameters.Add(new OracleParameter("taskCounter", taskCounter));

                var result = await command.ExecuteScalarAsync();
                if (result != null && result != DBNull.Value)
                {
                    targetTemperature = Convert.ToDecimal(result);
                }
            }

            decimal calcOxygenVolume = await GetSBReqO2Vol(connection, transaction, taskCounter);

            decimal actualOxygenVolume = 0;
            decimal currentTotalOxygen = await GetCurrentTotalOxygen(connection, transaction);

            bool sbHasTemperature = _sbTemperatureStateCache.TryGetValue(taskCounter, out var sbState) &&
                                   sbState.IsInitialized;

            bool mbHasTemperature = _mbTemperatureOxygenCache.TryGetValue(taskCounter, out var mbRecord);

            LogInfo($"高温阶段氧量数据源检查:", taskCounter);
            LogInfo($"  - SB表有温度: {sbHasTemperature}", taskCounter);
            LogInfo($"  - MB表有温度记录: {mbHasTemperature}", taskCounter);
            LogInfo($"  - 当前总氧量: {currentTotalOxygen:F1} m³", taskCounter);

            if (sbHasTemperature)
            {
                actualOxygenVolume = Math.Max(0, currentTotalOxygen - sbState.InitialOxygenVolume);

                LogInfo($"=== 第三阶段氧量计算（SB有温度后）===", taskCounter);
                LogInfo($"SB表有温度时总氧量: {sbState.InitialOxygenVolume:F1} m³", taskCounter);
                LogInfo($"当前总氧量: {currentTotalOxygen:F1} m³", taskCounter);
                LogInfo($"增量氧量: {actualOxygenVolume:F1} m³", taskCounter);
                LogInfo($"计算基准: SB表有温度时刻", taskCounter);
            }
            else if (mbHasTemperature)
            {
                actualOxygenVolume = Math.Max(0, currentTotalOxygen - mbRecord.oxygenVolume);

                LogInfo($"=== 第二阶段氧量计算（MB有温度后）===", taskCounter);
                LogInfo($"MB表有温度时总氧量: {mbRecord.oxygenVolume:F1} m³", taskCounter);
                LogInfo($"记录时间: {mbRecord.mbTemperatureTime:HH:mm:ss}", taskCounter);
                LogInfo($"当前总氧量: {currentTotalOxygen:F1} m³", taskCounter);
                LogInfo($"增量氧量: {actualOxygenVolume:F1} m³", taskCounter);
                LogInfo($"计算基准: MB表有温度时刻", taskCounter);
            }
            else
            {
                LogError($"高温阶段无MB或SB温度记录，无法计算氧量增量", null, taskCounter);
                actualOxygenVolume = 0;
            }

            return (baseTemperature, targetTemperature, calcOxygenVolume, actualOxygenVolume);
        }

        private static async Task CheckAndResetForSBFirstBathTemp(
    OracleConnection connection, OracleTransaction transaction, string taskCounter)
        {
            try
            {
                decimal sbFirstBathTemp = await GetFirstBathTempSB(connection, transaction, taskCounter);

                if (sbFirstBathTemp > 0)
                {
                    bool shouldInitialize = false;
                    if (!_sbTemperatureStateCache.TryGetValue(taskCounter, out var sbState))
                    {
                        shouldInitialize = true;
                    }
                    else if (!sbState.IsInitialized)
                    {
                        shouldInitialize = true;
                    }

                    if (shouldInitialize)
                    {
                                                decimal currentTotalOxygen = await GetCurrentTotalOxygen(connection, transaction);

                        var coolantMaterials = await GetAllCoolantMaterials(connection, transaction);
                        var initialMaterialWeights = await GetInitialMaterialWeightsForSB(connection, transaction, taskCounter, coolantMaterials);

                        sbState = new SBTemperatureState
                        {
                            TaskCounter = taskCounter,
                            SBTemperatureTime = DateTime.Now,
                            BaseTemperature = sbFirstBathTemp,
                            InitialOxygenVolume = currentTotalOxygen,                             InitialMaterialWeights = initialMaterialWeights,
                            IsInitialized = true
                        };

                        _sbTemperatureStateCache[taskCounter] = sbState;

                                                if (_waitingCoolingCache.TryGetValue(taskCounter, out var waitingProcess))
                        {
                            decimal previousWaitingCooling = waitingProcess.TotalWaitingCooling;

                            waitingProcess.TotalWaitingCooling = 0m;
                            waitingProcess.TiltCooling = 0m;
                            waitingProcess.TiltCoolingCalculated = false;
                            waitingProcess.WaitingStartTime = sbState.SBTemperatureTime;
                            waitingProcess.LastCalculationTime = sbState.SBTemperatureTime;
                            waitingProcess.IsInWaitingPeriod = false;
                            LogInfo($"SB表有温度后重置等待温降: 任务={taskCounter}, " +
                                   $"之前累计等待温降={previousWaitingCooling:F2}℃ → 0℃, " +
                                   $"重置时间={sbState.SBTemperatureTime:HH:mm:ss}", taskCounter);

                            waitingProcess.WaitingEndTemperature = sbFirstBathTemp;
                            waitingProcess.HasResumedFromWaiting = false;
                        }

                        LogInfo($"=== SB表有温度记录点 ===", taskCounter);
                        LogInfo($"SB表温度: {sbFirstBathTemp:F1}℃", taskCounter);
                        LogInfo($"记录时刻总氧量: {currentTotalOxygen:F1} m³", taskCounter);
                        LogInfo($"记录时间: {DateTime.Now:HH:mm:ss}", taskCounter);
                        LogInfo($"后续氧量计算将以此值为基准", taskCounter);

                        if (_highTempProcessCache.TryGetValue(taskCounter, out var highTempProcess))
                        {
                            decimal previousCooling = highTempProcess.TotalCompletedCooling;
                            decimal previousWeight = highTempProcess.TotalProcessedWeight;

                            highTempProcess.TotalCompletedCooling = 0m;
                            highTempProcess.TotalProcessedWeight = 0m;
                            highTempProcess.MaterialBatches.Clear();
                            highTempProcess.BaseTemperature = sbFirstBathTemp;
                            highTempProcess.StartTime = sbState.SBTemperatureTime;

                            if (highTempProcess.LastOxygenHeatingCoefficient > 0)
                            {
                                LogInfo($"保留SB表有温度前的氧气升温系数: {highTempProcess.LastOxygenHeatingCoefficient:F4}℃/m³", taskCounter);
                            }

                            LogInfo($"重置高温阶段累计物料温降: 任务={taskCounter}, " +
                                   $"基准温度={sbFirstBathTemp:F1}℃, 之前累计温降={previousCooling:F2}℃ → 0℃, " +
                                   $"之前处理重量={previousWeight:F1}kg → 0kg", taskCounter);
                        }
                        else
                        {
                            _highTempProcessCache[taskCounter] = new HighTempProcess
                            {
                                TaskCounter = taskCounter,
                                BaseTemperature = sbFirstBathTemp,
                                StartTime = sbState.SBTemperatureTime,
                                TotalCompletedCooling = 0m,
                                TotalProcessedWeight = 0m,
                                MaterialBatches = new List<MaterialBatch>()
                            };
                            LogInfo($"创建高温阶段过程: 任务={taskCounter}, 基准温度={sbFirstBathTemp:F1}℃", taskCounter);
                        }

                        if (_lowTempProcessCache.TryGetValue(taskCounter, out var lowTempProcess))
                        {
                            _lowTempProcessCache.TryRemove(taskCounter, out _);
                            LogInfo($"清理低温阶段缓存: 任务={taskCounter}，已进入高温阶段", taskCounter);
                        }

                                                if (_mbTemperatureOxygenCache.ContainsKey(taskCounter))
                        {
                            _mbTemperatureOxygenCache.TryRemove(taskCounter, out _);
                            LogInfo($"清理MB表有温度记录，切换到SB表有温度记录", taskCounter);
                        }

                        LogInfo($"SB表有温度初始化完成，当前计算周期立即结束。任务={taskCounter}, 温度={sbFirstBathTemp:F1}℃", taskCounter);
                        return;
                    }
                    else
                    {
                        LogInfo($"SB表温度状态已初始化，跳过重复重置 - 任务={taskCounter}, 温度={sbFirstBathTemp:F1}℃", taskCounter);
                    }
                }
            }
            catch (Exception ex)
            {
                LogError($"检查SB表首次浴温重置失败: {ex.Message}", ex, taskCounter);
            }
        }

        private static async Task<decimal> GetMaterialRB1Weight(
            OracleConnection connection, OracleTransaction transaction,
            string taskCounter, string materialId)
        {
            string sql = $@"
        SELECT NVL({TableConfig.RB1RepWgtColumn}, 0) 
        FROM {GetTableName(TableConfig.AdditionsTable)} 
        WHERE {TableConfig.TaskCounterColumn} = :taskCounter 
          AND {TableConfig.MaterialIdColumn} = :materialId";

            try
            {
                using (var command = new OracleCommand(sql, connection))
                {
                    command.Transaction = transaction;
                    command.Parameters.Add(new OracleParameter("taskCounter", taskCounter));
                    command.Parameters.Add(new OracleParameter("materialId", materialId));

                    var result = await command.ExecuteScalarAsync();
                    return result != null && result != DBNull.Value ? Convert.ToDecimal(result) : 0;
                }
            }
            catch (Exception ex)
            {
                LogError($"获取物料RB1重量失败: {ex.Message}", null, taskCounter);
                return 0;
            }
        }

        private static async Task<Dictionary<string, decimal>> GetInitialMaterialWeightsForSB(
    OracleConnection connection, OracleTransaction transaction, string taskCounter,
    Dictionary<string, int> coolantMaterials)
        {
            var initialWeights = new Dictionary<string, decimal>();

            if (!coolantMaterials.Any()) return initialWeights;

            const int batchSize = 1000;
            var materialIds = coolantMaterials.Keys.ToList();

            for (int i = 0; i < materialIds.Count; i += batchSize)
            {
                var batchMaterials = materialIds.Skip(i).Take(batchSize).ToList();

                string sql = $@"
            SELECT {TableConfig.MaterialIdColumn}, 
                   COALESCE(
                       NVL({TableConfig.RB1RepWgtColumn}, 0),
                       NVL({TableConfig.SBRepWgtColumn}, 0)
                   ) as initial_weight
            FROM {GetTableName(TableConfig.AdditionsTable)} 
            WHERE {TableConfig.TaskCounterColumn} = :taskCounter 
              AND {TableConfig.MaterialIdColumn} IN (" + string.Join(",", batchMaterials.Select(m => $"'{m}'")) + ")";

                try
                {
                    using (var command = new OracleCommand(sql, connection))
                    {
                        command.Transaction = transaction;
                        command.Parameters.Add(new OracleParameter("taskCounter", taskCounter));

                        using (var reader = await command.ExecuteReaderAsync())
                        {
                            while (await reader.ReadAsync())
                            {
                                string materialId = reader.GetString(0);
                                decimal weight = reader.GetDecimal(1);
                                initialWeights[materialId] = weight;
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    LogError($"获取SB表有温度时刻物料重量失败: {ex.Message}", null, taskCounter);
                }
            }

            foreach (var material in coolantMaterials.Keys)
            {
                if (!initialWeights.ContainsKey(material))
                {
                    initialWeights[material] = 0;
                }
            }

            return initialWeights;
        }

        private static ConcurrentDictionary<string, SBTemperatureState> _sbTemperatureStateCache =
    new ConcurrentDictionary<string, SBTemperatureState>();
        private static ConcurrentDictionary<string, (DateTime mbTemperatureTime, decimal oxygenVolume)> _mbTemperatureOxygenCache =
    new ConcurrentDictionary<string, (DateTime, decimal)>();

        private class SBTemperatureState
        {
            public string TaskCounter { get; set; }
            public DateTime SBTemperatureTime { get; set; }
            public decimal BaseTemperature { get; set; }
            public decimal InitialOxygenVolume { get; set; }
            public Dictionary<string, decimal> InitialMaterialWeights { get; set; } = new Dictionary<string, decimal>();
            public bool IsInitialized { get; set; } = false;
        }

        
        private static async Task<decimal> CalculateCurrentOtherCooling(LowTempCoolingProcess process, DateTime currentTime)
        {
            decimal totalInProgressCooling = 0m;
            var completedBatches = new List<MaterialBatch>();

            try
            {
                LogInfo($"计算其他物料累计温降 - 活跃批次数量: {process.OtherMaterialBatches.Count}, 已完成累计: {process.TotalCompletedOtherCooling:F3}℃", process.TaskCounter);

                foreach (var batch in process.OtherMaterialBatches)
                {
                    double elapsedSeconds = Math.Max(0, (currentTime - batch.StartTime).TotalSeconds);
                    double durationSeconds = (double)batch.Duration;

                    if (elapsedSeconds <= 0)
                    {
                        LogInfo($"  其他物料批次 {batch.MaterialId}: 尚未开始，跳过计算", process.TaskCounter);
                        continue;
                    }

                    if (elapsedSeconds < durationSeconds)
                    {
                                                decimal progress = (decimal)(elapsedSeconds / durationSeconds);
                        decimal batchCooling = progress * batch.TotalCooling;
                        totalInProgressCooling += batchCooling;

                        LogInfo($"  其他物料批次 {batch.MaterialId}: 进度={progress:P1}, 已过={elapsedSeconds:F1}/{durationSeconds:F1}秒, " +
                               $"当前温降={batchCooling:F3}℃, 批次总温降={batch.TotalCooling:F3}℃, 增量重量={batch.IncrementWeight:F1}kg", process.TaskCounter);
                    }
                    else
                    {
                                                process.TotalCompletedOtherCooling += batch.TotalCooling;
                        completedBatches.Add(batch);

                        LogInfo($"  其他物料批次 {batch.MaterialId}: 已完成, 加入总完成温降={batch.TotalCooling:F3}℃, " +
                               $"累计完成温降={process.TotalCompletedOtherCooling:F3}℃, 增量重量={batch.IncrementWeight:F1}kg", process.TaskCounter);
                    }
                }

                                process.OtherMaterialBatches.RemoveAll(completedBatches.Contains);
                int removedCount = completedBatches.Count;
                if (removedCount > 0)
                {
                    LogInfo($"清理完成的其他物料批次: 移除了{removedCount}个批次, 剩余{process.OtherMaterialBatches.Count}个活跃批次", process.TaskCounter);
                }

                LogInfo($"其他物料累计温降合计: 已完成={process.TotalCompletedOtherCooling:F3}℃, 进行中={totalInProgressCooling:F3}℃, 总计={process.TotalCompletedOtherCooling + totalInProgressCooling:F3}℃", process.TaskCounter);

                return totalInProgressCooling;
            }
            catch (Exception ex)
            {
                LogError($"计算其他物料累计温降失败: {ex.Message}", null, process.TaskCounter);
                return 0m;
            }
        }

        private static async Task<decimal> CalculateCurrentCoolantCooling(LowTempCoolingProcess process, DateTime currentTime)
        {
            decimal totalInProgressCooling = 0m;
            var completedBatches = new List<CoolantBatch>();

            try
            {
                LogInfo($"计算主冷却剂累计温降 - 活跃批次数量: {process.CoolantBatches.Count}, 已完成累计: {process.TotalCompletedCoolantCooling:F3}℃", process.TaskCounter);

                foreach (var batch in process.CoolantBatches)
                {
                    double elapsedSeconds = Math.Max(0, (currentTime - batch.StartTime).TotalSeconds);
                    double durationSeconds = (double)batch.Duration;

                    if (elapsedSeconds <= 0)
                    {
                        LogInfo($"  主冷却剂批次 {batch.MaterialId}: 尚未开始，跳过计算", process.TaskCounter);
                        continue;
                    }

                    if (elapsedSeconds < durationSeconds)
                    {
                                                decimal progress = (decimal)(elapsedSeconds / durationSeconds);
                        decimal batchCooling = progress * batch.TotalCooling;
                        totalInProgressCooling += batchCooling;

                        LogInfo($"  主冷却剂批次 {batch.MaterialId}: 进度={progress:P1}, 已过={elapsedSeconds:F1}/{durationSeconds:F1}秒, " +
                               $"当前温降={batchCooling:F3}℃, 批次总温降={batch.TotalCooling:F3}℃, 增量重量={batch.IncrementWeight:F1}kg", process.TaskCounter);
                    }
                    else
                    {
                                                process.TotalCompletedCoolantCooling += batch.TotalCooling;
                        completedBatches.Add(batch);

                        LogInfo($"  主冷却剂批次 {batch.MaterialId}: 已完成, 加入总完成温降={batch.TotalCooling:F3}℃, " +
                               $"累计完成温降={process.TotalCompletedCoolantCooling:F3}℃, 增量重量={batch.IncrementWeight:F1}kg", process.TaskCounter);
                    }
                }

                                int removedCount = process.CoolantBatches.RemoveAll(completedBatches.Contains);
                if (removedCount > 0)
                {
                    LogInfo($"清理完成的主冷却剂批次: 移除了{removedCount}个批次, 剩余{process.CoolantBatches.Count}个活跃批次", process.TaskCounter);
                }

                LogInfo($"主冷却剂累计温降合计: 已完成={process.TotalCompletedCoolantCooling:F3}℃, 进行中={totalInProgressCooling:F3}℃, 总计={process.TotalCompletedCoolantCooling + totalInProgressCooling:F3}℃", process.TaskCounter);

                return totalInProgressCooling;
            }
            catch (Exception ex)
            {
                LogError($"计算主冷却剂累计温降失败: {ex.Message}", null, process.TaskCounter);
                return 0m;
            }
        }

        private static async Task<decimal> CalculateCurrentScrapCooling(LowTempCoolingProcess process, DateTime currentTime)
        {
            decimal totalCurrentCooling = 0m;
            
            try
            {
                LogInfo($"计算废钢当前实际温降 - 批次数量: {process.ScrapBatches.Count}", process.TaskCounter);

                foreach (var batch in process.ScrapBatches)
                {
                                        decimal progress = batch.GetProgressRatio(currentTime);

                                        decimal currentActualCooling = batch.GetCurrentActualCooling(currentTime);
                    totalCurrentCooling += currentActualCooling;

                                        double elapsedSeconds = (currentTime - batch.StartTime).TotalSeconds;
                    LogInfo($"废钢批次[{batch.MaterialId}]: " +
                           $"重量={batch.Weight:F1}kg, " +
                           $"已过时间={elapsedSeconds:F1}/{batch.Duration:F1}秒, " +
                           $"进度={progress:P1}, " +
                           $"当前实际温降={currentActualCooling:F3}℃, " +
                           $"批次总温降={batch.TotalCooling:F3}℃",
                           process.TaskCounter);

                                                                                                                        
                                        if (progress >= 1.0m)
                    {
                        LogInfo($"废钢批次[{batch.MaterialId}]已完成熔化，但仍保留计算", process.TaskCounter);
                    }
                }

                                                                                                
                                process.InProgressScrapCooling = totalCurrentCooling;

                LogInfo($"废钢当前实际温降合计: {totalCurrentCooling:F3}℃", process.TaskCounter);
                return totalCurrentCooling;
            }
            catch (Exception ex)
            {
                LogError($"计算废钢当前实际温降失败: {ex.Message}", null, process.TaskCounter);
                return 0m;
            }
        }

                

        private static async Task ProcessMaterialIncrement(
    OracleConnection connection, OracleTransaction transaction,
    string taskCounter, LowTempCoolingProcess process, MaterialData materialData, string materialType,
    Dictionary<string, int> coolantMaterials)
        {
            decimal currentWeight = materialData.ActualWeight;
            string materialKey = $"{materialType}_{materialData.MaterialId}";
            decimal historicalWeight = process.MaterialTotalWeights.GetValueOrDefault(materialKey);
            decimal increment = currentWeight - historicalWeight;

            LogInfo($"物料增量检查: 任务={taskCounter}, 物料={materialData.MaterialId}, " +
                   $"当前重量={currentWeight:F1}kg, 历史重量={historicalWeight:F1}kg, 增量={increment:F1}kg", taskCounter);

                        if ((historicalWeight == 0 && currentWeight > 0) || increment > 10)
            {
                decimal weightToProcess = (historicalWeight == 0 && currentWeight > 0) ? currentWeight : increment;

                (decimal currentSteelWeight, decimal currentSlagWeight) =
                    await GetSteelAndSlagWeightsForLowTemp(connection, transaction, taskCounter);

                if (currentSteelWeight > 0)
                {
                    decimal steelSpecificHeat = await GetSteelSpecificHeat(connection, transaction);
                    decimal slagSpecificHeat = await GetSlagSpecificHeat(connection, transaction);
                    decimal currentTotalHeatCapacity = (currentSteelWeight * steelSpecificHeat) + (currentSlagWeight * slagSpecificHeat);

                    if (currentTotalHeatCapacity > 0)
                    {
                        int sdmIdx = coolantMaterials.GetValueOrDefault(materialData.MaterialId, 108);
                        decimal coolingEffect = await GetMaterialCoolingEffect(connection, transaction, sdmIdx);
                        decimal totalCooling = (weightToProcess / 1000) * coolingEffect / currentTotalHeatCapacity;

                        decimal meltingRate = materialType == "COOLANT"
                            ? await GetMainCoolantMeltingRate(connection, transaction)
                            : await GetMaterialMeltingRate(connection, transaction, sdmIdx);

                        decimal duration = Math.Max(1, weightToProcess / meltingRate);

                        LogInfo($"=== 创建物料批次 - 任务={taskCounter} ===", taskCounter);
                        LogInfo($"物料ID: {materialData.MaterialId}", taskCounter);
                        LogInfo($"  - 处理重量: {weightToProcess:F1}kg", taskCounter);
                        LogInfo($"  - 冷却效应: {coolingEffect:F0} kJ/吨", taskCounter);
                        LogInfo($"  - 总温降: {totalCooling:F3}℃", taskCounter);
                        LogInfo($"  - 熔化速率: {meltingRate:F1}kg/s", taskCounter);
                        LogInfo($"  - 持续时间: {duration:F1}秒", taskCounter);
                        LogInfo($"=== 物料批次创建完成 ===", taskCounter);

                        if (materialType == "COOLANT")
                        {
                            var batch = new CoolantBatch
                            {
                                MaterialId = materialData.MaterialId,
                                IncrementWeight = weightToProcess,
                                TotalCooling = totalCooling,
                                Duration = duration,
                                StartTime = DateTime.Now
                            };
                            process.CoolantBatches.Add(batch);
                        }
                        else
                        {
                            var batch = new MaterialBatch
                            {
                                MaterialId = materialData.MaterialId,
                                IncrementWeight = weightToProcess,
                                TotalCooling = totalCooling,
                                Duration = duration,
                                StartTime = DateTime.Now
                            };
                            process.OtherMaterialBatches.Add(batch);
                        }

                        process.MaterialTotalWeights[materialKey] = currentWeight;
                        LogInfo($"物料批次已创建并加入处理队列: {materialData.MaterialId}, 重量={weightToProcess:F1}kg", taskCounter);
                    }
                }
            }
            else if (increment > 0)
            {
                LogInfo($"物料小量增加: 任务={taskCounter}, 物料={materialData.MaterialId}, " +
                       $"增量={increment:F1}kg < 10kg, 仅更新历史重量", taskCounter);
                process.MaterialTotalWeights[materialKey] = currentWeight;
            }
            else
            {
                LogInfo($"物料无增量: 任务={taskCounter}, 物料={materialData.MaterialId}, " +
                       $"当前重量={currentWeight:F1}kg, 无变化", taskCounter);
                process.MaterialTotalWeights[materialKey] = currentWeight;
            }
        }

        private static async Task<decimal> ProcessAllMaterialBatches(
    OracleConnection connection, OracleTransaction transaction,
    string taskCounter, HighTempProcess process, AllMaterialData materialData)
        {
            if (ShouldStopCalculation(taskCounter))
            {
                return process.TotalCompletedCooling;
            }

            DateTime currentTime = DateTime.Now;
            decimal incrementalCooling = 0m;
            var completedBatches = new List<MaterialBatch>();

            LogInfo($"开始处理高温阶段物料批次 - 任务={taskCounter}: " +
                   $"累计完成温降={process.TotalCompletedCooling:F3}℃, " +
                   $"总处理重量={process.TotalProcessedWeight:F1}kg",
                   taskCounter);

                        if (materialData != null && materialData.MainCoolantData != null &&
                materialData.MainCoolantData.ActualWeight > process.TotalProcessedWeight)
            {
                decimal increment = materialData.MainCoolantData.ActualWeight - process.TotalProcessedWeight;
                if (increment > 0)
                {
                    decimal duration = increment / materialData.MainCoolantData.MeltingRate;
                    decimal incrementCooling = materialData.MainCoolantData.ActualCooling * (increment / materialData.MainCoolantData.ActualWeight);

                    var batch = new MaterialBatch
                    {
                        MaterialId = materialData.MainCoolantData.MaterialId,
                        IncrementWeight = increment,
                        TotalCooling = incrementCooling,
                        Duration = duration,
                        StartTime = currentTime
                    };

                    process.MaterialBatches.Add(batch);
                    process.TotalProcessedWeight += increment;

                    LogInfo($"高温阶段新增物料批次: 任务={taskCounter}, 物料={materialData.MainCoolantData.MaterialId}, " +
                           $"增量重量={increment:F1}kg, 预计温降={incrementCooling:F3}℃, " +
                           $"持续时间={duration:F1}秒", taskCounter);
                }
            }

                        foreach (var batch in process.MaterialBatches)
            {
                if (ShouldStopCalculation(taskCounter)) break;

                double elapsedSeconds = (currentTime - batch.StartTime).TotalSeconds;
                double durationSeconds = (double)batch.Duration;

                if (elapsedSeconds <= 0) continue;

                if (elapsedSeconds < durationSeconds)
                {
                                        decimal progress = (decimal)(elapsedSeconds / durationSeconds);
                    decimal currentBatchCooling = batch.TotalCooling * progress;
                    incrementalCooling += currentBatchCooling;

                    LogInfo($"高温物料批次 {batch.MaterialId}: 进度={progress:P1}, 已过={elapsedSeconds:F1}/{durationSeconds:F1}秒, " +
                           $"当前温降={currentBatchCooling:F3}℃, 批次总温降={batch.TotalCooling:F3}℃", taskCounter);
                }
                else
                {
                                        process.TotalCompletedCooling += batch.TotalCooling;
                    incrementalCooling += batch.TotalCooling;
                    completedBatches.Add(batch);

                    LogInfo($"高温物料批次 {batch.MaterialId}: 已完成, 加入总完成温降={batch.TotalCooling:F3}℃, " +
                           $"累计完成温降={process.TotalCompletedCooling:F3}℃", taskCounter);
                }
            }

                        process.MaterialBatches.RemoveAll(completedBatches.Contains);

            LogInfo($"高温阶段物料温降合计: 累计完成={process.TotalCompletedCooling:F3}℃, 本次增量={incrementalCooling:F3}℃", taskCounter);

                        return process.TotalCompletedCooling + incrementalCooling;
        }


        private static async Task<bool> CheckTappingStartEvent(OracleConnection connection, OracleTransaction transaction, string taskCounter)
        {
            string sql = $@"
        SELECT COUNT(*) 
        FROM {_currentSchema}.PGM_EVENTS 
        WHERE TEXT LIKE '%出钢开始 / Start Tapping%' 
          AND TASK_COUNTER = :task_counter";

            try
            {
                using (var command = new OracleCommand(sql, connection))
                {
                    command.Transaction = transaction;
                    command.Parameters.Add(new OracleParameter("task_counter", taskCounter));

                    var result = await command.ExecuteScalarAsync();
                    int count = result != null && result != DBNull.Value ? Convert.ToInt32(result) : 0;

                    if (count > 0)
                    {
                        LogInfo($"检测到任务 {taskCounter} 出钢开始事件", taskCounter);

                                                if (ChartFormManager.IsWindowShown(taskCounter) || _chartWindowsShown.ContainsKey(taskCounter))
                        {
                            LogInfo($"出钢事件触发，立即关闭温度监控弹窗 - 任务: {taskCounter}", taskCounter);

                                                        try
                            {
                                ChartFormManager.CloseTemperatureChart(taskCounter);
                            }
                            catch (Exception closeEx)
                            {
                                LogError($"关闭弹窗失败: {closeEx.Message}", closeEx, taskCounter);
                            }

                                                        _chartWindowsShown.TryRemove(taskCounter, out _);
                        }

                        return true;
                    }
                }
            }
            catch (Exception ex)
            {
                LogError($"检测出钢开始事件失败: {ex.Message}", ex, taskCounter);
            }

            return false;
        }


        private static void ClearTaskCache(string taskCounter)
        {
                        _lowTempProcessCache.TryRemove(taskCounter, out _);
            _highTempProcessCache.TryRemove(taskCounter, out _);
            _taskActiveStatus.TryRemove(taskCounter, out _);
            _latestDataCache.TryRemove(taskCounter, out _);
            _waitingCoolingCache.TryRemove(taskCounter, out _);
            _tappingStartedCache.TryRemove(taskCounter, out _);
            _mbTemperatureOxygenCache.TryRemove(taskCounter, out _);
            _chartWindowsShown.TryRemove(taskCounter, out _);
            _firstBlowingTimeCache.TryRemove(taskCounter, out _);             _sbTemperatureStateCache.TryRemove(taskCounter, out _); 
                        var pendingToRemove = _pendingUpdates.Where(kv => kv.Value.taskCounter == taskCounter)
                                                .Select(kv => kv.Key)
                                                .ToList();
            foreach (long msgId in pendingToRemove)
            {
                _pendingUpdates.TryRemove(msgId, out _);
            }

            LogInfo($"已清理任务{taskCounter}的所有缓存", taskCounter);
        }

        private static async Task UpdateRb1ExpectedBathTemperature(
            OracleConnection connection, OracleTransaction transaction,
            string taskCounter, decimal calculatedTemperature)
        {
            if (string.IsNullOrEmpty(taskCounter) || calculatedTemperature <= 0)
            {
                return;
            }

            try
            {
                string updateSql = $@"
                    UPDATE {GetTableName(TableConfig.PeriodDataRB1Table)} 
                    SET {TableConfig.ExpBathTempColumn} = :calculatedTemperature 
                    WHERE {TableConfig.TaskCounterColumn} = :taskCounter";

                using (var command = new OracleCommand(updateSql, connection))
                {
                    command.Transaction = transaction;
                    command.Parameters.Add(new OracleParameter("calculatedTemperature", calculatedTemperature));
                    command.Parameters.Add(new OracleParameter("taskCounter", taskCounter));

                    int rowsAffected = await command.ExecuteNonQueryAsync();

                    if (rowsAffected > 0)
                    {
                        LogInfo($"更新RB1表预期浴温: 任务={taskCounter}, 温度={calculatedTemperature:F2}℃");
                    }
                    else
                    {
                        await InsertRb1ExpectedBathTemperature(connection, transaction, taskCounter, calculatedTemperature);
                    }
                }
            }
            catch (Exception ex)
            {
                LogError($"更新RB1表预期浴温失败: {ex.Message}");
            }
        }

        private static async Task InsertRb1ExpectedBathTemperature(
            OracleConnection connection, OracleTransaction transaction,
            string taskCounter, decimal calculatedTemperature)
        {
            try
            {
                string sql = $@"
                    INSERT INTO {GetTableName(TableConfig.PeriodDataRB1Table)} 
                    ({TableConfig.TaskCounterColumn}, {TableConfig.ExpBathTempColumn}, CREATE_TIME, UPDATE_TIME) 
                    VALUES (:taskCounter, :calculatedTemperature, SYSDATE, SYSDATE)";

                using (var command = new OracleCommand(sql, connection))
                {
                    command.Transaction = transaction;
                    command.Parameters.Add(new OracleParameter("taskCounter", taskCounter));
                    command.Parameters.Add(new OracleParameter("calculatedTemperature", calculatedTemperature));

                    int rowsAffected = await command.ExecuteNonQueryAsync();

                    if (rowsAffected > 0)
                    {
                        LogInfo($"插入RB1表预期浴温: 任务={taskCounter}, 温度={calculatedTemperature:F2}℃");
                    }
                }
            }
            catch (Exception ex)
            {
                LogError($"插入RB1表预期浴温失败: {ex.Message}");
            }
        }
        private static async Task<decimal> GetBaseTemperature(OracleConnection connection, OracleTransaction transaction, string taskCounter)
        {
            string sql = $@"
                SELECT {TableConfig.TempColumn} 
                FROM {GetTableName(TableConfig.InputHMDataTable)} 
                WHERE {TableConfig.TaskCounterColumn} = :taskCounter 
                  AND {TableConfig.TypeColumn} = 4 
                  AND {TableConfig.TempColumn} IS NOT NULL";

            using (var command = new OracleCommand(sql, connection))
            {
                command.Transaction = transaction;
                command.Parameters.Add(new OracleParameter("taskCounter", taskCounter));

                var result = await command.ExecuteScalarAsync();
                return result != null && result != DBNull.Value ? Convert.ToDecimal(result) : 0;
            }
        }

        private static async Task<decimal> GetTargetTemperature(OracleConnection connection, OracleTransaction transaction, string taskCounter)
        {
            string sql = $@"
                SELECT {TableConfig.ExpBathTempColumn} 
                FROM {GetTableName(TableConfig.PeriodDataMBTable)} 
                WHERE {TableConfig.TaskCounterColumn} = :taskCounter";

            using (var command = new OracleCommand(sql, connection))
            {
                command.Transaction = transaction;
                command.Parameters.Add(new OracleParameter("taskCounter", taskCounter));

                var result = await command.ExecuteScalarAsync();
                return result != null && result != DBNull.Value ? Convert.ToDecimal(result) : 0;
            }
        }

        private static async Task<decimal> GetCalcOxygenVolume(OracleConnection connection, OracleTransaction transaction, string taskCounter)
        {
            string sql = $@"
                SELECT {TableConfig.ReqO2VolColumn} 
                FROM {GetTableName(TableConfig.PeriodDataMBTable)} 
                WHERE {TableConfig.TaskCounterColumn} = :taskCounter";

            using (var command = new OracleCommand(sql, connection))
            {
                command.Transaction = transaction;
                command.Parameters.Add(new OracleParameter("taskCounter", taskCounter));

                var result = await command.ExecuteScalarAsync();
                return result != null && result != DBNull.Value ? Convert.ToDecimal(result) : 0;
            }
        }

        private static async Task<decimal> GetFirstBathTemp(OracleConnection connection, OracleTransaction transaction, string taskCounter)
        {
            string sql = $@"
                SELECT {TableConfig.FirstBathTempColumn} 
                FROM {GetTableName(TableConfig.PeriodDataMBTable)} 
                WHERE {TableConfig.TaskCounterColumn} = :taskCounter";

            try
            {
                using (var command = new OracleCommand(sql, connection))
                {
                    command.Transaction = transaction;
                    command.Parameters.Add(new OracleParameter("taskCounter", taskCounter));

                    var result = await command.ExecuteScalarAsync();
                    return result != null && result != DBNull.Value ? Convert.ToDecimal(result) : 0;
                }
            }
            catch (Exception ex)
            {
                LogError($"获取{TableConfig.FirstBathTempColumn}失败: {ex.Message}");
                return 0;
            }
        }

        private static async Task<decimal> GetSBReqO2Vol(OracleConnection connection, OracleTransaction transaction, string taskCounter)
        {
            string sql = $@"
                SELECT {TableConfig.ReqO2VolColumn} 
                FROM {GetTableName(TableConfig.PeriodDataSBTable)} 
                WHERE {TableConfig.TaskCounterColumn} = :taskCounter";

            try
            {
                using (var command = new OracleCommand(sql, connection))
                {
                    command.Transaction = transaction;
                    command.Parameters.Add(new OracleParameter("taskCounter", taskCounter));

                    var result = await command.ExecuteScalarAsync();
                    if (result != null && result != DBNull.Value)
                    {
                        return Convert.ToDecimal(result);
                    }
                    return await GetMBReqO2Vol(connection, transaction, taskCounter);
                }
            }
            catch (Exception ex)
            {
                LogError($"获取SB表需求氧量失败: {ex.Message}");
                return await GetMBReqO2Vol(connection, transaction, taskCounter);
            }
        }

        private static async Task<decimal> GetMBReqO2Vol(OracleConnection connection, OracleTransaction transaction, string taskCounter)
        {
            string sql = $@"
                SELECT {TableConfig.ReqO2VolColumn} 
                FROM {GetTableName(TableConfig.PeriodDataMBTable)} 
                WHERE {TableConfig.TaskCounterColumn} = :taskCounter";

            using (var command = new OracleCommand(sql, connection))
            {
                command.Transaction = transaction;
                command.Parameters.Add(new OracleParameter("taskCounter", taskCounter));

                var result = await command.ExecuteScalarAsync();
                return result != null && result != DBNull.Value ? Convert.ToDecimal(result) : 0;
            }
        }

        
        private static async Task<AllMaterialData> GetAllMaterialData(
    OracleConnection connection, OracleTransaction transaction, string taskCounter)
        {
            var allData = new AllMaterialData();

            try
            {
                                bool sbHasTemperature = false;
                decimal sbFirstBathTemp = await GetFirstBathTempSB(connection, transaction, taskCounter);
                sbHasTemperature = sbFirstBathTemp > 0;

                if (_sbTemperatureStateCache.TryGetValue(taskCounter, out var sbState))
                {
                    sbHasTemperature = sbState.IsInitialized;
                }

                LogInfo($"高温阶段物料数据获取: 任务={taskCounter}, SB表有温度={sbHasTemperature}, SB表温度={sbFirstBathTemp:F1}℃", taskCounter);

                                int presetType = await GetHighTempCoolantPreset(connection, transaction);
                string mainCoolantMaterialId = await GetMainCoolantMaterialId(connection, transaction, presetType);

                                var coolantMaterials = await GetAllCoolantMaterials(connection, transaction);
                Dictionary<string, decimal> incrementalWeights = new Dictionary<string, decimal>();

                if (sbHasTemperature && sbState != null && sbState.IsInitialized)
                {
                                        LogInfo($"SB表有温度，从RB1_REP_WGT获取增量重量", taskCounter);
                    foreach (var material in coolantMaterials.Keys)
                    {
                        decimal rb1Weight = await GetMaterialRB1Weight(connection, transaction, taskCounter, material);
                        decimal initialWeight = sbState.InitialMaterialWeights.ContainsKey(material) ?
                            sbState.InitialMaterialWeights[material] : 0;
                        decimal incrementalWeight = Math.Max(0, rb1Weight - initialWeight);

                        incrementalWeights[material] = incrementalWeight;

                        if (incrementalWeight > 0)
                        {
                            LogInfo($"物料增量计算[{material}]: RB1重量={rb1Weight:F1}kg, " +
                                   $"初始重量={initialWeight:F1}kg, 增量={incrementalWeight:F1}kg", taskCounter);
                        }
                    }
                }
                else
                {
                                        LogInfo($"SB表没有温度，从SB_REP_WGT获取总重量", taskCounter);
                    foreach (var material in coolantMaterials.Keys)
                    {
                        decimal sbWeight = await GetMaterialSBWeight(connection, transaction, taskCounter, material);
                        incrementalWeights[material] = sbWeight;

                        if (sbWeight > 0)
                        {
                            LogInfo($"物料总重量[{material}]: {sbWeight:F1}kg (SB_REP_WGT)", taskCounter);
                        }
                    }
                }

                                if (!string.IsNullOrEmpty(mainCoolantMaterialId))
                {
                                        decimal calcWeight = await GetMainCoolantCalcWeight(connection, transaction, taskCounter,
                        mainCoolantMaterialId, true);

                                        decimal actualWeight = incrementalWeights.ContainsKey(mainCoolantMaterialId) ?
                        incrementalWeights[mainCoolantMaterialId] : 0;

                    if (calcWeight > 0 || actualWeight > 0)
                    {
                                                decimal calcCooling = await CalculateMainCoolantTotalCooling(connection, transaction, taskCounter,
                            mainCoolantMaterialId, calcWeight, true);

                                                decimal actualCooling = await CalculateMainCoolantTotalCooling(connection, transaction, taskCounter,
                            mainCoolantMaterialId, actualWeight, true);

                        decimal meltingRate = await GetMainCoolantMeltingRate(connection, transaction);

                        allData.MainCoolantData = new MaterialData
                        {
                            MaterialId = mainCoolantMaterialId,
                            MaterialName = "主冷却剂",
                            MaterialType = "COOLANT",
                            CalcWeight = calcWeight,                                  ActualWeight = actualWeight,                              CalcCooling = calcCooling,                                ActualCooling = actualCooling,                            MeltingRate = meltingRate
                        };

                        LogInfo($"主冷却剂数据: 计算重量={calcWeight:F1}kg, 计算温降={calcCooling:F3}℃, " +
                               $"实际重量={actualWeight:F1}kg, 实际温降={actualCooling:F3}℃", taskCounter);
                    }
                    else
                    {
                                                allData.MainCoolantData = new MaterialData
                        {
                            MaterialId = mainCoolantMaterialId ?? "DEFAULT_CA",
                            MaterialName = "主冷却剂(默认)",
                            MaterialType = "COOLANT",
                            CalcWeight = 0m,
                            ActualWeight = 0m,
                            CalcCooling = 0m,
                            ActualCooling = 0m,
                            MeltingRate = await GetMainCoolantMeltingRate(connection, transaction)
                        };
                    }
                }

                                foreach (var material in coolantMaterials.Keys)
                {
                    if (material != mainCoolantMaterialId)
                    {
                        decimal actualWeight = incrementalWeights.ContainsKey(material) ? incrementalWeights[material] : 0;

                        if (actualWeight > 0)
                        {
                            int sdmIdx = coolantMaterials[material];

                                                        decimal calcWeight = await GetMaterialCalcWeight(connection, transaction, taskCounter, material, true);

                                                        decimal calcCooling = await CalculateMaterialTotalCooling(
                                connection, transaction, taskCounter, material, calcWeight, sdmIdx, true);

                                                        decimal actualCooling = await CalculateMaterialTotalCooling(
                                connection, transaction, taskCounter, material, actualWeight, sdmIdx, true);

                            string materialName = CoolantMaterialMap.ContainsKey(sdmIdx) ?
                                CoolantMaterialMap[sdmIdx].EffectChar : "未知物料";

                            var materialData = new MaterialData
                            {
                                MaterialId = material,
                                MaterialName = materialName,
                                MaterialType = "OTHER",
                                CalcWeight = calcWeight,
                                ActualWeight = actualWeight,
                                CalcCooling = calcCooling,
                                ActualCooling = actualCooling,
                                MeltingRate = await GetMaterialMeltingRate(connection, transaction, sdmIdx)
                            };

                            allData.OtherMaterials.Add(materialData);

                            LogInfo($"其他物料[{materialName}]: 计算重量={calcWeight:F1}kg, 计算温降={calcCooling:F3}℃, " +
                                   $"实际重量={actualWeight:F1}kg, 实际温降={actualCooling:F3}℃", taskCounter);
                        }
                    }
                }

                LogInfo($"=== 高温阶段物料数据获取完成 - 任务={taskCounter} ===", taskCounter);
                LogInfo($"SB表有温度状态: {sbHasTemperature}", taskCounter);
                LogInfo($"主冷却剂计算温降: {allData.MainCoolantData?.CalcCooling ?? 0:F3}℃ (用于氧气升温系数)", taskCounter);
                LogInfo($"主冷却剂实际温降: {allData.MainCoolantData?.ActualCooling ?? 0:F3}℃ (用于累计物料温降)", taskCounter);
                LogInfo($"物料总计算温降: {(allData.MainCoolantData?.CalcCooling ?? 0) + allData.OtherMaterials.Sum(m => m.CalcCooling):F3}℃", taskCounter);
                LogInfo($"物料总实际温降: {(allData.MainCoolantData?.ActualCooling ?? 0) + allData.OtherMaterials.Sum(m => m.ActualCooling):F3}℃", taskCounter);

            }
            catch (Exception ex)
            {
                LogError($"获取高温阶段物料数据失败: {ex.Message}", ex, taskCounter);
            }

            return allData;
        }

        private static async Task<decimal> GetMaterialSBWeight(
    OracleConnection connection, OracleTransaction transaction,
    string taskCounter, string materialId)
        {
            string sql = $@"
        SELECT NVL({TableConfig.SBRepWgtColumn}, 0) 
        FROM {GetTableName(TableConfig.AdditionsTable)} 
        WHERE {TableConfig.TaskCounterColumn} = :taskCounter 
          AND {TableConfig.MaterialIdColumn} = :materialId";

            try
            {
                using (var command = new OracleCommand(sql, connection))
                {
                    command.Transaction = transaction;
                    command.Parameters.Add(new OracleParameter("taskCounter", taskCounter));
                    command.Parameters.Add(new OracleParameter("materialId", materialId));

                    var result = await command.ExecuteScalarAsync();
                    return result != null && result != DBNull.Value ? Convert.ToDecimal(result) : 0;
                }
            }
            catch (Exception ex)
            {
                LogError($"获取物料SB重量失败: {ex.Message}", null, taskCounter);
                return 0;
            }
        }

        private static async Task<decimal> GetMaterialCalcWeight(
    OracleConnection connection, OracleTransaction transaction,
    string taskCounter, string materialId, bool isHighTemp)
        {
            string weightColumn = isHighTemp ? TableConfig.SBCalcWgtColumn : TableConfig.MBCalcWgtColumn;

            string sql = $@"
        SELECT NVL({weightColumn}, 0) 
        FROM {GetTableName(TableConfig.AdditionsTable)} 
        WHERE {TableConfig.TaskCounterColumn} = :taskCounter 
          AND {TableConfig.MaterialIdColumn} = :materialId";

            try
            {
                using (var command = new OracleCommand(sql, connection))
                {
                    command.Transaction = transaction;
                    command.Parameters.Add(new OracleParameter("taskCounter", taskCounter));
                    command.Parameters.Add(new OracleParameter("materialId", materialId));

                    var result = await command.ExecuteScalarAsync();
                    return result != null && result != DBNull.Value ? Convert.ToDecimal(result) : 0;
                }
            }
            catch (Exception ex)
            {
                LogError($"获取物料计算重量失败: {ex.Message}");
                return 0;
            }
        }

        
        private static async Task<AllMaterialData> GetAllMaterialDataForLowTemp(
    OracleConnection connection, OracleTransaction transaction, string taskCounter)
        {
            var allData = new AllMaterialData();

                        int presetType = await GetLowTempCoolantPreset(connection, transaction);
            string mainCoolantMaterialId = await GetMainCoolantMaterialId(connection, transaction, presetType);

                        decimal mainCalcWeight = await GetMainCoolantCalcWeight(connection, transaction, taskCounter,
                mainCoolantMaterialId, false);
            decimal mainCalcCooling = mainCalcWeight > 0 ?
                await CalculateMainCoolantTotalCooling(connection, transaction, taskCounter,
                    mainCoolantMaterialId, mainCalcWeight, false) : 0m;

                        decimal mainActualWeight = await GetMainCoolantActualWeight(connection, transaction, taskCounter,
                mainCoolantMaterialId, false);
            decimal mainActualCooling = mainActualWeight > 0 ?
                await CalculateMainCoolantTotalCooling(connection, transaction, taskCounter,
                    mainCoolantMaterialId, mainActualWeight, false) : 0m;

            decimal mainMeltingRate = await GetMainCoolantMeltingRate(connection, transaction);

            allData.MainCoolantData = new MaterialData
            {
                MaterialId = mainCoolantMaterialId,
                MaterialName = "主冷却剂",
                MaterialType = "COOLANT",
                CalcWeight = mainCalcWeight,                          ActualWeight = mainActualWeight,                      CalcCooling = mainCalcCooling,                        ActualCooling = mainActualCooling,                    MeltingRate = mainMeltingRate
            };

                        var coolantMaterials = await GetAllCoolantMaterials(connection, transaction);

                        var calcWeights = await GetMaterialWeightsByColumn(connection, transaction,
                taskCounter, coolantMaterials, TableConfig.MBCalcWgtColumn, "计算重量");

                        var actualWeights = await GetMaterialWeightsByColumn(connection, transaction,
                taskCounter, coolantMaterials, TableConfig.MBRepWgtColumn, "实际重量");

            foreach (var material in coolantMaterials.Keys)
            {
                if (material != mainCoolantMaterialId)
                {
                    decimal calcWeight = calcWeights.GetValueOrDefault(material);
                    decimal actualWeight = actualWeights.GetValueOrDefault(material);

                                        decimal calcCooling = 0m;
                    if (calcWeight > 0)
                    {
                        int sdmIdx = coolantMaterials[material];
                        calcCooling = await CalculateMaterialTotalCooling(
                            connection, transaction, taskCounter, material, calcWeight, sdmIdx, false);
                    }

                                        decimal actualCooling = 0m;
                    if (actualWeight > 0)
                    {
                        int sdmIdx = coolantMaterials[material];
                        actualCooling = await CalculateMaterialTotalCooling(
                            connection, transaction, taskCounter, material, actualWeight, sdmIdx, false);
                    }

                    string materialName = CoolantMaterialMap.ContainsKey(coolantMaterials[material]) ?
                        CoolantMaterialMap[coolantMaterials[material]].EffectChar : "未知物料";

                    var materialData = new MaterialData
                    {
                        MaterialId = material,
                        MaterialName = materialName,
                        MaterialType = "OTHER",
                        CalcWeight = calcWeight,
                        ActualWeight = actualWeight,
                        CalcCooling = calcCooling,
                        ActualCooling = actualCooling,
                        MeltingRate = await GetMaterialMeltingRate(connection, transaction, coolantMaterials[material])
                    };

                    allData.OtherMaterials.Add(materialData);
                }
            }

            return allData;
        }

        private static async Task<Dictionary<string, decimal>> GetMaterialWeightsByColumn(
    OracleConnection connection, OracleTransaction transaction,
    string taskCounter, Dictionary<string, int> coolantMaterials,
    string weightColumn, string weightType)
        {
            var weights = new Dictionary<string, decimal>();

            if (!coolantMaterials.Any()) return weights;

            const int batchSize = 1000;
            var materialIds = coolantMaterials.Keys.ToList();

            for (int i = 0; i < materialIds.Count; i += batchSize)
            {
                var batchMaterials = materialIds.Skip(i).Take(batchSize).ToList();

                string sql = $@"
            SELECT {TableConfig.MaterialIdColumn}, NVL({weightColumn}, 0) 
            FROM {GetTableName(TableConfig.AdditionsTable)} 
            WHERE {TableConfig.TaskCounterColumn} = :taskCounter 
              AND {TableConfig.MaterialIdColumn} IN (" +
                          string.Join(",", batchMaterials.Select(m => $"'{m}'")) + ")";

                try
                {
                    using (var command = new OracleCommand(sql, connection))
                    {
                        command.Transaction = transaction;
                        command.Parameters.Add(new OracleParameter("taskCounter", taskCounter));

                        using (var reader = await command.ExecuteReaderAsync())
                        {
                            while (await reader.ReadAsync())
                            {
                                string materialId = reader.GetString(0);
                                decimal weight = reader.GetDecimal(1);
                                weights[materialId] = weight;
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    LogError($"获取低温阶段物料{weightType}失败: {ex.Message}");
                }
            }

                        foreach (var material in coolantMaterials.Keys)
            {
                if (!weights.ContainsKey(material))
                {
                    weights[material] = 0m;
                }
            }

            return weights;
        }

        private static async Task<Dictionary<string, decimal>> GetCurrentMaterialWeightsForLowTemp(
    OracleConnection connection, OracleTransaction transaction, string taskCounter,
    Dictionary<string, int> coolantMaterials)
        {
            var currentWeights = new Dictionary<string, decimal>();

            if (!coolantMaterials.Any()) return currentWeights;

            const int batchSize = 1000;
            var materialIds = coolantMaterials.Keys.ToList();

            for (int i = 0; i < materialIds.Count; i += batchSize)
            {
                var batchMaterials = materialIds.Skip(i).Take(batchSize).ToList();

                string sql = $@"
            SELECT {TableConfig.MaterialIdColumn}, NVL({TableConfig.MBRepWgtColumn}, 0) 
            FROM {GetTableName(TableConfig.AdditionsTable)} 
            WHERE {TableConfig.TaskCounterColumn} = :taskCounter 
              AND {TableConfig.MaterialIdColumn} IN (" + string.Join(",", batchMaterials.Select(m => $"'{m}'")) + ")";

                try
                {
                    using (var command = new OracleCommand(sql, connection))
                    {
                        command.Transaction = transaction;
                        command.Parameters.Add(new OracleParameter("taskCounter", taskCounter));

                        using (var reader = await command.ExecuteReaderAsync())
                        {
                            while (await reader.ReadAsync())
                            {
                                string materialId = reader.GetString(0);
                                decimal weight = reader.GetDecimal(1);
                                currentWeights[materialId] = weight;
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    LogError($"获取低温阶段当前物料重量失败: {ex.Message}");
                }
            }

            foreach (var material in coolantMaterials.Keys)
            {
                if (!currentWeights.ContainsKey(material))
                {
                    currentWeights[material] = 0;
                }
            }

            return currentWeights;
        }

        private static async Task<int> GetLowTempCoolantPreset(OracleConnection connection, OracleTransaction transaction)
        {
            string sql = $@"
        SELECT {TableConfig.IntegerValueColumn} 
        FROM {GetTableName(TableConfig.AuxConstantsTable)} 
        WHERE {TableConfig.ConstantIdColumn} = 'FCLCALMBL_PRESET'";

            try
            {
                using (var command = new OracleCommand(sql, connection))
                {
                    command.Transaction = transaction;

                    var result = await command.ExecuteScalarAsync();
                    if (result != null && result != DBNull.Value)
                    {
                        return Convert.ToInt32(result);
                    }
                }
            }
            catch (Exception ex)
            {
                LogError($"获取低温阶段主冷却剂预设类型失败: {ex.Message}");
            }

            return 0;
        }

        private static async Task<int> GetHighTempCoolantPreset(OracleConnection connection, OracleTransaction transaction)
        {
            string sql = $@"
        SELECT {TableConfig.IntegerValueColumn} 
        FROM {GetTableName(TableConfig.AuxConstantsTable)} 
        WHERE {TableConfig.ConstantIdColumn} = 'FCLCALDCP_PRESET'";

            try
            {
                using (var command = new OracleCommand(sql, connection))
                {
                    command.Transaction = transaction;

                    var result = await command.ExecuteScalarAsync();
                    if (result != null && result != DBNull.Value)
                    {
                        return Convert.ToInt32(result);
                    }
                }
            }
            catch (Exception ex)
            {
                LogError($"获取高温阶段主冷却剂预设类型失败: {ex.Message}");
            }

            return 0;
        }

        private static async Task<string> GetMainCoolantMaterialId(OracleConnection connection, OracleTransaction transaction, int presetType)
        {
            if (!PresetToMaterialMap.ContainsKey(presetType))
            {
                LogError($"无效的预设类型: {presetType}，无法获取主冷却剂物料ID");
                return null;
            }

            var presetInfo = PresetToMaterialMap[presetType];

            string sql = $@"
        SELECT {TableConfig.MatIdColumn}
        FROM {GetTableName(TableConfig.MaterialModelTable)} 
        WHERE {TableConfig.SdmIdxColumn} = :sdmIdx 
        AND ROWNUM = 1";

            try
            {
                using (var command = new OracleCommand(sql, connection))
                {
                    command.Transaction = transaction;
                    command.Parameters.Add(new OracleParameter("sdmIdx", presetInfo.SdmIdx));

                    var result = await command.ExecuteScalarAsync();
                    if (result != null && result != DBNull.Value)
                    {
                        string materialId = result.ToString();
                        return materialId;
                    }
                    else
                    {
                        LogError($"未找到SDM_IDX={presetInfo.SdmIdx}对应的物料ID");
                        return null;
                    }
                }
            }
            catch (Exception ex)
            {
                LogError($"获取主冷却剂物料ID失败: 预设类型={presetType}, SDM_IDX={presetInfo.SdmIdx}");
                return null;
            }
        }

        private static async Task<decimal> GetMainCoolantMeltingRate(OracleConnection connection, OracleTransaction transaction)
        {
            string sql = $@"
        SELECT {TableConfig.FloatValueColumn} 
        FROM {GetTableName(TableConfig.AuxValuesTable)} 
        WHERE {TableConfig.CharValueColumn} = 'DisrOre'";

            try
            {
                using (var command = new OracleCommand(sql, connection))
                {
                    command.Transaction = transaction;

                    var result = await command.ExecuteScalarAsync();
                    if (result != null && result != DBNull.Value)
                    {
                        return Convert.ToDecimal(result);
                    }

                    return 10m;
                }
            }
            catch (Exception ex)
            {
                LogError($"获取主冷却剂熔化速率失败: {ex.Message}");
                return 10m;
            }
        }

        private static async Task<decimal> GetMainCoolantCalcWeight(OracleConnection connection, OracleTransaction transaction, string taskCounter, string materialId, bool isHighTemp)
        {
            string weightColumn = isHighTemp ? TableConfig.SBCalcWgtColumn : TableConfig.MBCalcWgtColumn;

            string sql = $@"
                SELECT NVL({weightColumn}, 0) 
                FROM {GetTableName(TableConfig.AdditionsTable)} 
                WHERE {TableConfig.TaskCounterColumn} = :taskCounter 
                  AND {TableConfig.MaterialIdColumn} = :materialId";

            try
            {
                using (var command = new OracleCommand(sql, connection))
                {
                    command.Transaction = transaction;
                    command.Parameters.Add(new OracleParameter("taskCounter", taskCounter));
                    command.Parameters.Add(new OracleParameter("materialId", materialId));

                    var result = await command.ExecuteScalarAsync();
                    return result != null && result != DBNull.Value ? Convert.ToDecimal(result) : 0;
                }
            }
            catch (Exception ex)
            {
                LogError($"获取主冷却剂{(isHighTemp ? "高温" : "低温")}阶段计算重量失败: {ex.Message}");
                return 0;
            }
        }

        private static async Task<decimal> GetMainCoolantActualWeight(OracleConnection connection, OracleTransaction transaction, string taskCounter, string materialId, bool isHighTemp)
        {
            string weightColumn = isHighTemp ? TableConfig.SBRepWgtColumn : TableConfig.MBRepWgtColumn;

            string sql = $@"
                SELECT NVL({weightColumn}, 0) 
                FROM {GetTableName(TableConfig.AdditionsTable)} 
                WHERE {TableConfig.TaskCounterColumn} = :taskCounter 
                  AND {TableConfig.MaterialIdColumn} = :materialId";

            try
            {
                using (var command = new OracleCommand(sql, connection))
                {
                    command.Transaction = transaction;
                    command.Parameters.Add(new OracleParameter("taskCounter", taskCounter));
                    command.Parameters.Add(new OracleParameter("materialId", materialId));

                    var result = await command.ExecuteScalarAsync();
                    return result != null && result != DBNull.Value ? Convert.ToDecimal(result) : 0;
                }
            }
            catch (Exception ex)
            {
                LogError($"获取主冷却剂{(isHighTemp ? "高温" : "低温")}阶段实际重量失败: {ex.Message}");
                return 0;
            }
        }

        private static async Task<decimal> CalculateMainCoolantTotalCooling(
    OracleConnection connection, OracleTransaction transaction, string taskCounter,
    string mainCoolantMaterialId, decimal weight, bool isHighTemp)
        {
            try
            {
                if (weight <= 0)
                {
                    LogInfo($"主冷却剂计算: 任务={taskCounter}, 重量={weight:F1}kg ≤ 0, 跳过计算");
                    return 0;
                }

                (decimal steelWeight, decimal slagWeight) = isHighTemp ?
                    await GetSteelAndSlagWeightsForHighTemp(connection, transaction, taskCounter) :
                    await GetSteelAndSlagWeightsForLowTemp(connection, transaction, taskCounter);

                if (steelWeight <= 0)
                {
                    LogInfo($"主冷却剂计算: 任务={taskCounter}, 钢水重量={steelWeight:F1}kg ≤ 0, 跳过计算");
                    return 0;
                }

                decimal coolingEffect = await GetMainCoolantCoolingEffect(connection, transaction,
                    isHighTemp ? await GetHighTempCoolantPreset(connection, transaction) :
                                await GetLowTempCoolantPreset(connection, transaction));

                decimal steelSpecificHeat = await GetSteelSpecificHeat(connection, transaction);
                decimal slagSpecificHeat = await GetSlagSpecificHeat(connection, transaction);
                decimal totalHeatCapacity = (steelWeight * steelSpecificHeat) + (slagWeight * slagSpecificHeat);

                if (totalHeatCapacity == 0)
                {
                    LogInfo($"主冷却剂计算: 任务={taskCounter}, 总热容=0, 跳过计算");
                    return 0;
                }

                decimal weightInTons = weight / 1000m;
                decimal totalCoolingEffect = weightInTons * coolingEffect;
                decimal totalCooling = totalCoolingEffect / totalHeatCapacity;

                                LogInfo($"=== 主冷却剂温降计算过程 - 任务={taskCounter} ===");
                LogInfo($"阶段: {(isHighTemp ? "高温" : "低温")}");
                LogInfo($"物料ID: {mainCoolantMaterialId}");
                LogInfo($"输入参数:");
                LogInfo($"  - 冷却剂重量: {weight:F1}kg = {weightInTons:F3}吨");
                LogInfo($"  - 总热容量: ({steelWeight:F1} × {steelSpecificHeat:F3}) + ({slagWeight:F1} × {slagSpecificHeat:F3}) = {totalHeatCapacity:F1} kJ/℃");
                LogInfo($"  - 总冷却效应: {weightInTons:F3}吨 × {coolingEffect:F0} kJ/吨 = {totalCoolingEffect:F1} kJ");
                LogInfo($"  - 最终温降: {totalCoolingEffect:F1} kJ ÷ {totalHeatCapacity:F1} kJ/℃ = {totalCooling:F3}℃");
                LogInfo($"=== 主冷却剂温降计算完成 ===");

                return totalCooling;
            }
            catch (Exception ex)
            {
                LogError($"计算主冷却剂总降温值失败: {ex.Message}");
                return 0;
            }
        }

        private static async Task<decimal> GetMainCoolantCoolingEffect(OracleConnection connection, OracleTransaction transaction, int presetType)
        {
            if (!PresetToMaterialMap.ContainsKey(presetType))
            {
                return 3100m;
            }

            var presetInfo = PresetToMaterialMap[presetType];
            string sql = $@"
        SELECT {TableConfig.FloatValueColumn} 
        FROM {GetTableName(TableConfig.AuxValuesTable)} 
        WHERE {TableConfig.CharValueColumn} = :charValue";

            try
            {
                using (var command = new OracleCommand(sql, connection))
                {
                    command.Transaction = transaction;
                    command.Parameters.Add(new OracleParameter("charValue", presetInfo.MaterialType));

                    var result = await command.ExecuteScalarAsync();
                    if (result != null && result != DBNull.Value)
                    {
                        return Convert.ToDecimal(result);
                    }
                }
            }
            catch (Exception ex)
            {
                LogError($"获取主冷却剂冷却效应失败: {ex.Message}");
            }

            return 3100m;
        }

        private static async Task<(decimal steelWeight, decimal slagWeight)> GetSteelAndSlagWeightsForHighTemp(
            OracleConnection connection, OracleTransaction transaction, string taskCounter)
        {
            string sql = $@"
                SELECT {TableConfig.SteelWeight2Column}, {TableConfig.SlagWeight2Column} 
                FROM {GetTableName(TableConfig.LogDataTable)} 
                WHERE {TableConfig.StepColumn} = 1 
                  AND {TableConfig.SdmFunctionColumn} = 'DCPCAL' 
                  AND {TableConfig.TaskCounterColumn} = :taskCounter";

            try
            {
                using (var command = new OracleCommand(sql, connection))
                {
                    command.Transaction = transaction;
                    command.Parameters.Add(new OracleParameter("taskCounter", taskCounter));

                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        if (await reader.ReadAsync())
                        {
                            decimal steelWeight = reader.GetDecimal(0);
                            decimal slagWeight = reader.GetDecimal(1);
                            return (steelWeight, slagWeight);
                        }
                    }

                    return (0, 0);
                }
            }
            catch (Exception ex)
            {
                LogError($"获取高温阶段钢水和炉渣重量失败: {ex.Message}");
                return (0, 0);
            }
        }

        private static async Task<(decimal steelWeight, decimal slagWeight)> GetSteelAndSlagWeightsForLowTemp(
            OracleConnection connection, OracleTransaction transaction, string taskCounter)
        {
            string sql = $@"
                SELECT {TableConfig.SteelWeight1Column}, {TableConfig.SlagWeight1Column} 
                FROM {GetTableName(TableConfig.LogDataTable)} 
                WHERE {TableConfig.StepColumn} = 1 
                  AND {TableConfig.SdmFunctionColumn} = 'BLOCAL' 
                  AND {TableConfig.TaskCounterColumn} = :taskCounter";

            try
            {
                using (var command = new OracleCommand(sql, connection))
                {
                    command.Transaction = transaction;
                    command.Parameters.Add(new OracleParameter("taskCounter", taskCounter));

                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        if (await reader.ReadAsync())
                        {
                            decimal steelWeight = reader.GetDecimal(0);
                            decimal slagWeight = reader.GetDecimal(1);
                            return (steelWeight, slagWeight);
                        }
                    }

                    return (0, 0);
                }
            }
            catch (Exception ex)
            {
                LogError($"获取低温阶段钢水和炉渣重量失败: {ex.Message}");
                return (0, 0);
            }
        }

        private static async Task<decimal> GetSteelSpecificHeat(OracleConnection connection, OracleTransaction transaction)
        {
            string sql = $@"
                SELECT {TableConfig.FloatValueColumn} 
                FROM {GetTableName(TableConfig.AuxValuesTable)} 
                WHERE {TableConfig.CharValueColumn} = 'SPHST'";

            using (var command = new OracleCommand(sql, connection))
            {
                command.Transaction = transaction;

                var result = await command.ExecuteScalarAsync();
                return result != null && result != DBNull.Value ? Convert.ToDecimal(result) : 0.715m;
            }
        }

        private static async Task<decimal> GetSlagSpecificHeat(OracleConnection connection, OracleTransaction transaction)
        {
            string sql = $@"
                SELECT {TableConfig.FloatValueColumn} 
                FROM {GetTableName(TableConfig.AuxValuesTable)} 
                WHERE {TableConfig.CharValueColumn} = 'SPHSL'";

            using (var command = new OracleCommand(sql, connection))
            {
                command.Transaction = transaction;

                var result = await command.ExecuteScalarAsync();
                return result != null && result != DBNull.Value ? Convert.ToDecimal(result) : 1.191m;
            }
        }

        private static async Task<Dictionary<string, int>> GetAllCoolantMaterials(OracleConnection connection, OracleTransaction transaction)
        {
            var coolantMaterials = new Dictionary<string, int>();

            string sql = $@"
        SELECT {TableConfig.MatIdColumn}, {TableConfig.SdmIdxColumn}
        FROM {GetTableName(TableConfig.MaterialModelTable)} 
        WHERE {TableConfig.MatIdColumn} LIKE '%CA%'
          AND {TableConfig.SdmIdxColumn} IN (101, 103, 105, 106, 107, 108, 109, 112, 113)";

            try
            {
                using (var command = new OracleCommand(sql, connection))
                {
                    command.Transaction = transaction;

                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            string matId = reader.GetString(0);
                            int sdmIdx = reader.GetInt32(1);
                            coolantMaterials[matId] = sdmIdx;
                        }
                    }

                    return coolantMaterials;
                }
            }
            catch (Exception ex)
            {
                LogError($"获取冷却剂物料失败");
                return coolantMaterials;
            }
        }

        private static async Task<Dictionary<string, decimal>> GetCurrentMaterialWeights(
    OracleConnection connection, OracleTransaction transaction, string taskCounter,
    Dictionary<string, int> coolantMaterials)
        {
            var currentWeights = new Dictionary<string, decimal>();

            if (!coolantMaterials.Any()) return currentWeights;

            const int batchSize = 1000;
            var materialIds = coolantMaterials.Keys.ToList();

            for (int i = 0; i < materialIds.Count; i += batchSize)
            {
                var batchMaterials = materialIds.Skip(i).Take(batchSize).ToList();

                string sql = $@"
            SELECT {TableConfig.MaterialIdColumn}, NVL({TableConfig.SBRepWgtColumn}, 0) 
            FROM {GetTableName(TableConfig.AdditionsTable)} 
            WHERE {TableConfig.TaskCounterColumn} = :taskCounter 
              AND {TableConfig.MaterialIdColumn} IN (" + string.Join(",", batchMaterials.Select(m => $"'{m}'")) + ")";

                try
                {
                    using (var command = new OracleCommand(sql, connection))
                    {
                        command.Transaction = transaction;
                        command.Parameters.Add(new OracleParameter("taskCounter", taskCounter));

                        using (var reader = await command.ExecuteReaderAsync())
                        {
                            while (await reader.ReadAsync())
                            {
                                string materialId = reader.GetString(0);
                                decimal weight = reader.GetDecimal(1);
                                currentWeights[materialId] = weight;
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    LogError($"获取当前物料重量失败: {ex.Message}");
                }
            }

            foreach (var material in coolantMaterials.Keys)
            {
                if (!currentWeights.ContainsKey(material))
                {
                    currentWeights[material] = 0;
                }
            }

            return currentWeights;
        }

        private static async Task<decimal> GetMaterialMeltingRate(OracleConnection connection, OracleTransaction transaction, int sdmIdx)
        {
            if (!CoolantMaterialMap.ContainsKey(sdmIdx))
            {
                return 5m;
            }

            var materialInfo = CoolantMaterialMap[sdmIdx];
            string sql = $@"
        SELECT {TableConfig.FloatValueColumn} 
        FROM {GetTableName(TableConfig.AuxValuesTable)} 
        WHERE {TableConfig.CharValueColumn} = :charValue";

            try
            {
                using (var command = new OracleCommand(sql, connection))
                {
                    command.Transaction = transaction;
                    command.Parameters.Add(new OracleParameter("charValue", materialInfo.MeltingRateChar));

                    var result = await command.ExecuteScalarAsync();
                    if (result != null && result != DBNull.Value)
                    {
                        return Convert.ToDecimal(result);
                    }

                    return 5m;
                }
            }
            catch (Exception ex)
            {
                LogError($"获取物料熔化速率失败 SDM_IDX={sdmIdx}: {ex.Message}");
                return 5m;
            }
        }

        private static async Task<decimal> GetSlagHeatEffect(
    OracleConnection connection, OracleTransaction transaction, string scrapType)
        {
            try
            {
                string charValue = "SHSLGSCR" + scrapType.Substring(2);

                string sql = $@"
            SELECT {TableConfig.FloatValueColumn} 
            FROM {GetTableName(TableConfig.AuxValuesTable)} 
            WHERE {TableConfig.CharValueColumn} = :charValue";

                using (var command = new OracleCommand(sql, connection))
                {
                    command.Transaction = transaction;
                    command.Parameters.Add(new OracleParameter("charValue", charValue));

                    var result = await command.ExecuteScalarAsync();

                    if (result != null && result != DBNull.Value)
                    {
                        decimal heatEffect = Convert.ToDecimal(result);
                        return heatEffect;
                    }
                    else
                    {
                        return 2850m;
                    }
                }
            }
            catch (Exception ex)
            {
                LogError($"获取废钢{scrapType}炉渣热效应值失败: {ex.Message}，使用默认值2850");
                return 2850m;
            }
        }

        private static async Task<decimal> GetMaterialCoolingEffect(OracleConnection connection, OracleTransaction transaction, int sdmIdx)
        {
            if (!CoolantMaterialMap.ContainsKey(sdmIdx))
            {
                return 0;
            }

            var materialInfo = CoolantMaterialMap[sdmIdx];
            string sql = $@"
        SELECT {TableConfig.FloatValueColumn} 
        FROM {GetTableName(TableConfig.AuxValuesTable)} 
        WHERE {TableConfig.CharValueColumn} = :charValue";

            try
            {
                using (var command = new OracleCommand(sql, connection))
                {
                    command.Transaction = transaction;
                    command.Parameters.Add(new OracleParameter("charValue", materialInfo.EffectChar));

                    var result = await command.ExecuteScalarAsync();
                    if (result != null && result != DBNull.Value)
                    {
                        decimal coolingEffect = Convert.ToDecimal(result);
                        return coolingEffect;
                    }

                    return 0;
                }
            }
            catch (Exception ex)
            {
                LogError($"获取物料冷却效应失败 SDM_IDX={sdmIdx}: {ex.Message}");
                return 0;
            }
        }

        private static async Task<decimal> CalculateMaterialTotalCooling(
    OracleConnection connection, OracleTransaction transaction, string taskCounter,
    string materialId, decimal weight, int sdmIdx, bool isHighTemp)
        {
            try
            {
                if (weight <= 0)
                {
                    LogInfo($"其他物料计算: 任务={taskCounter}, 物料={materialId}, 重量={weight:F1}kg ≤ 0, 跳过计算");
                    return 0;
                }

                (decimal steelWeight, decimal slagWeight) = isHighTemp ?
                    await GetSteelAndSlagWeightsForHighTemp(connection, transaction, taskCounter) :
                    await GetSteelAndSlagWeightsForLowTemp(connection, transaction, taskCounter);

                if (steelWeight <= 0)
                {
                    LogInfo($"其他物料计算: 任务={taskCounter}, 物料={materialId}, 钢水重量={steelWeight:F1}kg ≤ 0, 跳过计算");
                    return 0;
                }

                decimal coolingEffect = await GetMaterialCoolingEffect(connection, transaction, sdmIdx);
                decimal steelSpecificHeat = await GetSteelSpecificHeat(connection, transaction);
                decimal slagSpecificHeat = await GetSlagSpecificHeat(connection, transaction);

                decimal totalHeatCapacity = (steelWeight * steelSpecificHeat) + (slagWeight * slagSpecificHeat);

                if (totalHeatCapacity == 0)
                {
                    LogInfo($"其他物料计算: 任务={taskCounter}, 物料={materialId}, 总热容=0, 跳过计算");
                    return 0;
                }

                decimal weightInTons = weight / 1000m;
                decimal totalCoolingEffect = weightInTons * coolingEffect;
                decimal totalCooling = totalCoolingEffect / totalHeatCapacity;

                                string materialName = CoolantMaterialMap.ContainsKey(sdmIdx) ?
                    CoolantMaterialMap[sdmIdx].EffectChar : "未知物料";

                                LogInfo($"=== 其他物料温降计算过程 - 任务={taskCounter} ===");
                LogInfo($"阶段: {(isHighTemp ? "高温" : "低温")}");
                LogInfo($"物料: {materialName} (ID: {materialId}, SDM_IDX: {sdmIdx})");
                LogInfo($"输入参数:");
                LogInfo($"  - 物料重量: {weight:F1}kg = {weightInTons:F3}吨");
                LogInfo($"  - 总冷却效应: {weightInTons:F3}吨 × {coolingEffect:F0} kJ/吨 = {totalCoolingEffect:F1} kJ");
                LogInfo($"  - 最终温降: {totalCoolingEffect:F1} kJ ÷ {totalHeatCapacity:F1} kJ/℃ = {totalCooling:F3}℃");
                LogInfo($"=== 其他物料温降计算完成 ===");

                return totalCooling;
            }
            catch (Exception ex)
            {
                LogError($"计算物料总降温值失败: 物料={materialId}, 错误={ex.Message}");
                return 0;
            }
        }

        private static async Task<int> ProcessPendingUpdates(
    OracleConnection connection, OracleTransaction transaction,
    Dictionary<string, decimal> taskCalculations)
        {
            if (!_pendingUpdates.Any()) return 0;

            int delayedUpdates = 0;
            int delayedSkipped = 0;
            int preBlowingSkipped = 0;
            var currentTime = DateTime.Now;

            if (_pendingUpdates.Count > 500)
            {
                var expiredRecords = _pendingUpdates.Where(kv => (currentTime - kv.Value.markTime).TotalMinutes > 30).ToList();
                foreach (var expired in expiredRecords)
                {
                    _pendingUpdates.TryRemove(expired.Key, out _);
                }
                if (expiredRecords.Any())
                {
                    LogInfo($"清理过期待处理记录: {expiredRecords.Count}条");
                }
            }

            var pendingRecordsInfo = await GetPendingRecordsInfo(connection, transaction);

            foreach (var pendingItem in _pendingUpdates)
            {
                long msgId = pendingItem.Key;
                var (taskCounter, markTime) = pendingItem.Value;

                if (!taskCalculations.ContainsKey(taskCounter) || !pendingRecordsInfo.ContainsKey(msgId))
                    continue;

                var recordInfo = pendingRecordsInfo[msgId];
                decimal newTemperature = taskCalculations[taskCounter];

                double elapsedSeconds = (currentTime - markTime).TotalSeconds;
                if (elapsedSeconds < 5.0)
                {
                    continue;
                }

                                if (!ShouldUpdateRecord(connection, transaction,
                    recordInfo.msgId, recordInfo.temperature, recordInfo.eventTime,
                    recordInfo.oxygenFlow, recordInfo.lanceHeight, recordInfo.taskCounter,
                    out string skipReason))
                {
                    delayedSkipped++;

                    if (skipReason.Contains("未开吹状态"))
                    {
                        preBlowingSkipped++;
                    }
                    else
                    {
                        LogInfo($"延迟更新跳过记录 {recordInfo.msgId}: {skipReason}");
                    }

                    _pendingUpdates.TryRemove(msgId, out _);
                    continue;
                }

                if (await UpdateSingleRecord(connection, transaction, msgId, newTemperature, recordInfo))
                {
                    delayedUpdates++;
                    _pendingUpdates.TryRemove(msgId, out _);
                }
            }

            if (delayedUpdates > 0 || delayedSkipped > 0)
            {
                var delayLog = new StringBuilder();
                delayLog.Append($"延迟更新处理: 成功{delayedUpdates}条");

                if (delayedSkipped > 0)
                {
                    delayLog.Append($", 跳过{delayedSkipped}条");
                    if (preBlowingSkipped > 0)
                    {
                        delayLog.Append($"(含{preBlowingSkipped}条未开吹状态)");
                    }
                }

                LogInfo(delayLog.ToString());
            }

            return delayedUpdates;
        }

        private static void CleanupOldLogFiles()
        {
            try
            {
                if (!Directory.Exists(LogDirectory))
                {
                    return;
                }

                DateTime cutoffDate = DateTime.Now.AddDays(-3);                 int deletedCount = 0;

                LogInfo($"开始清理日志文件，保留期限：最近3天（{cutoffDate:yyyy-MM-dd}之后）");

                                var logFiles = Directory.GetFiles(LogDirectory, "DataSync_*.log");

                foreach (var logFile in logFiles)
                {
                    try
                    {
                        FileInfo fileInfo = new FileInfo(logFile);

                                                string fileName = Path.GetFileNameWithoutExtension(logFile);

                                                                        
                        DateTime? fileDate = ParseDateFromFileName(fileName);

                        if (fileDate.HasValue)
                        {
                                                        if (fileDate.Value.Date < cutoffDate.Date)
                            {
                                File.Delete(logFile);
                                deletedCount++;
                                LogInfo($"删除过期日志文件: {Path.GetFileName(logFile)} (日期: {fileDate.Value:yyyy-MM-dd})");
                            }
                        }
                        else
                        {
                                                        if (fileInfo.LastWriteTime.Date < cutoffDate.Date)
                            {
                                File.Delete(logFile);
                                deletedCount++;
                                LogInfo($"删除过期日志文件: {Path.GetFileName(logFile)} (修改时间: {fileInfo.LastWriteTime:yyyy-MM-dd})");
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        LogError($"删除日志文件失败: {Path.GetFileName(logFile)} - {ex.Message}", ex);
                    }
                }

                if (deletedCount > 0)
                {
                    LogInfo($"日志清理完成，共删除{deletedCount}个过期文件");
                }
                else
                {
                    LogInfo($"日志清理完成，无过期文件需要删除");
                }
            }
            catch (Exception ex)
            {
                LogError($"清理旧日志文件失败: {ex.Message}", ex);
            }
        }

                                private static DateTime? ParseDateFromFileName(string fileName)
        {
            try
            {
                                var matches = System.Text.RegularExpressions.Regex.Matches(fileName, @"\d{8}");

                if (matches.Count > 0)
                {
                    string dateStr = matches[matches.Count - 1].Value;                     if (dateStr.Length == 8)
                    {
                        int year = int.Parse(dateStr.Substring(0, 4));
                        int month = int.Parse(dateStr.Substring(4, 2));
                        int day = int.Parse(dateStr.Substring(6, 2));

                        return new DateTime(year, month, day);
                    }
                }
            }
            catch
            {
                            }

            return null;
        }

        private static void CleanupExpiredCache()
        {
            var currentTime = DateTime.Now;

                        bool shouldCleanLogs = false;

                        if (currentTime.Minute == 30 && currentTime.Second < 10)
            {
                shouldCleanLogs = true;
            }

                        if (currentTime.Hour == 2 && currentTime.Minute == 0 && currentTime.Second < 10)
            {
                shouldCleanLogs = true;
            }

            var expiredLowTemp = _lowTempProcessCache.Where(kv =>
                (currentTime - kv.Value.StartTime).TotalHours > 2 &&
                ShouldStopCalculation(kv.Key)).ToList();

            foreach (var expired in expiredLowTemp)
            {
                LogInfo($"清理过期低温阶段缓存: 任务={expired.Key}, 运行时间={(currentTime - expired.Value.StartTime).TotalHours:F1}小时", expired.Key);
                _lowTempProcessCache.TryRemove(expired.Key, out _);
            }

            var expiredWaiting = _waitingCoolingCache.Where(kv =>
                (currentTime - kv.Value.WaitingStartTime).TotalHours > 2 &&
                ShouldStopCalculation(kv.Key)).ToList();

            foreach (var expired in expiredWaiting)
            {
                LogInfo($"清理过期等待期缓存: 任务={expired.Key}, 等待时间={(currentTime - expired.Value.WaitingStartTime).TotalHours:F1}小时", expired.Key);
                _waitingCoolingCache.TryRemove(expired.Key, out _);
            }

            var expiredPending = _pendingUpdates.Where(kv =>
                (currentTime - kv.Value.markTime).TotalMinutes > 30).ToList();

            foreach (var expired in expiredPending)
            {
                _pendingUpdates.TryRemove(expired.Key, out _);
            }

            var expiredMBRecords = _mbTemperatureOxygenCache.Where(kv =>
                (currentTime - kv.Value.mbTemperatureTime).TotalHours > 2 &&
                ShouldStopCalculation(kv.Key)).ToList();

            foreach (var expired in expiredMBRecords)
            {
                LogInfo($"清理过期MB表有温度记录: 任务={expired.Key}, 记录时间={expired.Value.mbTemperatureTime:HH:mm:ss}", expired.Key);
                _mbTemperatureOxygenCache.TryRemove(expired.Key, out _);
            }

            var expiredHighTemp = _highTempProcessCache.Where(kv =>
                (currentTime - kv.Value.StartTime).TotalHours > 2 &&
                ShouldStopCalculation(kv.Key)).ToList();

            foreach (var expired in expiredHighTemp)
            {
                LogInfo($"清理过期高温阶段缓存: 任务={expired.Key}, 运行时间={(currentTime - expired.Value.StartTime).TotalHours:F1}小时", expired.Key);
                _highTempProcessCache.TryRemove(expired.Key, out _);
            }

                        var expiredBlowingTimes = _firstBlowingTimeCache.Where(kv =>
            {
                                if (_latestDataCache.TryGetValue(kv.Key, out var cacheValue))
                {
                    return (currentTime - cacheValue.eventTime).TotalHours > 4 &&
                           ShouldStopCalculation(kv.Key);
                }
                                return ShouldStopCalculation(kv.Key);
            }).ToList();

            foreach (var expired in expiredBlowingTimes)
            {
                _firstBlowingTimeCache.TryRemove(expired.Key, out _);
            }

                        var expiredSBStates = _sbTemperatureStateCache.Where(kv =>
            {
                                bool hasLatestData = _latestDataCache.TryGetValue(kv.Key, out var cacheValue);
                if (hasLatestData)
                {
                    return (currentTime - cacheValue.eventTime).TotalHours > 4 &&
                           ShouldStopCalculation(kv.Key);
                }
                return ShouldStopCalculation(kv.Key);
            }).ToList();

            foreach (var expired in expiredSBStates)
            {
                _sbTemperatureStateCache.TryRemove(expired.Key, out _);
            }

            var expiredTaskLogs = _taskLogFiles.Where(kv =>
                !_latestDataCache.ContainsKey(kv.Key) &&
                !_lowTempProcessCache.ContainsKey(kv.Key) &&
                !_highTempProcessCache.ContainsKey(kv.Key) &&
                !_waitingCoolingCache.ContainsKey(kv.Key)).ToList();

            foreach (var expired in expiredTaskLogs)
            {
                LogInfo($"清理过期任务日志文件引用: 任务={expired.Key}");
                _taskLogFiles.TryRemove(expired.Key, out _);
            }

                        if (shouldCleanLogs)
            {
                CleanupOldLogFiles();
            }

            if (expiredLowTemp.Any() || expiredWaiting.Any() || expiredPending.Any() ||
                expiredHighTemp.Any() || expiredTaskLogs.Any() || expiredMBRecords.Any() ||
                expiredBlowingTimes.Any() || expiredSBStates.Any())
            {
                LogInfo($"缓存清理完成: 低温阶段{expiredLowTemp.Count}个, 高温阶段{expiredHighTemp.Count}个, " +
                       $"等待期{expiredWaiting.Count}个, 待处理更新{expiredPending.Count}条, " +
                       $"任务日志引用{expiredTaskLogs.Count}个, MB温度记录{expiredMBRecords.Count}个, " +
                       $"吹炼时间缓存{expiredBlowingTimes.Count}个, SB温度状态{expiredSBStates.Count}个");
            }
        }

        private static async Task<Dictionary<long, (string msgId, string taskCounter, DateTime eventTime, decimal? temperature, decimal oxygenFlow, decimal? lanceHeight)>>
    GetPendingRecordsInfo(OracleConnection connection, OracleTransaction transaction)
        {
            var pendingRecordsInfo = new Dictionary<long, (string, string, DateTime, decimal?, decimal, decimal?)>();
            var msgIdList = _pendingUpdates.Keys.ToList();

            if (!msgIdList.Any()) return pendingRecordsInfo;

            const int batchSize = 1000;
            for (int i = 0; i < msgIdList.Count; i += batchSize)
            {
                var batchIds = msgIdList.Skip(i).Take(batchSize).ToList();

                string batchQuerySql = $@"
            SELECT {TableConfig.MsgIdColumn}, {TableConfig.TaskCounterColumn}, 
                   {TableConfig.TemperatureColumn}, {TableConfig.EventTimeColumn}, 
                   {TableConfig.OxygenFlowColumn}, {TableConfig.LanceHeightColumn}
            FROM {GetTableName(TableConfig.TrendsDataTable)}
            WHERE {TableConfig.MsgIdColumn} IN (" + string.Join(",", batchIds.Select(id => $"'{id}'")) + ")";

                try
                {
                    using (var batchCommand = new OracleCommand(batchQuerySql, connection))
                    {
                        batchCommand.Transaction = transaction;

                        using (var reader = await batchCommand.ExecuteReaderAsync())
                        {
                            while (await reader.ReadAsync())
                            {
                                long msgId = long.Parse(reader.GetString(0));
                                string taskCounter = reader.GetString(1);
                                decimal? temperature = reader.IsDBNull(2) ? (decimal?)null : reader.GetDecimal(2);
                                DateTime eventTime = reader.GetDateTime(3);
                                decimal oxygenFlow = reader.GetDecimal(4);
                                decimal? lanceHeight = reader.IsDBNull(5) ? (decimal?)null : reader.GetDecimal(5);

                                pendingRecordsInfo[msgId] = (msgId.ToString(), taskCounter, eventTime, temperature, oxygenFlow, lanceHeight);
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    LogError($"获取待处理记录信息失败: {ex.Message}", ex);
                                    }
            }

            return pendingRecordsInfo;
        }

        private static bool ShouldUpdateRecord(
    OracleConnection connection, OracleTransaction transaction,
    string msgId, decimal? temperature, DateTime eventTime,
    decimal oxygenFlow, decimal? lanceHeight, string taskCounter,
    out string skipReason)
        {
            skipReason = string.Empty;

            var recordInfo = (msgId: msgId, taskCounter: taskCounter, eventTime: eventTime,
                             temperature: temperature, oxygenFlow: oxygenFlow, lanceHeight: lanceHeight);

                        if (temperature.HasValue && temperature.Value > 0)
            {
                skipReason = $"记录已有温度值{temperature.Value:F1}℃";
                return false;
            }

                        if (oxygenFlow > _oxygenFlowThreshold)
            {
                return true;
            }

            
                        DateTime? firstBlowingTime = null;
            try
            {
                                var getBlowingTimeTask = GetFirstBlowingTime(connection, transaction, taskCounter);
                getBlowingTimeTask.Wait();                 firstBlowingTime = getBlowingTimeTask.Result;
            }
            catch (Exception ex)
            {
                LogError($"在ShouldUpdateRecord中获取吹炼开始时间失败: {ex.Message}", ex, taskCounter);
                                bool wasPreBlowingState = WasPreBlowingStateWhenCreated(recordInfo);
                if (wasPreBlowingState)
                {
                    skipReason = "未开吹状态（获取吹炼时间失败，使用原有逻辑）";
                    return false;
                }
                return true;
            }

            if (firstBlowingTime.HasValue)
            {
                                TimeSpan timeDiff = eventTime - firstBlowingTime.Value;

                if (timeDiff.TotalSeconds >= 0)
                {
                                        LogInfo($"记录{msgId}在吹炼开始后创建，允许更新: " +
                           $"记录时间={eventTime:HH:mm:ss.fff}, " +
                           $"吹炼开始时间={firstBlowingTime.Value:HH:mm:ss.fff}, " +
                           $"时间差={timeDiff.TotalSeconds:F3}秒", taskCounter);
                    return true;
                }
                else
                {
                                        skipReason = $"未开吹状态（记录创建于吹炼开始前{Math.Abs(timeDiff.TotalSeconds):F1}秒）";
                    return false;
                }
            }
            else
            {
                                                bool wasPreBlowingState = WasPreBlowingStateWhenCreated(recordInfo);

                if (wasPreBlowingState)
                {
                    skipReason = "未开吹状态（无吹炼开始记录）";
                    return false;
                }

                return true;
            }
        }

        private static async Task<DateTime?> GetFirstBlowingTime(
    OracleConnection connection, OracleTransaction transaction, string taskCounter)
        {
            try
            {
                                if (_firstBlowingTimeCache.TryGetValue(taskCounter, out var cachedTime) &&
                    cachedTime.HasValue)
                {
                    return cachedTime.Value;
                }

                                string sql = $@"
            SELECT MIN({TableConfig.EventTimeColumn})
            FROM {GetTableName(TableConfig.TrendsDataTable)}
            WHERE {TableConfig.TaskCounterColumn} = :taskCounter
              AND {TableConfig.OxygenFlowColumn} > :oxygenThreshold
              AND {TableConfig.OxygenFlowColumn} IS NOT NULL";

                using (var command = new OracleCommand(sql, connection))
                {
                    command.Transaction = transaction;
                    command.Parameters.Add(new OracleParameter("taskCounter", taskCounter));
                    command.Parameters.Add(new OracleParameter("oxygenThreshold", _oxygenFlowThreshold));

                    var result = await command.ExecuteScalarAsync();
                    if (result != null && result != DBNull.Value)
                    {
                        DateTime firstBlowingTime = Convert.ToDateTime(result);
                        _firstBlowingTimeCache[taskCounter] = firstBlowingTime;

                        LogInfo($"检测到任务{taskCounter}吹炼开始时间: {firstBlowingTime:yyyy-MM-dd HH:mm:ss.fff}", taskCounter);
                        return firstBlowingTime;
                    }
                    else
                    {
                        LogInfo($"任务{taskCounter}未找到吹炼开始记录", taskCounter);
                        _firstBlowingTimeCache[taskCounter] = null;
                        return null;
                    }
                }
            }
            catch (Exception ex)
            {
                LogError($"获取吹炼开始时间失败: {ex.Message}", ex, taskCounter);
                _firstBlowingTimeCache[taskCounter] = null;
                return null;
            }
        }

        private static bool WasPreBlowingStateWhenCreated((string msgId, string taskCounter, DateTime eventTime, decimal? temperature, decimal oxygenFlow, decimal? lanceHeight) recordInfo)
        {
            bool wasOxygenLowWhenCreated = recordInfo.oxygenFlow <= _oxygenFlowThreshold;

            if (!wasOxygenLowWhenCreated)
            {
                return false;
            }

            bool hadStartedCalculationBefore = HadCalculationStartedBefore(recordInfo.taskCounter, recordInfo.eventTime);

            return !hadStartedCalculationBefore;
        }

        private static bool HadCalculationStartedBefore(string taskCounter, DateTime checkTime)
        {
            if (_highTempProcessCache.TryGetValue(taskCounter, out var highTempProcess))
            {
                return highTempProcess.StartTime < checkTime;
            }

            if (_lowTempProcessCache.TryGetValue(taskCounter, out var lowTempProcess))
            {
                return lowTempProcess.StartTime < checkTime;
            }

            if (_waitingCoolingCache.TryGetValue(taskCounter, out var waitingProcess))
            {
                return waitingProcess.WaitingStartTime < checkTime;
            }

            return false;
        }

        private static bool IsPreBlowingState((string taskCounter, DateTime eventTime, decimal? temperature, decimal oxygenFlow, decimal? lanceHeight) recordInfo)
        {
            bool isOxygenLow = recordInfo.oxygenFlow <= _oxygenFlowThreshold;

            bool isLanceHigh = !recordInfo.lanceHeight.HasValue || recordInfo.lanceHeight > _lanceHeightThreshold;

            bool noProcessCache = !_highTempProcessCache.ContainsKey(recordInfo.taskCounter) &&
                                 !_lowTempProcessCache.ContainsKey(recordInfo.taskCounter);

            bool noActiveWaiting = !_waitingCoolingCache.ContainsKey(recordInfo.taskCounter) ||
                                  !_waitingCoolingCache[recordInfo.taskCounter].IsInWaitingPeriod;

            return isOxygenLow && isLanceHigh && noProcessCache && noActiveWaiting;
        }

        private static async Task<bool> UpdateSingleRecord(
    OracleConnection connection, OracleTransaction transaction,
    long msgId, decimal newTemperature,
    (string msgId, string taskCounter, DateTime eventTime, decimal? temperature, decimal oxygenFlow, decimal? lanceHeight) recordInfo)
        {
            string updateSql = $@"
        UPDATE {GetTableName(TableConfig.TrendsDataTable)} 
        SET {TableConfig.TemperatureColumn} = :newTemperature 
        WHERE {TableConfig.MsgIdColumn} = :msgId";

            try
            {
                using (var updateCommand = new OracleCommand(updateSql, connection))
                {
                    updateCommand.Transaction = transaction;
                    updateCommand.Parameters.Add(new OracleParameter("newTemperature", newTemperature));
                    updateCommand.Parameters.Add(new OracleParameter("msgId", msgId.ToString()));

                    int rowsAffected = await updateCommand.ExecuteNonQueryAsync();
                    if (rowsAffected > 0)
                    {
                        LogInfo($"延迟更新成功: MSG_ID={msgId}, 任务={recordInfo.taskCounter}, " +
                               $"温度={newTemperature:F1}℃, 原温度={recordInfo.temperature}", recordInfo.taskCounter);
                        return true;
                    }
                    else
                    {
                        LogInfo($"延迟更新无影响行: MSG_ID={msgId}, 可能记录已被更新", recordInfo.taskCounter);
                        return false;
                    }
                }
            }
            catch (Exception ex)
            {
                LogError($"延迟更新记录失败: MSG_ID={msgId}, 错误={ex.Message}", ex, recordInfo.taskCounter);
                return false;
            }
        }

        private static async Task SyncCurrentTimeData(string connectionString)
        {
            using (var connection = new OracleConnection(connectionString))
            {
                await connection.OpenAsync();

                try
                {
                    DateTime currentTime = DateTime.Now;
                    DateTime exactMatchTime = new DateTime(currentTime.Year, currentTime.Month, currentTime.Day, currentTime.Hour, currentTime.Minute, currentTime.Second);

                    var currentRecords = await GetCurrentTimeRecords(connection, exactMatchTime);

                    UpdateCacheAndStatus(currentRecords);

                    if ((DateTime.Now - _lastWriteTime) >= WriteInterval)
                    {
                        await ProcessBatchCalculations(connection);
                    }
                }
                catch (Exception ex)
                {
                    LogError($"数据同步失败: {ex.Message}");
                    throw;
                }
            }
        }

        private static async Task<List<(DateTime eventTime, string taskCounter, decimal oxygenFlow, decimal? temperature, string msgId)>>
            GetCurrentTimeRecords(OracleConnection connection, DateTime exactMatchTime)
        {
            string queryCurrentSql = $@"
                SELECT {TableConfig.EventTimeColumn}, {TableConfig.TaskCounterColumn}, 
                       {TableConfig.OxygenFlowColumn}, {TableConfig.TemperatureColumn}, {TableConfig.MsgIdColumn}
                FROM {GetTableName(TableConfig.TrendsDataTable)} 
                WHERE {TableConfig.EventTimeColumn} IS NOT NULL 
                  AND {TableConfig.TaskCounterColumn} IS NOT NULL 
                  AND {TableConfig.OxygenFlowColumn} IS NOT NULL
                  AND {TableConfig.EventTimeColumn} >= :startTime 
                  AND {TableConfig.EventTimeColumn} < :endTime
                ORDER BY {TableConfig.EventTimeColumn} DESC";

            var currentRecords = new List<(DateTime, string, decimal, decimal?, string)>();

            using (var currentCommand = new OracleCommand(queryCurrentSql, connection))
            {
                currentCommand.Parameters.Add(new OracleParameter("startTime", exactMatchTime));
                currentCommand.Parameters.Add(new OracleParameter("endTime", exactMatchTime.AddSeconds(1)));

                using (var currentReader = await currentCommand.ExecuteReaderAsync())
                {
                    if (currentReader.HasRows)
                    {
                        while (await currentReader.ReadAsync())
                        {
                            var eventTime = currentReader.GetDateTime(0);
                            var taskCounter = currentReader.GetString(1);
                            var oxygenFlow = currentReader.GetDecimal(2);
                            var temperature = currentReader.IsDBNull(3) ? (decimal?)null : currentReader.GetDecimal(3);
                            var msgId = currentReader.IsDBNull(4) ? null : currentReader.GetString(4);

                            currentRecords.Add((eventTime, taskCounter, oxygenFlow, temperature, msgId));

                            if (temperature == null && !string.IsNullOrEmpty(msgId) && long.TryParse(msgId, out long msgIdLong))
                            {
                                _pendingUpdates[msgIdLong] = (taskCounter, DateTime.Now);
                            }
                        }
                    }
                }
            }

            return currentRecords;
        }

        private static void UpdateCacheAndStatus(List<(DateTime eventTime, string taskCounter, decimal oxygenFlow, decimal? temperature, string msgId)> currentRecords)
        {
            if (!currentRecords.Any())
            {
                return;
            }

            var statusChanges = new List<string>();

            foreach (var record in currentRecords)
            {
                string cacheKey = record.taskCounter;
                bool isActive = record.oxygenFlow > _oxygenFlowThreshold;
                bool previousActive;
                _taskActiveStatus.TryGetValue(cacheKey, out previousActive);
                if (!_taskActiveStatus.ContainsKey(cacheKey))
                {
                    previousActive = true;
                }

                _taskActiveStatus[cacheKey] = isActive;
                _latestDataCache[cacheKey] = (record.eventTime, record.taskCounter, record.oxygenFlow, record.temperature ?? 0, record.msgId, isActive);

                if (isActive != previousActive)
                {
                    statusChanges.Add($"{cacheKey}: {(isActive ? "恢复计算" : "停止计算")}");
                }
            }

            if (statusChanges.Any())
            {
                LogInfo($"任务状态变更: {string.Join("; ", statusChanges)}");
            }
        }

        private static async Task ProcessBatchCalculations(OracleConnection connection)
        {
            if (DateTime.Now.Minute % 10 == 0)
            {
                CleanupExpiredCache();
            }

            var candidateTasks = _latestDataCache
                .Where(kv =>
                    (_taskActiveStatus.ContainsKey(kv.Key) && _taskActiveStatus[kv.Key]) ||
                    _highTempProcessCache.ContainsKey(kv.Key) ||
                    _lowTempProcessCache.ContainsKey(kv.Key)
                )
                .ToList();

            var tasksToProcess = candidateTasks
                .Where(kv => !ShouldStopCalculation(kv.Key))
                .ToDictionary(kv => kv.Key, kv => kv.Value);

            if (!tasksToProcess.Any())
            {
                _lastWriteTime = DateTime.Now;
                LogInfo("无活跃任务需要处理");
                return;
            }

            LogInfo($"开始批量计算处理，活跃任务数量: {tasksToProcess.Count}");
            foreach (var task in tasksToProcess.Keys)
            {
                LogInfo($"开始处理任务计算: {task}", task);
            }

            using (var transaction = connection.BeginTransaction())
            {
                try
                {
                    bool hasTappingEvent = false;
                    foreach (var taskCounter in tasksToProcess.Keys.ToList())
                    {
                        bool hasTappingStart = await CheckTappingStartEvent(connection, transaction, taskCounter);
                        if (hasTappingStart)
                        {
                            _tappingStartedCache[taskCounter] = true;
                            ClearTaskCache(taskCounter);
                            tasksToProcess.Remove(taskCounter);
                            hasTappingEvent = true;
                            LogInfo($"检测到出钢开始事件，立即终止任务计算: {taskCounter}", taskCounter);

                            if (_chartWindowsShown.ContainsKey(taskCounter))
                            {
                                LogInfo($"检测到出钢，立即关闭温度监控弹窗 - 任务: {taskCounter}", taskCounter);
                                _chartWindowsShown.TryRemove(taskCounter, out _);
                                ChartFormManager.CloseTemperatureChart(taskCounter);
                            }

                            continue;
                        }

                        await CheckAndResetForSBFirstBathTemp(connection, transaction, taskCounter);

                        if (_tappingStartedCache.ContainsKey(taskCounter))
                        {
                            LogInfo($"任务 {taskCounter} 已出钢，跳过高温阶段检查和弹窗显示", taskCounter);

                            if (_chartWindowsShown.ContainsKey(taskCounter))
                            {
                                LogInfo($"清理已出钢任务的弹窗缓存 - 任务: {taskCounter}", taskCounter);
                                _chartWindowsShown.TryRemove(taskCounter, out _);
                                ChartFormManager.CloseTemperatureChart(taskCounter);
                            }

                            continue;
                        }

                        bool isHighTempStage = false;
                        string stageReason = "";

                        decimal mbFirstBathTemp = await GetFirstBathTemp(connection, transaction, taskCounter);
                        if (mbFirstBathTemp >= 1450)
                        {
                            isHighTempStage = true;
                            stageReason = $"MB表温度{mbFirstBathTemp:F1}℃ ≥ 1450℃";
                        }

                        decimal sbFirstBathTemp = await GetFirstBathTempSB(connection, transaction, taskCounter);
                        if (sbFirstBathTemp > 0)
                        {
                            isHighTempStage = true;
                            stageReason = stageReason == "" ? $"SB表温度{sbFirstBathTemp:F1}℃ > 0℃" : stageReason + $"，且SB表温度{sbFirstBathTemp:F1}℃ > 0℃";
                        }

                        if (_mbTemperatureOxygenCache.ContainsKey(taskCounter))
                        {
                            isHighTempStage = true;
                            stageReason = stageReason == "" ? "已有MB表温度记录" : stageReason + "，且已有MB表温度记录";
                        }

                        if (_sbTemperatureStateCache.ContainsKey(taskCounter) && _sbTemperatureStateCache[taskCounter].IsInitialized)
                        {
                            isHighTempStage = true;
                            stageReason = stageReason == "" ? "已有SB表温度记录" : stageReason + "，且已有SB表温度记录";
                        }

                        if (isHighTempStage && !_tappingStartedCache.ContainsKey(taskCounter) && !_chartWindowsShown.ContainsKey(taskCounter))
                        {
                            bool popupEnabled = ShouldShowTemperatureChartPopup();

                            if (popupEnabled)
                            {
                                try
                                {
                                    LogInfo($"检测到高温阶段条件满足: {stageReason}", taskCounter);

                                    _chartWindowsShown[taskCounter] = true;

                                    Task.Run(async () =>
                                    {
                                        try
                                        {
                                            await Task.Delay(500);

                                            if (_tappingStartedCache.ContainsKey(taskCounter))
                                            {
                                                LogInfo($"弹窗前二次检查发现已出钢，取消弹窗显示 - 任务: {taskCounter}", taskCounter);
                                                _chartWindowsShown.TryRemove(taskCounter, out _);
                                                return;
                                            }

                                            LogInfo($"准备显示温度监控弹窗，任务: {taskCounter}", taskCounter);

                                            string connectionString = DatabaseConfig.GetConnectionString();
                                            string currentSchema = _currentSchema;

                                            if (Application.MessageLoop)
                                            {
                                                ChartFormManager.ShowTemperatureCarbonChart(taskCounter, connectionString, currentSchema);
                                            }
                                            else
                                            {
                                                var thread = new Thread(() =>
                                                {
                                                    try
                                                    {
                                                        Application.EnableVisualStyles();
                                                        Application.SetCompatibleTextRenderingDefault(false);

                                                        using (var chartConnection = new OracleConnection(connectionString))
                                                        {
                                                            chartConnection.Open();

                                                            var form = new TemperatureCarbonChartForm(taskCounter, chartConnection, currentSchema);

                                                            form.TopMost = true;
                                                            form.TopLevel = true;

                                                            form.FormClosed += (sender, e) =>
                                                            {
                                                                _chartWindowsShown.TryRemove(taskCounter, out _);
                                                                LogInfo($"温度监控弹窗已关闭，任务: {taskCounter}", taskCounter);
                                                            };

                                                            LogInfo($"启动温度监控弹窗显示，任务: {taskCounter}", taskCounter);

                                                            form.Show();
                                                            form.BringToFront();
                                                            form.Activate();
                                                            form.Focus();

                                                            Application.Run(form);
                                                        }
                                                    }
                                                    catch (Exception threadEx)
                                                    {
                                                        LogError($"弹窗线程错误: {threadEx.Message}", threadEx, taskCounter);
                                                        _chartWindowsShown.TryRemove(taskCounter, out _);
                                                    }
                                                });

                                                thread.SetApartmentState(ApartmentState.STA);
                                                thread.IsBackground = true;
                                                thread.Start();
                                            }
                                        }
                                        catch (Exception taskEx)
                                        {
                                            LogError($"显示温度监控弹窗任务失败: {taskEx.Message}", taskEx, taskCounter);
                                            _chartWindowsShown.TryRemove(taskCounter, out _);
                                        }
                                    });

                                    LogInfo($"已启动温度监控弹窗显示任务", taskCounter);
                                }
                                catch (Exception ex)
                                {
                                    LogError($"启动温度监控弹窗显示任务失败: {ex.Message}", ex, taskCounter);
                                    _chartWindowsShown.TryRemove(taskCounter, out _);
                                }
                            }
                            else
                            {
                                LogInfo($"检测到高温阶段条件满足: {stageReason}，但弹窗配置已禁用，不显示监控弹窗", taskCounter);
                                _chartWindowsShown[taskCounter] = true;
                            }
                        }
                        else if (isHighTempStage && _chartWindowsShown.ContainsKey(taskCounter))
                        {
                            LogInfo($"高温阶段弹窗已显示，跳过重复显示", taskCounter);
                        }
                        else if (isHighTempStage && !_chartWindowsShown.ContainsKey(taskCounter) && !ShouldShowTemperatureChartPopup())
                        {
                            LogInfo($"检测到高温阶段条件满足: {stageReason}，弹窗配置已禁用，不显示监控弹窗", taskCounter);
                            _chartWindowsShown[taskCounter] = true;
                        }
                    }

                    if (hasTappingEvent)
                    {
                        transaction.Commit();
                        _lastWriteTime = DateTime.Now;
                        LogInfo("检测到出钢事件，终止批量计算流程");
                        return;
                    }

                    if (!tasksToProcess.Any())
                    {
                        transaction.Commit();
                        _lastWriteTime = DateTime.Now;
                        LogInfo("所有任务都已终止，跳过批量计算");
                        return;
                    }

                    decimal currentTotalOxygen = await GetCurrentTotalOxygen(connection, transaction);
                    if (currentTotalOxygen == 0)
                    {
                        transaction.Commit();
                        LogInfo("当前总氧量为0，跳过批量计算");
                        return;
                    }

                    LogInfo($"获取当前总氧量: {currentTotalOxygen:F1} m³");

                    var taskCalculations = await CalculateTemperatures(connection, transaction, tasksToProcess, currentTotalOxygen);

                    if (taskCalculations.Any())
                    {
                        await UpdateDatabaseRecords(connection, transaction, taskCalculations);
                    }
                    else
                    {
                        transaction.Commit();
                        _lastWriteTime = DateTime.Now;
                        LogInfo("无有效温度计算结果，跳过数据库更新");
                    }

                    LogInfo($"批量计算处理完成，成功计算任务: {taskCalculations.Count}个");
                    foreach (var task in taskCalculations.Keys)
                    {
                        LogInfo($"任务计算完成: {task}, 温度={taskCalculations[task]:F1}℃", task);
                    }
                }
                catch (Exception ex)
                {
                    transaction.Rollback();
                    LogError($"批量计算事务失败: {ex.Message}", ex);
                    throw;
                }
            }
        }


        private static async Task CheckTappingStartEvents(OracleConnection connection, OracleTransaction transaction, IEnumerable<string> taskCounters)
        {
            var tasksToClear = new List<string>();
            foreach (var taskCounter in taskCounters)
            {
                bool hasTappingStart = await CheckTappingStartEvent(connection, transaction, taskCounter);
                if (hasTappingStart)
                {
                    _tappingStartedCache[taskCounter] = true;
                    tasksToClear.Add(taskCounter);

                    ClearTaskCache(taskCounter);
                    LogInfo($"检测到出钢开始事件，立即清除任务缓存: {taskCounter}");
                }
            }
        }

        private static bool ShouldShowTemperatureChartPopup()
        {
                        return TemperatureCarbonChartForm.GetPopupEnabled();
        }

        private static async Task<decimal> GetCurrentTotalOxygen(OracleConnection connection, OracleTransaction transaction)
        {
            string queryActualSql = $@"
        SELECT NVL({TableConfig.TotalOxygenColumn}, 0) 
        FROM {GetTableName(TableConfig.ActualDataTable)}";

            using (var actualCommand = new OracleCommand(queryActualSql, connection))
            {
                actualCommand.Transaction = transaction;

                var result = await actualCommand.ExecuteScalarAsync();
                if (result != null && result != DBNull.Value)
                {
                    decimal totalOxygen = Convert.ToDecimal(result);
                    return totalOxygen;
                }
            }
            return 0;
        }

        private static async Task<Dictionary<string, decimal>> CalculateTemperatures(
    OracleConnection connection, OracleTransaction transaction,
    Dictionary<string, (DateTime eventTime, string taskCounter, decimal oxygenFlow, decimal ca13Wgt, string msgId, bool isActive)> tasksToProcess,
    decimal currentTotalOxygen)
        {
            var taskCalculations = new Dictionary<string, decimal>();

            foreach (var cacheItem in tasksToProcess.Values)
            {
                var taskCounter = cacheItem.taskCounter;

                if (ShouldStopCalculation(taskCounter))
                {
                    continue;
                }

                decimal firstBathTemp = await GetFirstBathTemp(connection, transaction, taskCounter);
                decimal sbFirstBathTemp = await GetFirstBathTempSB(connection, transaction, taskCounter);

                decimal newTemperature;

                bool shouldEnterHighTemp = false;

                                                if (_mbTemperatureOxygenCache.ContainsKey(taskCounter))
                {
                    shouldEnterHighTemp = true;
                    LogInfo($"根据MB表温度记录进入高温阶段", taskCounter);
                }
                                else if (firstBathTemp >= 1450)
                {
                    shouldEnterHighTemp = true;
                    LogInfo($"根据MB表温度进入高温阶段: {firstBathTemp:F1}℃ ≥ 1450℃", taskCounter);
                }
                                else if (sbFirstBathTemp > 0)
                {
                    shouldEnterHighTemp = true;
                    LogInfo($"根据SB表温度进入高温阶段: {sbFirstBathTemp:F1}℃ > 0℃", taskCounter);
                }

                if (shouldEnterHighTemp)
                {
                    newTemperature = await CalculateHighTempTemperature(connection, transaction,
                        taskCounter, cacheItem.oxygenFlow, currentTotalOxygen);
                }
                else
                {
                    newTemperature = await CalculateLowTempTemperature(connection, transaction,
                        taskCounter, cacheItem.oxygenFlow, currentTotalOxygen);
                }

                if (newTemperature > 0)
                {
                    taskCalculations[taskCounter] = newTemperature;
                }
            }

            return taskCalculations;
        }

        private static async Task UpdateDatabaseRecords(
    OracleConnection connection, OracleTransaction transaction,
    Dictionary<string, decimal> taskCalculations)
        {
            int totalUpdated = 0;
            int totalSkipped = 0;
            int preBlowingSkipped = 0;
            var skippedDetails = new Dictionary<string, int>();

            foreach (var taskCounter in taskCalculations.Keys)
            {
                decimal newTemperature = taskCalculations[taskCounter];

                var taskRecords = await GetTaskRecordsToUpdate(connection, transaction, taskCounter);

                if (!taskRecords.Any())
                    continue;

                foreach (var record in taskRecords)
                {
                    if (string.IsNullOrEmpty(record.msgId))
                    {
                        continue;
                    }

                                        if (!ShouldUpdateRecord(connection, transaction,
                        record.msgId, record.temperature, record.eventTime,
                        record.oxygenFlow, record.lanceHeight, record.taskCounter,
                        out string skipReason))
                    {
                        totalSkipped++;

                        if (!skippedDetails.ContainsKey(skipReason))
                            skippedDetails[skipReason] = 0;
                        skippedDetails[skipReason]++;

                        if (skipReason.Contains("未开吹状态"))
                        {
                            preBlowingSkipped++;
                                                        if (preBlowingSkipped % 50 == 0)                             {
                                LogInfo($"未开吹状态跳过累计: {preBlowingSkipped}条", taskCounter);
                            }
                        }
                        else
                        {
                            LogInfo($"跳过记录 {record.msgId}: {skipReason}, " +
                                   $"氧流量={record.oxygenFlow:F1}, " +
                                   $"时间={record.eventTime:HH:mm:ss}", taskCounter);
                        }

                        continue;
                    }

                    if (await UpdateTemperatureRecord(connection, transaction, record.msgId, taskCounter, newTemperature, record))
                    {
                        totalUpdated++;
                        LogInfo($"成功更新记录: MSG_ID={record.msgId}, " +
                               $"任务={taskCounter}, 温度={newTemperature:F1}℃", taskCounter);
                    }
                }

                if (newTemperature > 0)
                {
                    await UpdateRb1ExpectedBathTemperature(connection, transaction, taskCounter, newTemperature);
                }
            }

            int delayedUpdates = await ProcessPendingUpdates(connection, transaction, taskCalculations);
            totalUpdated += delayedUpdates;

            transaction.Commit();
            _lastWriteTime = DateTime.Now;

            if (totalUpdated > 0 || totalSkipped > 0)
            {
                var logMessage = new StringBuilder();
                logMessage.AppendLine($"批量写入完成: 更新{totalUpdated}条记录, 跳过{totalSkipped}条记录");

                if (preBlowingSkipped > 0)
                {
                    logMessage.AppendLine($"  - 未开吹状态跳过: {preBlowingSkipped}条");
                }

                var otherReasons = skippedDetails.Where(kv => !kv.Key.Contains("未开吹状态")).ToList();
                foreach (var reason in otherReasons)
                {
                    if (reason.Value > 0)
                    {
                        logMessage.AppendLine($"  - {reason.Key}: {reason.Value}条");
                    }
                }

                LogInfo(logMessage.ToString());
            }
        }

        private static async Task<List<(string msgId, decimal? temperature, DateTime eventTime, decimal oxygenFlow, decimal? lanceHeight, string taskCounter)>>
    GetTaskRecordsToUpdate(OracleConnection connection, OracleTransaction transaction, string taskCounter)
        {
            string queryTaskRecordsSql = $@"
        SELECT {TableConfig.MsgIdColumn}, {TableConfig.TemperatureColumn}, 
               {TableConfig.EventTimeColumn}, {TableConfig.OxygenFlowColumn},
               {TableConfig.LanceHeightColumn}, {TableConfig.TaskCounterColumn}
        FROM {GetTableName(TableConfig.TrendsDataTable)} 
        WHERE {TableConfig.TaskCounterColumn} = :taskCounter 
          AND ({TableConfig.TemperatureColumn} = 0 OR {TableConfig.TemperatureColumn} IS NULL)
        ORDER BY {TableConfig.EventTimeColumn}";

            var taskRecords = new List<(string, decimal?, DateTime, decimal, decimal?, string)>();

            using (var taskCommand = new OracleCommand(queryTaskRecordsSql, connection))
            {
                taskCommand.Transaction = transaction;
                taskCommand.Parameters.Add(new OracleParameter("taskCounter", taskCounter));

                using (var taskRecordsReader = await taskCommand.ExecuteReaderAsync())
                {
                    while (await taskRecordsReader.ReadAsync())
                    {
                        var msgId = taskRecordsReader.GetString(0);
                        var temperature = taskRecordsReader.IsDBNull(1) ? (decimal?)null : taskRecordsReader.GetDecimal(1);
                        var eventTime = taskRecordsReader.GetDateTime(2);
                        var oxygenFlow = taskRecordsReader.GetDecimal(3);
                        var lanceHeight = taskRecordsReader.IsDBNull(4) ? (decimal?)null : taskRecordsReader.GetDecimal(4);
                        var recordTaskCounter = taskRecordsReader.GetString(5);

                        taskRecords.Add((msgId, temperature, eventTime, oxygenFlow, lanceHeight, recordTaskCounter));
                    }
                }
            }

            return taskRecords;
        }

        private static async Task<bool> UpdateTemperatureRecord(
    OracleConnection connection, OracleTransaction transaction,
    string msgId, string taskCounter, decimal newTemperature,
    (string msgId, decimal? temperature, DateTime eventTime, decimal oxygenFlow, decimal? lanceHeight, string taskCounter) record)
        {
            string updateSql = $@"
        UPDATE {GetTableName(TableConfig.TrendsDataTable)} 
        SET {TableConfig.TemperatureColumn} = :newTemperature 
        WHERE {TableConfig.MsgIdColumn} = :msgId 
          AND {TableConfig.TaskCounterColumn} = :taskCounter";

            using (var updateCommand = new OracleCommand(updateSql, connection))
            {
                updateCommand.Transaction = transaction;
                updateCommand.Parameters.Add(new OracleParameter("newTemperature", newTemperature));
                updateCommand.Parameters.Add(new OracleParameter("msgId", msgId));
                updateCommand.Parameters.Add(new OracleParameter("taskCounter", taskCounter));

                int rowsAffected = await updateCommand.ExecuteNonQueryAsync();
                if (rowsAffected > 0)
                {
                    if (long.TryParse(record.msgId, out long msgIdLong))
                    {
                        _pendingUpdates.TryRemove(msgIdLong, out _);
                    }

                    return true;
                }

                return false;
            }
        }

        private static void FixConsoleEncoding()
        {
            try
            {
                Console.OutputEncoding = Encoding.UTF8;
                Console.InputEncoding = Encoding.UTF8;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"设置控制台编码失败: {ex.Message}");
            }
        }

        private static void InitializeLogging()
        {
            try
            {
                if (!Directory.Exists(LogDirectory))
                {
                    Directory.CreateDirectory(LogDirectory);
                }

                                LogInfo("=== 数据同步服务启动 ===");
                LogInfo($"当前时间: {DateTime.Now:yyyy-MM-dd HH:mm:ss}");
                LogInfo($"日志目录: {Path.GetFullPath(LogDirectory)}");
                LogInfo($"日志保留策略: 保留最近3天的日志文件");
                LogInfo($"当前日志文件: {Path.GetFileName(LogFilePath)}");

                                try
                {
                    if (Directory.Exists(LogDirectory))
                    {
                        var logFiles = Directory.GetFiles(LogDirectory, "DataSync_*.log");
                        if (logFiles.Any())
                        {
                            LogInfo($"发现{logFiles.Length}个日志文件:");
                            foreach (var file in logFiles.Take(5))                             {
                                var fileInfo = new FileInfo(file);
                                LogInfo($"  - {Path.GetFileName(file)} ({fileInfo.Length / 1024:N0}KB, 修改于{fileInfo.LastWriteTime:yyyy-MM-dd HH:mm})");
                            }
                            if (logFiles.Length > 5)
                            {
                                LogInfo($"  ... 还有{logFiles.Length - 5}个文件");
                            }
                        }
                        else
                        {
                            LogInfo("日志目录中未发现日志文件");
                        }
                    }
                }
                catch (Exception ex)
                {
                    LogInfo($"读取日志文件信息失败: {ex.Message}");
                }

                var furnaceSettings = ConfigurationManager.GetSection("furnaceSettings") as NameValueCollection;
                string currentFurnace = furnaceSettings?["CurrentFurnace"] ?? "未知";
                LogInfo($"当前炉座: {currentFurnace}号炉");
                LogInfo($"数据库Schema: {_currentSchema}");

                int customScrapCount = 0;
                foreach (var scrapInfo in ScrapMaterialMap)
                {
                    decimal meltingRate = scrapInfo.Value.MeltingRate;
                    bool isCustom = meltingRate != 50m;
                    if (isCustom) customScrapCount++;
                }
                LogInfo($"  - 自定义配置: {customScrapCount}/15 种废钢");

                LogInfo("冷却剂物料映射配置:");
                foreach (var preset in PresetToMaterialMap)
                {
                    var presetInfo = preset.Value;
                    LogInfo($"  - 预设{preset.Key}: {presetInfo.MaterialType} (SDM_IDX={presetInfo.SdmIdx})");
                }


                var oracleSettings = ConfigurationManager.GetSection("oracleSettings") as NameValueCollection;
                if (oracleSettings != null)
                {
                    LogInfo($"  - 数据库主机: {oracleSettings["Host"]}");
                    LogInfo($"  - 数据库端口: {oracleSettings["Port"]}");
                    LogInfo($"  - 服务名称: {oracleSettings["ServiceName"]}");
                    LogInfo($"  - 连接超时: {oracleSettings["ConnectionTimeout"]} 秒");
                }

                LogInfo("=== 数据同步服务初始化完成 ===");

                Console.WriteLine("=== 数据同步服务启动信息 ===");
                Console.WriteLine($"当前炉座: {currentFurnace}号炉, Schema: {_currentSchema}");
                Console.WriteLine($"氧气阈值: {_oxygenFlowThreshold}m³/h, 枪位阈值: {_lanceHeightThreshold}mm");
                Console.WriteLine($"废钢配置: {customScrapCount}/15 种自定义熔化速率");
                Console.WriteLine($"冷却剂表: {GetTableName(TableConfig.MaterialModelTable)}");
                Console.WriteLine($"废钢表: {TableConfig.ScrapMaterialTable}");
                Console.WriteLine($"日志路径: {LogFilePath}");
                Console.WriteLine("==============================");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"初始化日志系统失败: {ex.Message}");

                Console.WriteLine("=== 数据同步服务启动 ===");
                Console.WriteLine($"当前时间: {DateTime.Now:yyyy-MM-dd HH:mm:ss}");
                Console.WriteLine($"日志目录: {LogDirectory}");
                Console.WriteLine("使用默认配置继续运行...");
            }
        }

                private static string GetTaskLogFilePath(string taskCounter)
        {
            return _taskLogFiles.GetOrAdd(taskCounter, key =>
            {
                string taskLogFileName = $"DataSync_{taskCounter}_{DateTime.Now:yyyyMMdd}.log";
                return Path.Combine(LogDirectory, taskLogFileName);
            });
        }

                public static void LogInfo(string message, string taskCounter = null)
        {
            WriteLog("INFO", message, taskCounter);
        }

        public static void LogWarning(string message, string taskCounter = null)
        {
            WriteLog("WARN", message, taskCounter);
        }

        public static void LogError(string message, Exception ex = null, string taskCounter = null)
        {
            var logMessage = message;
            if (ex != null)
            {
                logMessage += $"\n异常信息: {ex.Message}";
                if (ex.InnerException != null)
                {
                    logMessage += $"\n内部异常: {ex.InnerException.Message}";
                }
                logMessage += $"\n堆栈跟踪: {ex.StackTrace}";
            }
            WriteLog("ERROR", logMessage, taskCounter);
        }

                private static void WriteLog(string level, string message, string taskCounter = null)
        {
            try
            {
                string logEntry = $"{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff} [{level}] {message}";

                                Console.WriteLine(logEntry);

                                if (!Directory.Exists(LogDirectory))
                {
                    Directory.CreateDirectory(LogDirectory);
                }

                                try
                {
                    using (var writer = new StreamWriter(LogFilePath, true, Encoding.UTF8))
                    {
                        writer.WriteLine(logEntry);
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"写入主日志文件失败: {ex.Message}");
                }

                                if (!string.IsNullOrEmpty(taskCounter))
                {
                    try
                    {
                        string taskLogFilePath = GetTaskLogFilePath(taskCounter);
                        using (var taskWriter = new StreamWriter(taskLogFilePath, true, Encoding.UTF8))
                        {
                            taskWriter.WriteLine(logEntry);
                        }

                                                if (level == "INFO" && message.Contains("开始等待温降计算"))
                        {
                            Console.WriteLine($"任务日志文件: {taskLogFilePath}");
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"写入任务日志文件失败 - 任务={taskCounter}: {ex.Message}");

                                                try
                        {
                            _taskLogFiles.TryRemove(taskCounter, out _);
                            string taskLogFilePath = GetTaskLogFilePath(taskCounter);
                            using (var taskWriter = new StreamWriter(taskLogFilePath, true, Encoding.UTF8))
                            {
                                taskWriter.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff} [INFO] 重新创建任务日志文件");
                                taskWriter.WriteLine(logEntry);
                            }
                        }
                        catch (Exception retryEx)
                        {
                            Console.WriteLine($"重新创建任务日志文件失败 - 任务={taskCounter}: {retryEx.Message}");
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                try
                {
                    Console.WriteLine($"写入日志失败: {ex.Message}");
                }
                catch
                {
                                    }
            }
        }

        private static bool InitializeFurnaceConfiguration()
        {
            try
            {
                var furnaceSettings = ConfigurationManager.GetSection("furnaceSettings") as NameValueCollection;
                if (furnaceSettings == null)
                {
                    LogError("未找到furnaceSettings配置节");
                    return false;
                }

                string currentFurnace = furnaceSettings["CurrentFurnace"];
                if (string.IsNullOrEmpty(currentFurnace))
                {
                    LogError("未配置当前炉座(CurrentFurnace)");
                    return false;
                }

                string schemaKey = $"Furnace{currentFurnace}_Schema";
                _currentSchema = furnaceSettings[schemaKey];

                if (string.IsNullOrEmpty(_currentSchema))
                {
                    LogError($"未找到炉座{currentFurnace}对应的数据库schema配置");
                    return false;
                }

                string oxygenFlowThresholdStr = furnaceSettings["OxygenFlowThreshold"];
                if (!string.IsNullOrEmpty(oxygenFlowThresholdStr) && decimal.TryParse(oxygenFlowThresholdStr, out decimal threshold))
                {
                    _oxygenFlowThreshold = threshold;
                }

                string lanceHeightThresholdStr = furnaceSettings["LanceHeightThreshold"];
                if (!string.IsNullOrEmpty(lanceHeightThresholdStr) && decimal.TryParse(lanceHeightThresholdStr, out decimal lanceThreshold))
                {
                    _lanceHeightThreshold = lanceThreshold;
                }

                string temperatureMeasurementDelayStr = furnaceSettings["TemperatureMeasurementDelay"];
                if (!string.IsNullOrEmpty(temperatureMeasurementDelayStr) && int.TryParse(temperatureMeasurementDelayStr, out int measurementDelay))
                {
                    _temperatureMeasurementDelay = measurementDelay;
                }

                string tiltAngleThresholdStr = furnaceSettings["TiltAngleThreshold"];
                if (!string.IsNullOrEmpty(tiltAngleThresholdStr) && decimal.TryParse(tiltAngleThresholdStr, out decimal tiltThreshold))
                {
                    _tiltAngleThreshold = tiltThreshold;
                }
                else
                {
                    _tiltAngleThreshold = 85m;
                }

                string waitingCoolingCoefficientStr = furnaceSettings["DefaultWaitingCoolingCoefficient"];
                if (!string.IsNullOrEmpty(waitingCoolingCoefficientStr) && decimal.TryParse(waitingCoolingCoefficientStr, out decimal waitingCoefficient))
                {
                }

                string tiltCoolingCoefficientStr = furnaceSettings["DefaultTiltCoolingCoefficient"];
                if (!string.IsNullOrEmpty(tiltCoolingCoefficientStr) && decimal.TryParse(tiltCoolingCoefficientStr, out decimal tiltCoolingCoefficient))
                {
                }

                LogInfo("废钢熔化速率配置:");
                bool hasScrapConfig = false;
                foreach (var scrapType in new[] { "SC1", "SC2", "SC3", "SC4", "SC5", "SC6", "SC7", "SC8", "SC9", "SC10", "SC11", "SC12", "SC13", "SC14", "SC15" })
                {
                    decimal meltingRate = GetScrapMeltingRate(scrapType);

                    string configKey = $"{scrapType}_MeltingRate";
                    if (furnaceSettings[configKey] != null)
                    {
                        hasScrapConfig = true;
                    }
                }

                if (!hasScrapConfig)
                {
                    LogInfo("所有废钢类型使用默认熔化速率 50kg/s");
                }


                Console.WriteLine($"=== 炉座配置信息 ===");
                Console.WriteLine($"当前运行炉座: {currentFurnace}号炉");
                Console.WriteLine($"数据库Schema: {_currentSchema}");
                Console.WriteLine($"氧气流量阈值: {_oxygenFlowThreshold} m³/h");
                Console.WriteLine($"枪位高度阈值: {_lanceHeightThreshold} mm");
                Console.WriteLine($"测温延迟时间: {_temperatureMeasurementDelay} 分钟");
                Console.WriteLine($"倒炉角度阈值: {_tiltAngleThreshold}°");

                decimal defaultScrapRate = GetScrapMeltingRate("SC1");
                int customScrapCount = 0;
                foreach (var scrapType in new[] { "SC1", "SC2", "SC3", "SC4", "SC5", "SC6", "SC7", "SC8", "SC9", "SC10", "SC11", "SC12", "SC13", "SC14", "SC15" })
                {
                    string configKey = $"{scrapType}_MeltingRate";
                    if (furnaceSettings[configKey] != null)
                    {
                        customScrapCount++;
                    }
                }

                if (customScrapCount > 0)
                {
                    Console.WriteLine($"废钢熔化速率: {customScrapCount}种自定义, {15 - customScrapCount}种使用默认值({defaultScrapRate}kg/s)");
                }
                else
                {
                    Console.WriteLine($"废钢熔化速率: 全部使用默认值 {defaultScrapRate}kg/s");
                }

                Console.WriteLine($"日志路径: {LogFilePath}");
                Console.WriteLine($"==============================");

                if (_oxygenFlowThreshold <= 0)
                {
                    LogError($"氧气流量阈值配置无效: {_oxygenFlowThreshold}");
                    return false;
                }

                if (_lanceHeightThreshold <= 0)
                {
                    LogError($"枪位高度阈值配置无效: {_lanceHeightThreshold}");
                    return false;
                }

                if (_tiltAngleThreshold <= 0 || _tiltAngleThreshold > 90)
                {
                    LogError($"倒炉角度阈值配置无效: {_tiltAngleThreshold}，应在1-90度之间");
                    return false;
                }

                foreach (var scrapType in new[] { "SC1", "SC2", "SC3", "SC4", "SC5", "SC6", "SC7", "SC8", "SC9", "SC10", "SC11", "SC12", "SC13", "SC14", "SC15" })
                {
                    decimal meltingRate = GetScrapMeltingRate(scrapType);
                    if (meltingRate <= 0 || meltingRate > 1000)
                    {
                        LogError($"废钢{scrapType}熔化速率配置无效: {meltingRate}，应在0.1-1000 kg/s之间");
                        return false;
                    }
                }

                return true;
            }
            catch (Exception ex)
            {
                LogError($"初始化炉座配置失败: {ex.Message}", ex);
                return false;
            }
        }

        public static string GetTableName(string tableName)
        {
            return $"{_currentSchema}.{tableName}";
        }

        private static Assembly CurrentDomain_AssemblyResolve(object sender, ResolveEventArgs args)
        {
            var assemblyName = new AssemblyName(args.Name);

            if (assemblyName.Name == "Oracle.ManagedDataAccess")
            {
                try
                {
                    return Assembly.LoadFrom("Oracle.ManagedDataAccess.dll");
                }
                catch (Exception ex)
                {
                    LogError($"加载Oracle.ManagedDataAccess程序集失败: {ex.Message}", ex);
                    return null;
                }
            }

            return null;
        }
    }

    
    public static class TableConfig
    {
        private static readonly NameValueCollection _tableSettings;
        private static readonly NameValueCollection _columnSettings;
        public static string RB1RepWgtColumn => GetColumnSetting("RB1RepWgtColumn");

        static TableConfig()
        {
            try
            {
                _tableSettings = ConfigurationManager.GetSection("tableSettings") as NameValueCollection;
                _columnSettings = ConfigurationManager.GetSection("columnSettings") as NameValueCollection;

                if (_tableSettings == null)
                    throw new ConfigurationErrorsException("未找到tableSettings配置节");
                if (_columnSettings == null)
                    throw new ConfigurationErrorsException("未找到columnSettings配置节");
            }
            catch (Exception ex)
            {
                Program.LogError($"读取表配置失败: {ex.Message}", ex);
                throw;
            }
        }

        public static string TrendsDataTable => GetTableSetting("TrendsDataTable");
        public static string PeriodDataMBTable => GetTableSetting("PeriodDataMBTable");
        public static string PeriodDataSBTable => GetTableSetting("PeriodDataSBTable");
        public static string PeriodDataRB1Table => GetTableSetting("PeriodDataRB1Table");
        public static string ActualDataTable => GetTableSetting("ActualDataTable");
        public static string MaterialModelTable => GetTableSetting("MaterialModelTable");
        public static string ScrapMaterialTable => GetTableSetting("ScrapMaterialTable");
        public static string AdditionsTable => GetTableSetting("AdditionsTable");
        public static string LogDataTable => GetTableSetting("LogDataTable");
        public static string ScrapDataTable => GetTableSetting("ScrapDataTable");
        public static string AuxConstantsTable => GetTableSetting("AuxConstantsTable");
        public static string AuxValuesTable => GetTableSetting("AuxValuesTable");
        public static string InputHMDataTable => GetTableSetting("InputHMDataTable");
        public static string EventsTable => GetTableSetting("EventsTable");

        public static string EventTimeColumn => GetColumnSetting("EventTimeColumn");
        public static string TaskCounterColumn => GetColumnSetting("TaskCounterColumn");
        public static string OxygenFlowColumn => GetColumnSetting("OxygenFlowColumn");
        public static string TemperatureColumn => GetColumnSetting("TemperatureColumn");
        public static string MsgIdColumn => GetColumnSetting("MsgIdColumn");
        public static string LanceHeightColumn => GetColumnSetting("LanceHeightColumn");
        public static string ConverterAngleColumn => GetColumnSetting("ConverterAngleColumn");
        public static string FirstBathTempColumn => GetColumnSetting("FirstBathTempColumn");
        public static string FirstBathTempSBColumn => GetColumnSetting("FirstBathTempSBColumn");
        public static string ReqO2VolColumn => GetColumnSetting("ReqO2VolColumn");
        public static string ExpBathTempColumn => GetColumnSetting("ExpBathTempColumn");
        public static string ActualO2VolColumn => GetColumnSetting("ActualO2VolColumn");
        public static string TotalOxygenColumn => GetColumnSetting("TotalOxygenColumn");
        public static string MatIdColumn => GetColumnSetting("MatIdColumn");
        public static string ScrapMatIdColumn => GetColumnSetting("ScrapMatIdColumn");
        public static string ScrapSlagPctColumn => GetColumnSetting("ScrapSlagPctColumn");
        public static string SdmIdxColumn => GetColumnSetting("SdmIdxColumn");
        public static string SBCalcWgtColumn => GetColumnSetting("SBCalcWgtColumn");
        public static string MBCalcWgtColumn => GetColumnSetting("MBCalcWgtColumn");
        public static string SBRepWgtColumn => GetColumnSetting("SBRepWgtColumn");
        public static string MBRepWgtColumn => GetColumnSetting("MBRepWgtColumn");
        public static string MaterialIdColumn => GetColumnSetting("MaterialIdColumn");
        public static string SteelWeight1Column => GetColumnSetting("SteelWeight1Column");
        public static string SteelWeight2Column => GetColumnSetting("SteelWeight2Column");
        public static string SlagWeight1Column => GetColumnSetting("SlagWeight1Column");
        public static string SlagWeight2Column => GetColumnSetting("SlagWeight2Column");
        public static string StepColumn => GetColumnSetting("StepColumn");
        public static string SdmFunctionColumn => GetColumnSetting("SdmFunctionColumn");

        public static string ScrapWeight1Column => GetColumnSetting("ScrapWeight1Column");
        public static string ScrapWeight2Column => GetColumnSetting("ScrapWeight2Column");
        public static string ScrapWeight3Column => GetColumnSetting("ScrapWeight3Column");
        public static string ScrapWeight4Column => GetColumnSetting("ScrapWeight4Column");
        public static string ScrapWeight5Column => GetColumnSetting("ScrapWeight5Column");
        public static string ScrapWeight6Column => GetColumnSetting("ScrapWeight6Column");
        public static string ScrapWeight7Column => GetColumnSetting("ScrapWeight7Column");
        public static string ScrapWeight8Column => GetColumnSetting("ScrapWeight8Column");
        public static string ScrapWeight9Column => GetColumnSetting("ScrapWeight9Column");
        public static string ScrapWeight10Column => GetColumnSetting("ScrapWeight10Column");
        public static string ScrapWeight11Column => GetColumnSetting("ScrapWeight11Column");
        public static string ScrapWeight12Column => GetColumnSetting("ScrapWeight12Column");
        public static string ScrapWeight13Column => GetColumnSetting("ScrapWeight13Column");
        public static string ScrapWeight14Column => GetColumnSetting("ScrapWeight14Column");
        public static string ScrapWeight15Column => GetColumnSetting("ScrapWeight15Column");

        public static string IntegerValueColumn => GetColumnSetting("IntegerValueColumn");
        public static string FloatValueColumn => GetColumnSetting("FloatValueColumn");
        public static string CharValueColumn => GetColumnSetting("CharValueColumn");
        public static string ConstantIdColumn => GetColumnSetting("ConstantIdColumn");

        public static string TempColumn => GetColumnSetting("TempColumn");
        public static string TypeColumn => GetColumnSetting("TypeColumn");

        public static string EventTextColumn => GetColumnSetting("EventTextColumn");

        private static string GetTableSetting(string key)
        {
            return GetRequiredSetting(_tableSettings, key, $"表配置项 '{key}'");
        }

        private static string GetColumnSetting(string key)
        {
            return GetRequiredSetting(_columnSettings, key, $"字段配置项 '{key}'");
        }

        private static string GetRequiredSetting(NameValueCollection settings, string key, string errorMessage)
        {
            var value = settings[key];
            if (string.IsNullOrWhiteSpace(value))
            {
                throw new ConfigurationErrorsException($"{errorMessage} 不能为空");
            }
            return value;
        }
    }
}
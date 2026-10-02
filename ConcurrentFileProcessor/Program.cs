using System.Threading.Channels;
using System.Text.Json.Nodes;

namespace ConcurrentFileProcessor;

public static class Program
{
    private static string _outputPath;
    private static string _inputPath;
    private static Task _process;
    private static FileSystemWatcher _watcher; //注意结束的时候要手动释放监听器
    private static Channel<string> _channel;

    private static readonly object _producerGate = new();
    private static readonly List<Task> _inFlight = new();
    private static bool _accepting;

    private static int _shutdownStage;

    private static CancellationTokenSource _cts;

    private static async Task Main()
    {
        Init();

        _process = TakeAndProcessFromChannel();
        try
        {
            await _process;
            Console.WriteLine("正常退出");
        }
        catch (OperationCanceledException oce)
        {
            Console.WriteLine("用户控制台强制退出");
        }
        catch (Exception e)
        {
            Console.WriteLine($"异常退出，原因为{e}");
        }
    }

    private static void Init()
    {
        try
        {
            JsonNode? config = JsonNode.Parse(File.ReadAllText("config.json"));
            _inputPath = config?["INPUT_DIR"]?.GetValue<string>();
            _outputPath = config?["OUTPUT_DIR"]?.GetValue<string>();
            if (_inputPath == null || _outputPath == null)
                throw new ArgumentException();

            _channel = Channel.CreateBounded<string>(100);
            _accepting = true;
            _inFlight.Clear();
            _cts = new CancellationTokenSource();
            Console.CancelKeyPress += Shutdown;

            _watcher = new FileSystemWatcher(_inputPath);
            _watcher.NotifyFilter = NotifyFilters.FileName;
            _watcher.Filter = "*.txt";
            // 监听器缓冲区溢出处理
            _watcher.InternalBufferSize = 64 * 1024;
            _watcher.Created += OnFileCreated;
            _watcher.Error += (s, e) => Console.WriteLine($"Watcher 出错：{e.GetException()}");
            _watcher.EnableRaisingEvents = true;


        }
        catch (Exception e)
        {
            Console.WriteLine(e);
            throw;
        }
    }

    private static async Task WaitUntilReady(string path, CancellationToken token)
    {
        while (true)
        {
            token.ThrowIfCancellationRequested();
            try
            {
                using var fs = File.Open(path, FileMode.Open, FileAccess.Read, FileShare.None);
                return;
            }
            catch (FileNotFoundException)
            {
                // 文件在就绪前被删除/改名：跳过，否则会无限重试
                return;
            }
            catch (DirectoryNotFoundException)
            {
                return;
            }
            catch (IOException)
            {
                await Task.Delay(100, token);
            }
        }
    }

    private static async Task TakeAndProcessFromChannel()
    {
        await foreach (var filePath in _channel.Reader.ReadAllAsync(_cts.Token))
        {
            await WaitUntilReady(filePath, _cts.Token);
            _cts.Token.ThrowIfCancellationRequested();
            Processor processor = new(filePath);
            processor.Process(_cts.Token);
            var rez = processor.GainResult();

            Dictionary<string, uint> userDict = ((Dictionary<string, uint>)rez[1]);
            string topUsers = userDict.Count == 0
                ? "N/A"
                : string.Join(", ",
                    userDict.Where(kv => kv.Value == userDict.Values.Max()).Select(kv => kv.Key));

            Dictionary<string, uint> debugLeveldict = ((Dictionary<string, uint>)rez[2]);
            uint infoCnt = debugLeveldict.GetValueOrDefault<string, uint>("INFO", 0);
            uint warnCnt = debugLeveldict.GetValueOrDefault<string, uint>("WARN", 0);
            uint errorCnt = debugLeveldict.GetValueOrDefault<string, uint>("ERROR", 0);

            var content = $"File: {Path.GetFileName(filePath)}\n" +
                          $"Total lines: {rez[0]}\n" +
                          $"Info: {infoCnt}\n" +
                          $"Warn: {warnCnt}\n" +
                          $"Error: {errorCnt}\n" +
                          $"UniqueUsers: {userDict.Count}\n" +
                          $"MostActiveUser: {topUsers}";
            await File.WriteAllTextAsync(
                Path.Combine(_outputPath, Path.GetFileNameWithoutExtension(filePath) + ".output.txt"), content);
        }
    }

    private static void OnFileCreated(object sender, FileSystemEventArgs e)
    {
        lock (_producerGate)
        {
            if (!_accepting) return; // 忽略晚到的回调
            var task = EnqueueAsync(e.FullPath);
            _inFlight.Add(task);
            // 完成后自动移除
            _ = task.ContinueWith(
                t =>
                {
                    lock (_producerGate) _inFlight.Remove(t);
                },
                CancellationToken.None,
                TaskContinuationOptions.ExecuteSynchronously,
                TaskScheduler.Default);
        }
    }

    private static async Task EnqueueAsync(string path)
    {
        try
        {
            await _channel.Writer.WriteAsync(path, _cts.Token);
        }
        catch (OperationCanceledException)
        {
        }
        catch (ChannelClosedException)
        {
        }
        catch (Exception ex)
        {
            Console.WriteLine($"写入Channel失败，原因：{ex}");
        }
    }

    private static void Shutdown(object? obj, ConsoleCancelEventArgs e)
    {
        e.Cancel = true;

        if (Interlocked.CompareExchange(ref _shutdownStage, 1, 0) == 0)
        {
            _ = StopProducingAsync();
            Console.WriteLine("正在处理剩余任务，再按一次 Ctrl+C 强制退出...");
        }
        else
        {
            _cts.Cancel();
        }
    }
    private static async Task StopProducingAsync()
    {
        _watcher.EnableRaisingEvents = false;
        _watcher.Dispose();

        Task[] pending;
        lock (_producerGate)
        {
            _accepting = false;
            pending = _inFlight.ToArray();
        }

        await Task.WhenAll(pending);
        _channel.Writer.Complete();
    }
}
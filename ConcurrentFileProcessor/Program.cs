using System.Threading.Channels;

namespace ConcurrentFileProcessor;

public static class Program
{
    private static string _outputPath;
    private static Task _process;
    private static FileSystemWatcher _watcher; //注意结束的时候要手动释放监听器
    private static Channel<string> _channel;

    private static CancellationTokenSource _cts;

    private static async Task Main()
    {
        Init("G:\\Project\\.test_data_area\\CFP_Input");

        _process = TakeAndProcessFromChannel();
        try
        {
            await _process;
        }
        catch (OperationCanceledException oce)
        {
            Console.WriteLine("正常退出");
        }
        catch (Exception e)
        {
            Console.WriteLine($"异常退出，原因为{e}");
        }
    }

    private static void Init(string dirPath)
    {
        _watcher = new FileSystemWatcher(dirPath);
        _watcher.NotifyFilter = NotifyFilters.FileName;
        _watcher.Filter = "*.txt";
        // 监听器缓冲区溢出处理
        _watcher.Error += (s, e) => Console.WriteLine($"Watcher 出错：{e.GetException()}");
        _watcher.InternalBufferSize = 64 * 1024;
        _watcher.EnableRaisingEvents = true;
        _watcher.Created += WriteIntoChannel;

        _outputPath = "G:\\Project\\.test_data_area\\CFP_Output";

        _channel = Channel.CreateBounded<string>(100);

        _cts = new CancellationTokenSource();

        Console.CancelKeyPress += Shutdown;
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
            catch (IOException)
            {
                await Task.Delay(100, token);
            }
        }
    }

    private static async void WriteIntoChannel(object obj, FileSystemEventArgs e)
    {
        try
        {
            await WaitUntilReady(e.FullPath, _cts.Token);
            await _channel.Writer.WriteAsync(e.FullPath);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"写入Channel失败，原因：{ex}");
        }
    }

    private static async Task TakeAndProcessFromChannel()
    {
        await foreach (var filePath in _channel.Reader.ReadAllAsync(_cts.Token))
        {
            _cts.Token.ThrowIfCancellationRequested();
            Processor processor = new(filePath);
            processor.Process(_cts.Token);
            var rez = processor.GainResult();

            var content = $"File: {Path.GetFileName(filePath)}\n" +
                          $"Total lines: {rez[0]}\n" +
                          $"Info: {((Dictionary<string, uint>)rez[2])["Info"]}\n" +
                          $"Warn: {((Dictionary<string, uint>)rez[2])["Warn"]}\n" +
                          $"Error: {((Dictionary<string, uint>)rez[2])["Error"]}\n" +
                          $"UniqueUsers: {((Dictionary<string, uint>)rez[1]).Count}\n" +
                          $"MostActiveUser: {((Dictionary<string, uint>)rez[1])
                              .Where(kv => kv.Value == ((Dictionary<string, uint>)rez[1]).Values.Max())
                              .Select(kv => kv.Key)
                              .ToArray()}";
            await File.WriteAllTextAsync(
                Path.Combine(_outputPath, Path.GetFileNameWithoutExtension(filePath) + ".output.txt"), content);
        }
    }

    private static void Shutdown(object? obj, ConsoleCancelEventArgs e)
    {
        e.Cancel = true;
        _channel.Writer.Complete();
        _watcher.Dispose();
        _cts.Cancel();
    }
}
using System.Text;

namespace ConcurrentFileProcessor;

public class Processor
{
    private readonly Dictionary<string, uint> _debugLevelCnt;
    private readonly string _filePath;
    private uint _rowCnt;
    private readonly Dictionary<string, uint> _userCnt;

    public Processor(string filePath)
    {
        _filePath = filePath;
        _rowCnt = 0;
        _userCnt = new Dictionary<string, uint>();
        _debugLevelCnt = new Dictionary<string, uint>();
    }

    public void Process(CancellationToken token)
    {
        try
        {
            foreach (var line in File.ReadLines(_filePath, Encoding.UTF8))
            {
                token.ThrowIfCancellationRequested();
                _rowCnt++;
                
                var parts = line.Split(' ', StringSplitOptions.RemoveEmptyEntries);
                if(parts.Length == 0)
                    continue;
                
                // C# 字典对未创建的键值对可写（创建）不可读，应当指定一个默认值
                _debugLevelCnt[parts[0]] = _debugLevelCnt.GetValueOrDefault(parts[0]) + 1;

                if (parts[^1].Contains("user="))
                {
                    string name = parts[^1].Split("=")[1];
                    _userCnt[name] = _userCnt.GetValueOrDefault(name) + 1;
                }
            }
        }
        catch (OperationCanceledException oce)
        {
            Console.WriteLine("[Processor] 已经通过token停止处理");
            // 通知调用方
            throw;
        }
        catch (Exception e)
        {
            Console.WriteLine($"[Processor] 异常，原因: {e}");
            throw;
        }
    }

    public object[] GainResult()
    {
        var rez = new object[] { _rowCnt, _userCnt, _debugLevelCnt };
        return rez;
    }
}
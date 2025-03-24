using Microsoft.Extensions.Logging;

namespace DataStax.AstraDB.DataApi.IntegrationTests;

public class FileLogger : ILogger
{
    private readonly string _filePath;
    private readonly LogLevel _minLogLevel;
    private readonly object _lock = new object();

    public FileLogger(string filePath, LogLevel minLogLevel = LogLevel.Trace)
    {
        _filePath = filePath;
        _minLogLevel = minLogLevel;

        string directory = Path.GetDirectoryName(filePath);
        if (!string.IsNullOrEmpty(directory) && !Directory.Exists(directory))
        {
            Directory.CreateDirectory(directory);
        }
        if (File.Exists(filePath))
        {
            File.Delete(filePath);
        }
    }

    public IDisposable BeginScope<TState>(TState state) => null;

    public bool IsEnabled(LogLevel logLevel) => logLevel >= _minLogLevel;

    public void Log<TState>(
        LogLevel logLevel,
        EventId eventId,
        TState state,
        Exception exception,
        Func<TState, Exception, string> formatter)
    {
        if (!IsEnabled(logLevel))
        {
            return;
        }

        string message = formatter(state, exception);
        string timestamp = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff");
        string logEntry = $"{timestamp} [{logLevel}] {message}";

        if (exception != null)
        {
            logEntry += $"\nException: {exception}";
        }

        lock (_lock)
        {
            try
            {
                File.AppendAllText(_filePath, logEntry + Environment.NewLine);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Failed to write to log file: {ex.Message}");
            }
        }
    }
}

public class FileLoggerProvider : ILoggerProvider
{
    private readonly string _filePath;
    private readonly LogLevel _minLogLevel;

    public FileLoggerProvider(string filePath, LogLevel minLogLevel = LogLevel.Information)
    {
        _filePath = filePath;
        _minLogLevel = minLogLevel;
    }

    public ILogger CreateLogger(string categoryName)
    {
        return new FileLogger(_filePath, _minLogLevel);
    }

    public void Dispose()
    {
        // Nothing to dispose
    }
}

public static class FileLoggerExtensions
{
    public static ILoggingBuilder AddFileLogger(
        this ILoggingBuilder factory,
        string filePath,
        LogLevel minLogLevel = LogLevel.Information)
    {
        factory.AddProvider(new FileLoggerProvider(filePath, minLogLevel));
        return factory;
    }
}
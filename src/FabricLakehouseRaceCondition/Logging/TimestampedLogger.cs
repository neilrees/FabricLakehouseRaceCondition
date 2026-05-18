namespace FabricLakehouseRaceCondition.Logging;

/// <summary>
/// Thread-safe console logger that prefixes every line with an ISO-8601 timestamp
/// and colour-codes output by severity level.
/// </summary>
public static class TimestampedLogger
{
    private static readonly object Lock = new();

    public enum Level
    {
        Info,
        Warning,
        Error,
        Success,
        Phase
    }

    // ── Public API ───────────────────────────────────────────────────

    public static void Info(string message) => Log(Level.Info, message);
    public static void Warn(string message) => Log(Level.Warning, message);
    public static void Error(string message) => Log(Level.Error, message);
    public static void Success(string message) => Log(Level.Success, message);
    public static void Phase(string message) => Log(Level.Phase, message);

    public static void Banner(string text)
    {
        var bar = new string('═', text.Length + 4);
        Phase($"╔{bar}╗");
        Phase($"║  {text}  ║");
        Phase($"╚{bar}╝");
    }

    public static void Separator() => Phase(new string('─', 80));

    // ── Internals ────────────────────────────────────────────────────

    private static void Log(Level level, string message)
    {
        var timestamp = DateTimeOffset.UtcNow.ToString("yyyy-MM-ddTHH:mm:ss.fffZ");
        var colour = level switch
        {
            Level.Info    => ConsoleColor.White,
            Level.Warning => ConsoleColor.Yellow,
            Level.Error   => ConsoleColor.Red,
            Level.Success => ConsoleColor.Green,
            Level.Phase   => ConsoleColor.Cyan,
            _             => ConsoleColor.Gray
        };

        lock (Lock)
        {
            var prev = Console.ForegroundColor;
            Console.ForegroundColor = colour;
            Console.WriteLine($"[{timestamp}] {message}");
            Console.ForegroundColor = prev;
        }
    }
}

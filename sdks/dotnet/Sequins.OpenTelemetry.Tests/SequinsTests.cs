using Sequins.OpenTelemetry;
using Xunit;

namespace Sequins.OpenTelemetry.Tests;

public class SequinsTests
{
    [Fact]
    public void Build_ReturnsHandle()
    {
        using var handle = new SequinsBuilder("test-service").Build();
        Assert.NotNull(handle);
    }

    [Fact]
    public void TracerProvider_IsNotNull()
    {
        using var handle = new SequinsBuilder("test-tracer").Build();
        Assert.NotNull(handle.TracerProvider);
    }

    [Fact]
    public void MeterProvider_IsNotNull()
    {
        using var handle = new SequinsBuilder("test-meter").Build();
        Assert.NotNull(handle.MeterProvider);
    }

    [Fact]
    public void LoggerProvider_IsNotNull()
    {
        using var handle = new SequinsBuilder("test-logger").Build();
        Assert.NotNull(handle.LoggerProvider);
    }

    [Fact]
    public void Dispose_DoesNotThrow()
    {
        var handle = new SequinsBuilder("test-dispose").Build();
        var ex = Record.Exception(() => handle.Dispose());
        Assert.Null(ex);
    }

    [Fact]
    public void CustomOptions_Applied()
    {
        var options = new SequinsOptions
        {
            ServiceName = "custom-service",
            Endpoint = "http://localhost:4317",
        };
        using var handle = new SequinsBuilder(options).Build();
        Assert.NotNull(handle);
    }
}

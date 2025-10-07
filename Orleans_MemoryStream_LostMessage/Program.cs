// <configuration>

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Orleans.Streams;


var builder = new HostBuilder();

builder.UseOrleans(static siloBuilder =>
{
    siloBuilder.UseLocalhostClustering();
    siloBuilder.AddMemoryGrainStorage("PubSubStore");
    siloBuilder.AddMemoryStreams(Constants.StreamProviderName, cfg =>
    {
        cfg.ConfigureCacheEviction(cacheEvictionOptions =>
        {
            cacheEvictionOptions.Configure(options =>
            {
                options.DataMaxAgeInCache = TimeSpan.FromSeconds(1);
                options.DataMinTimeInCache = TimeSpan.FromSeconds(1);
                options.MetadataMinTimeInCache = TimeSpan.FromSeconds(22);
            });
        });

        cfg.ConfigurePullingAgent(pullingAgentOptions =>
        {
            pullingAgentOptions.Configure(opt =>
            {
                opt.StreamInactivityPeriod = TimeSpan.FromSeconds(10);
            });
        });
    });
});

using var host = builder.Build();
await host.StartAsync();

var consumerGrain = host.Services.GetRequiredService<IGrainFactory>().GetGrain<IConsumerGrain>(Guid.Empty);
await consumerGrain.Subscribe();

var producerGrain = host.Services.GetRequiredService<IGrainFactory>().GetGrain<IEventProducerTestGrain>(Guid.Empty);

var totalMessage = 16 * 1024 + 10;
for (var i = 0; i < totalMessage; i++)
{
    await producerGrain.Produce(1);
}

while (true)
{
    var consumedMessageCount = await consumerGrain.GetConsumedMessageCount();
    if (consumedMessageCount == totalMessage)
    {
        break;
    }

    Console.WriteLine($"----- Consumed {consumedMessageCount} waiting 2 sec --------");
    await Task.Delay(TimeSpan.FromSeconds(2));
}

await consumerGrain.SetEnabledLogging(true);

Console.WriteLine("----- Waiting for 5 minutes --------");

await Task.Delay(TimeSpan.FromMinutes(5));

await producerGrain.Produce(2);
Console.WriteLine("----- message 2 produced --------");

await producerGrain.Produce(3);
Console.WriteLine("----- message 2 produced --------");

// Observe the console. Message 2 is lost.

await Task.Delay(TimeSpan.MaxValue);

public interface IEventProducerTestGrain : IGrainWithGuidKey
{
    Task Produce(int item);
}

public sealed class EventProducerTestGrain : Grain, IEventProducerTestGrain
{
    private IAsyncStream<int>? _stream;

    public override Task OnActivateAsync(CancellationToken cancellationToken)
    {
        var streamId = StreamId.Create(Constants.NamespaceName, Guid.Empty);
        _stream = this.GetStreamProvider(Constants.StreamProviderName).GetStream<int>(streamId);

        return base.OnActivateAsync(cancellationToken);
    }

    public async Task Produce(int item)
    {
        await _stream!.OnNextAsync(item);
    }
}

public interface IConsumerGrain : IGrainWithGuidKey
{
    Task Subscribe();

    Task<int> GetConsumedMessageCount();

    Task SetEnabledLogging(bool enableLogging);
}

public class ConsumerGrain : Grain, IConsumerGrain, IAsyncObserver<int>
{
    private readonly IClusterClient _clusterClient;
    private int _consumedMessageCount;
    private bool _enableLogging;

    public ConsumerGrain(IClusterClient clusterClient)
    {
        _clusterClient = clusterClient;
    }

    public async Task Subscribe()
    {
        var streamProvider = _clusterClient.GetStreamProvider(Constants.StreamProviderName);
        var streamId = StreamId.Create(Constants.NamespaceName, Guid.Empty);
        var stream = streamProvider.GetStream<int>(streamId);
        await stream.SubscribeAsync(this);
    }

    public Task SetEnabledLogging(bool enableLogging)
    {
        _enableLogging = enableLogging;
        return Task.CompletedTask;
    }

    public Task<int> GetConsumedMessageCount()
    {
        return Task.FromResult(_consumedMessageCount);
    }

    Task IAsyncObserver<int>.OnCompletedAsync() => Task.CompletedTask;

    Task IAsyncObserver<int>.OnErrorAsync(Exception ex)
    {
        Console.WriteLine($"OnErrorAsync: {ex}");
        return Task.CompletedTask;
    }

    Task IAsyncObserver<int>.OnNextAsync(int item, StreamSequenceToken? token = null)
    {
        Interlocked.Increment(ref _consumedMessageCount);
        if (_enableLogging)
        {
            Console.WriteLine($"OnNextAsync: Item: {item}, Token = {token}");
        }

        return Task.CompletedTask;
    }
}

public static class Constants
{
    public const string StreamProviderName = "StreamProvider-1";
    public const string NamespaceName = "ns-1";
}
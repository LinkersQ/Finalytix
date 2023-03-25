#if NETCOREAPP3_1_OR_GREATER
using JetBrains.Annotations;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Telegram.Bot.Extensions.Polling.Extensions;
using Telegram.Bot.Requests;
using Telegram.Bot.Types;
using Telegram.Bot.Types.Enums;

// ReSharper disable once CheckNamespace
namespace Telegram.Bot.Extensions.Polling;
/// <summary>
/// Supports asynchronous iteration over <see cref="Update"/>s
/// </summary>
[PublicAPI]
public class BlockingUpdateReceiver : IAsyncEnumerable<Update>
{
    private readonly ReceiverOptions? _receiverOptions;
    private readonly ITelegramBotClient _botClient;
    private readonly Func<Exception, CancellationToken, Task>? _errorHandler;
    private int _inProcess;

    /// <summary>
    /// Constructs a new <see cref="BlockingUpdateReceiver"/> for the specified <see cref="ITelegramBotClient"/>
    /// </summary>
    /// <param name="botClient">The <see cref="ITelegramBotClient"/> used for making GetUpdates calls</param>
    /// <param name="receiverOptions"></param>
    /// <param name="errorHandler">
    /// The function used to handle <see cref="Exception"/>s thrown by ReceiveUpdates
    /// </param>
    public BlockingUpdateReceiver(
        ITelegramBotClient botClient,
        ReceiverOptions? receiverOptions = default,
        Func<Exception, CancellationToken, Task>? errorHandler = default)
    {
        _botClient = botClient ?? throw new ArgumentNullException(nameof(botClient));
        _receiverOptions = receiverOptions;
        _errorHandler = errorHandler;
    }

    /// <summary>
    /// Gets the <see cref="IAsyncEnumerator{Update}"/>. This method may only be called once.
    /// </summary>
    /// <param name="cancellationToken">
    /// The <see cref="CancellationToken"/> with which you can stop receiving
    /// </param>
    public IAsyncEnumerator<Update> GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
        if (Interlocked.CompareExchange(ref _inProcess, 1, 0) == 1)
        {
            throw new InvalidOperationException(nameof(GetAsyncEnumerator) + " may only be called once");
        }

        return new Enumerator(receiver: this, cancellationToken: cancellationToken);
    }

    private class Enumerator : IAsyncEnumerator<Update>
    {
        private readonly BlockingUpdateReceiver _receiver;
        private readonly CancellationTokenSource _cts;
        private readonly CancellationToken _token;
        private readonly UpdateType[]? _allowedUpdates;
        private readonly int? _limit;
        private Update[] _updateArray = Array.Empty<Update>();
        private int _updateIndex;
        private int _messageOffset;
        private bool _updatesThrown;

        public Enumerator(BlockingUpdateReceiver receiver, CancellationToken cancellationToken)
        {
            _receiver = receiver;
            _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, default);
            _token = _cts.Token;
            _messageOffset = receiver._receiverOptions?.Offset ?? 0;
            _limit = receiver._receiverOptions?.Limit ?? default;
            _allowedUpdates = receiver._receiverOptions?.AllowedUpdates;
        }

        public ValueTask<bool> MoveNextAsync()
        {
            _token.ThrowIfCancellationRequested();

            _updateIndex += 1;

            return _updateIndex < _updateArray.Length
                ? new(true)
                : new(ReceiveUpdatesAsync());
        }

        private async Task<bool> ReceiveUpdatesAsync()
        {
            var shouldThrowPendingUpdates = (
                _updatesThrown,
                _receiver._receiverOptions?.ThrowPendingUpdates ?? false
            );

            if (shouldThrowPendingUpdates is (false, true))
            {
                try
                {
                    _messageOffset = await _receiver._botClient.ThrowOutPendingUpdatesAsync(
                        cancellationToken: _token
                    );
                }
                catch (OperationCanceledException)
                {
                    // ignored
                }
                finally
                {
                    _updatesThrown = true;
                }
            }

            _updateArray = Array.Empty<Update>();
            _updateIndex = 0;

            while (_updateArray.Length == 0)
            {
                try
                {
                    _updateArray = await _receiver._botClient
                        .MakeRequestAsync(
                            request: new GetUpdatesRequest
                            {
                                Offset = _messageOffset,
                                Timeout = (int)_receiver._botClient.Timeout.TotalSeconds,
                                Limit = _limit,
                                AllowedUpdates = _allowedUpdates,
                            },
                            cancellationToken: _token
                        )
                        .ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    throw;
                }
                catch (Exception ex) when (_receiver._errorHandler is not null)
                {
                    await _receiver._errorHandler(ex, _token).ConfigureAwait(false);
                }
            }

            _messageOffset = _updateArray[^1].Id + 1;
            return true;
        }

        public Update Current => _updateArray[_updateIndex];

        public ValueTask DisposeAsync()
        {
            _cts.Cancel();
            _cts.Dispose();
            return new();
        }
    }
}
#endif

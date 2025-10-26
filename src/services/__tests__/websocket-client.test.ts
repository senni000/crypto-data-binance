/**
 * Unit tests for DeribitWebSocketClient
 * Tests WebSocket connection, subscription, and reconnection functionality
 */

import { DeribitWebSocketClient, DeribitTradeMessage } from '../websocket-client';
import WebSocket from 'ws';
import { EventEmitter } from 'events';

// Mock WebSocket
jest.mock('ws');
const MockedWebSocket = WebSocket as jest.MockedClass<typeof WebSocket>;

describe('DeribitWebSocketClient', () => {
  let client: DeribitWebSocketClient;
  let mockWs: any;

  beforeEach(() => {
    jest.clearAllMocks();
    
    // Create mock WebSocket instance
    mockWs = new EventEmitter();
    mockWs.readyState = WebSocket.CONNECTING;
    mockWs.send = jest.fn();
    mockWs.close = jest.fn();
    
    // Mock WebSocket constructor
    MockedWebSocket.mockImplementation(() => mockWs);
    
    client = new DeribitWebSocketClient({
      url: 'wss://test.deribit.com/ws/api/v2',
      reconnectInterval: 100,
      maxReconnectAttempts: 3,
      heartbeatInterval: 1000,
    });
  });

  afterEach(() => {
    client.disconnect();
    jest.clearAllTimers();
  });

  describe('Connection Management', () => {
    it('should connect to WebSocket successfully', async () => {
      const connectPromise = client.connect();
      
      // Simulate successful connection
      mockWs.readyState = WebSocket.OPEN;
      mockWs.emit('open');
      
      await expect(connectPromise).resolves.not.toThrow();
      expect(MockedWebSocket).toHaveBeenCalledWith('wss://test.deribit.com/ws/api/v2');
    });

    it('should handle connection timeout', async () => {
      jest.useFakeTimers();
      
      const connectPromise = client.connect();
      
      // Fast-forward past timeout
      jest.advanceTimersByTime(10000);
      
      await expect(connectPromise).rejects.toThrow('Connection timeout');
      
      jest.useRealTimers();
    });

    it('should handle connection error', async () => {
      const connectPromise = client.connect();
      
      const error = new Error('Connection failed');
      mockWs.emit('error', error);
      
      await expect(connectPromise).rejects.toThrow('Connection failed');
    });

    it('should disconnect properly', async () => {
      mockWs.readyState = WebSocket.OPEN;
      const connectPromise = client.connect();
      mockWs.emit('open');
      await connectPromise;

      client.disconnect();
      
      expect(mockWs.close).toHaveBeenCalled();
    });

    it('should return correct connection status', async () => {
      expect(client.isConnected()).toBe(false);
      
      mockWs.readyState = WebSocket.OPEN;
      const connectPromise = client.connect();
      mockWs.emit('open');
      await connectPromise;

      expect(client.isConnected()).toBe(true);
    });
  });

  describe('Subscription Management', () => {
    beforeEach(async () => {
      // Setup connected state
      mockWs.readyState = WebSocket.OPEN;
      const connectPromise = client.connect();
      mockWs.emit('open');
      await connectPromise;
    });

    it('should subscribe to BTC trades successfully', async () => {
      await client.subscribeToBTCTrades();
      
      // Should send subscription messages for BTC channels
      expect(mockWs.send).toHaveBeenCalledTimes(2);
      
      const calls = (mockWs.send as jest.Mock).mock.calls;
      const message1 = JSON.parse(calls[0][0]);
      const message2 = JSON.parse(calls[1][0]);
      
      expect(message1.method).toBe('public/subscribe');
      expect(message1.params.channels).toContain('trades.BTC-PERPETUAL.100ms');
      
      expect(message2.method).toBe('public/subscribe');
      expect(message2.params.channels).toContain('trades.BTC-USD.100ms');
    });

    it('should throw error when subscribing without connection', async () => {
      mockWs.readyState = WebSocket.CLOSED;
      
      await expect(client.subscribeToBTCTrades()).rejects.toThrow('WebSocket is not connected');
    });

    it('should track subscriptions correctly', async () => {
      await client.subscribeToBTCTrades();
      
      const subscriptions = client.getSubscriptions();
      expect(subscriptions).toContain('trades.BTC-PERPETUAL.100ms');
      expect(subscriptions).toContain('trades.BTC-USD.100ms');
    });
  });

  describe('Message Handling', () => {
    beforeEach(async () => {
      mockWs.readyState = WebSocket.OPEN;
      const connectPromise = client.connect();
      mockWs.emit('open');
      await connectPromise;
    });

    it('should parse and emit trade data correctly', (done) => {
      const mockTradeMessage: DeribitTradeMessage = {
        jsonrpc: '2.0',
        method: 'subscription',
        params: {
          channel: 'trades.BTC-PERPETUAL.100ms',
          data: [{
            trade_seq: 1,
            trade_id: 'test_trade_1',
            timestamp: Date.now(),
            tick_direction: 1,
            price: 45000,
            mark_price: 45000,
            instrument_name: 'BTC-PERPETUAL',
            index_price: 45000,
            direction: 'buy',
            amount: 0.1
          }]
        }
      };

      client.on('tradeData', (trades) => {
        expect(trades).toHaveLength(1);
        expect(trades[0]).toMatchObject({
          symbol: 'BTC-PERPETUAL',
          price: 45000,
          amount: 0.1,
          direction: 'buy',
          tradeId: 'test_trade_1'
        });
        done();
      });

      mockWs.emit('message', JSON.stringify(mockTradeMessage));
    });

    it('should handle invalid JSON messages gracefully', (done) => {
      client.on('error', (error) => {
        expect(error).toBeInstanceOf(Error);
        done();
      });

      mockWs.emit('message', 'invalid json');
    });

    it('should ignore subscription confirmation messages', () => {
      const confirmationMessage = {
        jsonrpc: '2.0',
        id: 123,
        result: ['trades.BTC-PERPETUAL.100ms']
      };

      const tradeDataSpy = jest.fn();
      client.on('tradeData', tradeDataSpy);

      mockWs.emit('message', JSON.stringify(confirmationMessage));

      expect(tradeDataSpy).not.toHaveBeenCalled();
    });
  });

  describe('Reconnection Logic', () => {
    beforeEach(() => {
      jest.useFakeTimers();
    });

    afterEach(() => {
      jest.useRealTimers();
    });

    it('should attempt reconnection on connection close', async () => {
      // Initial connection
      mockWs.readyState = WebSocket.OPEN;
      const connectPromise = client.connect();
      mockWs.emit('open');
      await connectPromise;

      // Reset mock to track reconnection attempts
      MockedWebSocket.mockClear();

      // Simulate connection close
      mockWs.readyState = WebSocket.CLOSED;
      mockWs.emit('close', 1000, Buffer.from('Normal closure'));

      // Fast-forward to trigger reconnection
      jest.advanceTimersByTime(100);

      expect(MockedWebSocket).toHaveBeenCalledTimes(1);
    });

    it('should use exponential backoff for reconnection delays', async () => {
      // Initial connection
      mockWs.readyState = WebSocket.OPEN;
      const connectPromise = client.connect();
      mockWs.emit('open');
      await connectPromise;

      MockedWebSocket.mockClear();
      const errorListener = jest.fn();
      client.on('error', errorListener);
      
      // Simulate multiple connection failures
      for (let i = 0; i < 3; i++) {
        mockWs.readyState = WebSocket.CLOSED;
        mockWs.emit('close', 1006, Buffer.from('Abnormal closure'));

        // Fast-forward to trigger reconnection
        const expectedDelay = 100 * Math.pow(2, i);
        jest.advanceTimersByTime(expectedDelay);

        expect(MockedWebSocket).toHaveBeenCalledTimes(i + 1);

        // Simulate connection failure
        mockWs.emit('error', new Error('Connection failed'));
      }
    });

    it('should stop reconnecting after max attempts', async () => {
      // Initial connection
      mockWs.readyState = WebSocket.OPEN;
      const connectPromise = client.connect();
      mockWs.emit('open');
      await connectPromise;

      MockedWebSocket.mockClear();
      
      const maxReconnectSpy = jest.fn();
      client.on('maxReconnectAttemptsReached', maxReconnectSpy);
      client.on('error', jest.fn());

      // Simulate connection failures beyond max attempts
      for (let i = 0; i < 4; i++) {
        mockWs.readyState = WebSocket.CLOSED;
        mockWs.emit('close', 1006, Buffer.from('Abnormal closure'));

        if (i < 3) {
          jest.advanceTimersByTime(100 * Math.pow(2, i));
          mockWs.emit('error', new Error('Connection failed'));
        }
      }

      expect(maxReconnectSpy).toHaveBeenCalled();
    });

    it('should resubscribe to channels after reconnection', async () => {
      // Initial connection and subscription
      mockWs.readyState = WebSocket.OPEN;
      const connectPromise = client.connect();
      mockWs.emit('open');
      await connectPromise;

      await client.subscribeToBTCTrades();
      
      // Clear send calls from initial subscription
      (mockWs.send as jest.Mock).mockClear();

      // Simulate reconnection
      mockWs.readyState = WebSocket.CLOSED;
      mockWs.emit('close', 1000, Buffer.from('Normal closure'));

      jest.advanceTimersByTime(100);

      // Simulate successful reconnection
      mockWs.readyState = WebSocket.OPEN;
      mockWs.emit('open');

      // Should resubscribe to previous channels
      expect(mockWs.send).toHaveBeenCalledTimes(2);
    });

    it('should not reconnect when explicitly disconnected', async () => {
      // Initial connection
      mockWs.readyState = WebSocket.OPEN;
      const connectPromise = client.connect();
      mockWs.emit('open');
      await connectPromise;

      MockedWebSocket.mockClear();

      // Explicit disconnect
      client.disconnect();

      // Simulate close event
      mockWs.emit('close', 1000, Buffer.from('Normal closure'));

      // Fast-forward time
      jest.advanceTimersByTime(1000);

      // Should not attempt reconnection
      expect(MockedWebSocket).not.toHaveBeenCalled();
    });
  });

  describe('Heartbeat Functionality', () => {
    beforeEach(() => {
      jest.useFakeTimers();
    });

    afterEach(() => {
      jest.useRealTimers();
    });

    it('should send heartbeat messages periodically', async () => {
      mockWs.readyState = WebSocket.OPEN;
      const connectPromise = client.connect();
      mockWs.emit('open');
      await connectPromise;

      // Clear initial connection messages
      (mockWs.send as jest.Mock).mockClear();

      // Fast-forward to trigger heartbeat
      jest.advanceTimersByTime(1000);

      expect(mockWs.send).toHaveBeenCalledWith(
        expect.stringContaining('"method":"public/ping"')
      );
    });

    it('should stop heartbeat when disconnected', async () => {
      mockWs.readyState = WebSocket.OPEN;
      const connectPromise = client.connect();
      mockWs.emit('open');
      await connectPromise;

      client.disconnect();

      (mockWs.send as jest.Mock).mockClear();

      // Fast-forward time
      jest.advanceTimersByTime(2000);

      // Should not send heartbeat after disconnect
      expect(mockWs.send).not.toHaveBeenCalled();
    });
  });

  describe('Error Handling', () => {
    it('should emit error events for WebSocket errors', async () => {
      mockWs.readyState = WebSocket.OPEN;
      const connectPromise = client.connect();
      mockWs.emit('open');
      await connectPromise;

      const errorListener = jest.fn();
      client.on('error', errorListener);

      mockWs.emit('error', new Error('Test error'));

      expect(errorListener).toHaveBeenCalledWith(expect.any(Error));
      expect((errorListener.mock.calls[0][0] as Error).message).toBe('Test error');
    });

    it('should handle multiple connection attempts gracefully', async () => {
      mockWs.readyState = WebSocket.CONNECTING;

      // Start multiple connection attempts
      const promise1 = client.connect();
      const promise2 = client.connect();

      mockWs.readyState = WebSocket.OPEN;
      mockWs.emit('open');

      await expect(promise1).resolves.not.toThrow();
      await expect(promise2).resolves.not.toThrow();

      // Should only create one WebSocket instance
      expect(MockedWebSocket).toHaveBeenCalledTimes(1);
    });
  });
});

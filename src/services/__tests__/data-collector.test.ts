/**
 * Unit tests for DataCollector
 * Tests integration of WebSocket and REST clients with reconnection functionality
 */

import { DataCollector } from '../data-collector';
import { DeribitWebSocketClient } from '../websocket-client';
import { DeribitRestClient } from '../rest-client';
import { IDatabaseManager } from '../interfaces';
import { TradeData, OptionData } from '../../types';

// Mock the client classes
jest.mock('../websocket-client');
jest.mock('../rest-client');

const MockedWebSocketClient = DeribitWebSocketClient as jest.MockedClass<typeof DeribitWebSocketClient>;
const MockedRestClient = DeribitRestClient as jest.MockedClass<typeof DeribitRestClient>;

describe('DataCollector', () => {
  let dataCollector: DataCollector;
  let mockDatabaseManager: jest.Mocked<IDatabaseManager>;
  let mockWsClient: jest.Mocked<DeribitWebSocketClient>;
  let mockRestClient: jest.Mocked<DeribitRestClient>;

  beforeEach(() => {
    jest.clearAllMocks();

    // Mock database manager
    mockDatabaseManager = {
      initializeDatabase: jest.fn(),
      saveTradeData: jest.fn(),
      saveOptionData: jest.fn(),
      getTradeDataLast24Hours: jest.fn(),
      getLatestOptionData: jest.fn(),
      saveCVDData: jest.fn(),
      getCVDDataLast24Hours: jest.fn(),
    };

    // Mock WebSocket client
    mockWsClient = {
      connect: jest.fn(),
      disconnect: jest.fn(),
      subscribeToBTCTrades: jest.fn(),
      isConnected: jest.fn(),
      getSubscriptions: jest.fn(),
      on: jest.fn(),
      emit: jest.fn(),
      removeAllListeners: jest.fn(),
    } as any;

    // Mock REST client
    mockRestClient = {
      startPeriodicCollection: jest.fn(),
      stopPeriodicCollection: jest.fn(),
      testConnection: jest.fn(),
      collectOptionData: jest.fn(),
      isCollecting: jest.fn(),
      getInterval: jest.fn(),
      setInterval: jest.fn(),
      on: jest.fn(),
      emit: jest.fn(),
      removeAllListeners: jest.fn(),
    } as any;

    MockedWebSocketClient.mockImplementation(() => mockWsClient);
    MockedRestClient.mockImplementation(() => mockRestClient);

    dataCollector = new DataCollector(mockDatabaseManager, {
      websocketUrl: 'wss://test.deribit.com/ws/api/v2',
      restApiUrl: 'https://test.deribit.com/api/v2',
      optionDataInterval: 1000,
      enableTradeCollection: true,
      enableOptionCollection: true,
    });
  });

  afterEach(() => {
    jest.clearAllTimers();
  });

  describe('Initialization', () => {
    it('should create WebSocket and REST clients with correct options', () => {
      expect(MockedWebSocketClient).toHaveBeenCalledWith({
        url: 'wss://test.deribit.com/ws/api/v2',
        reconnectInterval: 5000,
        maxReconnectAttempts: 10,
        heartbeatInterval: 30000,
      });

      expect(MockedRestClient).toHaveBeenCalledWith({
        baseUrl: 'https://test.deribit.com/api/v2',
        timeout: 10000,
        maxRetries: 3,
        retryDelay: 1000,
        intervalMs: 1000,
      });
    });

    it('should setup event handlers for clients', () => {
      expect(mockWsClient.on).toHaveBeenCalledWith('tradeData', expect.any(Function));
      expect(mockWsClient.on).toHaveBeenCalledWith('connected', expect.any(Function));
      expect(mockWsClient.on).toHaveBeenCalledWith('disconnected', expect.any(Function));
      expect(mockWsClient.on).toHaveBeenCalledWith('error', expect.any(Function));

      expect(mockRestClient.on).toHaveBeenCalledWith('optionData', expect.any(Function));
      expect(mockRestClient.on).toHaveBeenCalledWith('error', expect.any(Function));
    });
  });

  describe('Trade Data Collection', () => {
    it('should start trade data collection successfully', async () => {
      mockWsClient.connect.mockResolvedValue();
      mockWsClient.subscribeToBTCTrades.mockResolvedValue();

      await dataCollector.startTradeDataCollection();

      expect(mockWsClient.connect).toHaveBeenCalled();
      expect(mockWsClient.subscribeToBTCTrades).toHaveBeenCalled();
    });

    it('should handle trade data collection errors', async () => {
      const error = new Error('Connection failed');
      mockWsClient.connect.mockRejectedValue(error);

      await expect(dataCollector.startTradeDataCollection()).rejects.toThrow('Connection failed');
    });

    it('should skip trade collection when disabled', async () => {
      const disabledCollector = new DataCollector(mockDatabaseManager, {
        websocketUrl: 'wss://test.deribit.com/ws/api/v2',
        enableTradeCollection: false,
      });

      await disabledCollector.startTradeDataCollection();

      expect(mockWsClient.connect).not.toHaveBeenCalled();
    });
  });

  describe('Option Data Collection', () => {
    it('should start option data collection successfully', async () => {
      mockRestClient.testConnection.mockResolvedValue(true);

      await dataCollector.startOptionDataCollection();

      expect(mockRestClient.testConnection).toHaveBeenCalled();
      expect(mockRestClient.startPeriodicCollection).toHaveBeenCalled();
    });

    it('should handle REST API connection failure', async () => {
      mockRestClient.testConnection.mockResolvedValue(false);

      await expect(dataCollector.startOptionDataCollection()).rejects.toThrow('Failed to connect to Deribit REST API');
    });

    it('should skip option collection when disabled', async () => {
      const disabledCollector = new DataCollector(mockDatabaseManager, {
        websocketUrl: 'wss://test.deribit.com/ws/api/v2',
        enableOptionCollection: false,
      });

      await disabledCollector.startOptionDataCollection();

      expect(mockRestClient.testConnection).not.toHaveBeenCalled();
    });
  });

  describe('Complete System Start/Stop', () => {
    beforeEach(() => {
      jest.useFakeTimers();
    });

    afterEach(() => {
      jest.useRealTimers();
    });

    it('should start complete data collection system', async () => {
      mockWsClient.connect.mockResolvedValue();
      mockWsClient.subscribeToBTCTrades.mockResolvedValue();
      mockRestClient.testConnection.mockResolvedValue(true);

      await dataCollector.start();

      expect(mockWsClient.connect).toHaveBeenCalled();
      expect(mockWsClient.subscribeToBTCTrades).toHaveBeenCalled();
      expect(mockRestClient.testConnection).toHaveBeenCalled();
      expect(mockRestClient.startPeriodicCollection).toHaveBeenCalled();
    });

    it('should stop all collection processes', async () => {
      mockDatabaseManager.saveTradeData.mockResolvedValue();
      mockDatabaseManager.saveOptionData.mockResolvedValue();

      await dataCollector.stopCollection();

      expect(mockWsClient.disconnect).toHaveBeenCalled();
      expect(mockRestClient.stopPeriodicCollection).toHaveBeenCalled();
    });

    it('should handle start errors gracefully', async () => {
      const error = new Error('Start failed');
      mockWsClient.connect.mockRejectedValue(error);

      await expect(dataCollector.start()).rejects.toThrow('Start failed');
    });
  });

  describe('Data Validation and Buffering', () => {
    beforeEach(() => {
      jest.useFakeTimers();
    });

    afterEach(() => {
      jest.useRealTimers();
    });

    it('should validate and buffer trade data', () => {
      const validTrades: TradeData[] = [{
        symbol: 'BTC-PERPETUAL',
        timestamp: Date.now(),
        price: 45000,
        amount: 0.1,
        direction: 'buy',
        tradeId: 'valid_trade'
      }];

      const invalidTrades: TradeData[] = [{
        symbol: '',
        timestamp: Date.now(),
        price: -100, // Invalid price
        amount: 0.1,
        direction: 'buy',
        tradeId: 'invalid_trade'
      }];

      // Get the trade data handler
      const tradeDataHandler = (mockWsClient.on as jest.Mock).mock.calls
        .find(call => call[0] === 'tradeData')[1];

      const receivedSpy = jest.fn();
      dataCollector.on('tradeDataReceived', receivedSpy);

      // Test valid data
      tradeDataHandler(validTrades);
      expect(receivedSpy).toHaveBeenCalledWith(validTrades);

      // Test invalid data
      receivedSpy.mockClear();
      tradeDataHandler(invalidTrades);
      expect(receivedSpy).not.toHaveBeenCalled();
    });

    it('should validate and buffer option data', () => {
      const validOptions: OptionData[] = [{
        symbol: 'BTC-29DEC23-45000-C',
        timestamp: Date.now(),
        underlyingPrice: 45000,
        markPrice: 1500,
        impliedVolatility: 0.65,
        delta: 0.5,
        gamma: 0.001,
        theta: -10,
        vega: 50,
        rho: 5
      }];

      const invalidOptions: OptionData[] = [{
        symbol: '',
        timestamp: Date.now(),
        underlyingPrice: -100, // Invalid price
        markPrice: 1500,
        impliedVolatility: 0.65,
        delta: 0.5,
        gamma: 0.001,
        theta: -10,
        vega: 50,
        rho: 5
      }];

      // Get the option data handler
      const optionDataHandler = (mockRestClient.on as jest.Mock).mock.calls
        .find(call => call[0] === 'optionData')[1];

      const receivedSpy = jest.fn();
      dataCollector.on('optionDataReceived', receivedSpy);

      // Test valid data
      optionDataHandler(validOptions);
      expect(receivedSpy).toHaveBeenCalledWith(validOptions);

      // Test invalid data
      receivedSpy.mockClear();
      optionDataHandler(invalidOptions);
      expect(receivedSpy).not.toHaveBeenCalled();
    });

    it('should flush buffers periodically', async () => {
      mockDatabaseManager.saveTradeData.mockResolvedValue();
      mockDatabaseManager.saveOptionData.mockResolvedValue();

      // Start the collector to initialize buffer flushing
      mockWsClient.connect.mockResolvedValue();
      mockWsClient.subscribeToBTCTrades.mockResolvedValue();
      mockRestClient.testConnection.mockResolvedValue(true);

      await dataCollector.start();

      // Add some data to buffers
      const tradeDataHandler = (mockWsClient.on as jest.Mock).mock.calls
        .find(call => call[0] === 'tradeData')[1];

      const validTrades: TradeData[] = [{
        symbol: 'BTC-PERPETUAL',
        timestamp: Date.now(),
        price: 45000,
        amount: 0.1,
        direction: 'buy',
        tradeId: 'test_trade'
      }];

      tradeDataHandler(validTrades);

      // Fast-forward to trigger buffer flush
      jest.advanceTimersByTime(5000);

      expect(mockDatabaseManager.saveTradeData).toHaveBeenCalledWith(validTrades);
    });

    it('should force flush buffers when requested', async () => {
      mockDatabaseManager.saveTradeData.mockResolvedValue();
      mockDatabaseManager.saveOptionData.mockResolvedValue();

      await dataCollector.forceFlush();

      expect(mockDatabaseManager.saveTradeData).toHaveBeenCalled();
      expect(mockDatabaseManager.saveOptionData).toHaveBeenCalled();
    });
  });

  describe('Status and Monitoring', () => {
    it('should return correct status information', () => {
      mockWsClient.isConnected.mockReturnValue(true);
      mockRestClient.isCollecting.mockReturnValue(true);

      const status = dataCollector.getStatus();

      expect(status).toMatchObject({
        isRunning: false, // Not started yet
        websocketConnected: true,
        restClientRunning: true,
        tradeBufferSize: 0,
        optionBufferSize: 0,
      });
    });

    it('should return buffer sizes', () => {
      const bufferSizes = dataCollector.getBufferSizes();

      expect(bufferSizes).toEqual({
        trades: 0,
        options: 0,
      });
    });
  });

  describe('Error Handling and Reconnection', () => {
    it('should handle WebSocket connection errors', () => {
      const errorHandler = (mockWsClient.on as jest.Mock).mock.calls
        .find(call => call[0] === 'error')[1];

      const errorSpy = jest.fn();
      dataCollector.on('websocketError', errorSpy);

      const error = new Error('WebSocket error');
      errorHandler(error);

      expect(errorSpy).toHaveBeenCalledWith(error);
    });

    it('should handle REST client errors', () => {
      const errorHandler = (mockRestClient.on as jest.Mock).mock.calls
        .find(call => call[0] === 'error')[1];

      const errorSpy = jest.fn();
      dataCollector.on('restError', errorSpy);

      const error = new Error('REST error');
      errorHandler(error);

      expect(errorSpy).toHaveBeenCalledWith(error);
    });

    it('should emit WebSocket connection events', () => {
      const connectedHandler = (mockWsClient.on as jest.Mock).mock.calls
        .find(call => call[0] === 'connected')[1];

      const disconnectedHandler = (mockWsClient.on as jest.Mock).mock.calls
        .find(call => call[0] === 'disconnected')[1];

      const connectedSpy = jest.fn();
      const disconnectedSpy = jest.fn();

      dataCollector.on('websocketConnected', connectedSpy);
      dataCollector.on('websocketDisconnected', disconnectedSpy);

      connectedHandler();
      disconnectedHandler({ code: 1000, reason: 'Normal closure' });

      expect(connectedSpy).toHaveBeenCalled();
      expect(disconnectedSpy).toHaveBeenCalledWith({ code: 1000, reason: 'Normal closure' });
    });

    it('should handle database save errors gracefully', async () => {
      const error = new Error('Database error');
      mockDatabaseManager.saveTradeData.mockRejectedValue(error);

      const errorSpy = jest.fn();
      dataCollector.on('error', errorSpy);

      // Trigger trade data handling
      const tradeDataHandler = (mockWsClient.on as jest.Mock).mock.calls
        .find(call => call[0] === 'tradeData')[1];

      const validTrades: TradeData[] = [{
        symbol: 'BTC-PERPETUAL',
        timestamp: Date.now(),
        price: 45000,
        amount: 0.1,
        direction: 'buy',
        tradeId: 'test_trade'
      }];

      tradeDataHandler(validTrades);

      // Force flush to trigger database save
      await dataCollector.forceFlush();

      expect(errorSpy).toHaveBeenCalledWith(error);
    });
  });

  describe('Configuration Options', () => {
    it('should handle disabled collections', () => {
      const disabledCollector = new DataCollector(mockDatabaseManager, {
        websocketUrl: 'wss://test.deribit.com/ws/api/v2',
        enableTradeCollection: false,
        enableOptionCollection: false,
      });

      expect(disabledCollector).toBeDefined();
    });

    it('should use default configuration values', () => {
      new DataCollector(mockDatabaseManager, {
        websocketUrl: 'wss://test.deribit.com/ws/api/v2',
      });

      expect(MockedRestClient).toHaveBeenCalledWith(
        expect.objectContaining({
          intervalMs: 3600000, // Default 1 hour
        })
      );
    });
  });
});
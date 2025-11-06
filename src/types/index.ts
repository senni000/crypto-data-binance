/**
 * コアドメイン型定義
 * Binance向けデータ収集で扱うエンティティを表現する
 */

import { CvdAlertPayload } from '@crypto-data/cvd-core';

export type MarketType = 'SPOT' | 'USDT-M' | 'COIN-M';

export interface SymbolMetadata {
  symbol: string;
  baseAsset: string;
  quoteAsset: string;
  marketType: MarketType;
  status: 'ACTIVE' | 'INACTIVE';
  onboardDate: number;
  contractType?: string;
  deliveryDate?: number;
  filters: {
    tickSize?: number;
    stepSize?: number;
    minNotional?: number;
  };
  updatedAt: number;
}

export type OHLCVTimeframe = '1m' | '30m' | '1d';

export interface OHLCVData {
  symbol: string;
  interval: OHLCVTimeframe;
  openTime: number;
  closeTime: number;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
  quoteVolume: number;
  trades: number;
}

export type AggTradeSource = 'ws' | 'rest';

export interface AggTrade {
  symbol: string;
  marketType: Extract<MarketType, 'SPOT' | 'USDT-M'>;
  tradeId: number;
  price: number;
  quantity: number;
  firstTradeId: number;
  lastTradeId: number;
  tradeTime: number;
  isBuyerMaker: boolean;
  isBestMatch: boolean;
  source: AggTradeSource;
}

export interface TopTraderPositionData {
  symbol: string;
  timestamp: number;
  longShortRatio: number;
  longAccount: number;
  shortAccount: number;
  longPosition: number;
  shortPosition: number;
}

export interface TopTraderAccountData {
  symbol: string;
  timestamp: number;
  longShortRatio: number;
  longAccount: number;
  shortAccount: number;
}

export interface WebSocketKlinePayload {
  stream: string;
  data: {
    e: string;
    E: number;
    s: string;
    k: {
      t: number;
      T: number;
      s: string;
      i: string;
      f: number;
      L: number;
      o: string;
      c: string;
      h: string;
      l: string;
      v: string;
      n: number;
      x: boolean;
      q: string;
      V: string;
      Q: string;
      B: string;
    };
  };
}

export interface TradeData {
  symbol: string;
  timestamp: number;
  price: number;
  amount: number;
  direction: 'buy' | 'sell';
  tradeId: string;
  marketType: MarketType;
  streamType: 'aggTrade' | 'trade';
}

export interface CVDData {
  symbol: string;
  timestamp: number;
  cvdValue: number;
  zScore: number;
  delta: number;
  deltaZScore: number;
}

export interface AlertHistory {
  id?: number;
  alertType: string;
  symbol: string;
  timestamp: number;
  value: number;
  threshold: number;
  message: string;
  createdAt?: string;
}

export interface TradeDataRow extends TradeData {
  rowId: number;
}

export interface AlertQueueRecord {
  id: number;
  alertType: string;
  payload: CvdAlertPayload;
  attemptCount: number;
  lastError?: string | null;
  processedAt?: number | null;
  enqueuedAt?: number;
}

export interface LiquidationEvent {
  /**
   * 重複排除のためのユニークキー
   */
  eventId: string;
  symbol: string;
  marketType: Extract<MarketType, 'USDT-M' | 'COIN-M'>;
  side: 'buy' | 'sell';
  orderType?: string;
  timeInForce?: string;
  status?: string;
  orderId?: string;
  price: number;
  averagePrice?: number;
  lastFilledPrice?: number;
  originalQuantity: number;
  filledQuantity: number;
  lastFilledQuantity?: number;
  eventTime: number;
  tradeTime?: number;
  isMaker?: boolean;
  reduceOnly?: boolean;
  createdAt: number;
}

export interface CvdStreamConfig {
  symbol: string;
  marketType: MarketType;
  streamType?: 'aggTrade' | 'trade';
}

export interface CvdAggregatorConfig {
  id: string;
  displayName: string;
  streams: CvdStreamConfig[];
  alertsEnabled?: boolean;
}

export interface WebSocketAggTradePayload {
  stream: string;
  data: {
    e: string;
    E: number;
    s: string;
    a: number;
    p: string;
    q: string;
    f: number;
    l: number;
    T: number;
    m: boolean;
    M: boolean;
  };
}

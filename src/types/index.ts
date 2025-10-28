/**
 * コアドメイン型定義
 * Binance向けデータ収集で扱うエンティティを表現する
 */

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

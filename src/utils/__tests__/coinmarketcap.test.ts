import fs from 'fs';
import os from 'os';
import path from 'path';
import { DEFAULT_STABLE_SYMBOLS, loadCoinMarketCapTopEntries } from '../coinmarketcap';

const createTempCsv = (rows: string[]): string => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'cmc-'));
  const filePath = path.join(dir, 'top.csv');
  fs.writeFileSync(filePath, rows.join('\n'), 'utf8');
  return filePath;
};

describe('loadCoinMarketCapTopEntries', () => {
  it('filters BTC and stable coins and respects the limit', () => {
    const csvRows = [
      '"rank","name","symbol"',
      '1,"Bitcoin","BTC"',
      '2,"Ethereum","ETH"',
      '3,"Tether","USDT"',
      '4,"Solana","SOL"',
    ];
    const csvPath = createTempCsv(csvRows);

    const entries = loadCoinMarketCapTopEntries(csvPath, { limit: 3 });

    expect(entries).toHaveLength(2);
    expect(entries.map((entry) => entry.symbol)).toEqual(['ETH', 'SOL']);

    fs.unlinkSync(csvPath);
    fs.rmdirSync(path.dirname(csvPath));
  });

  it('allows overriding stable symbol list', () => {
    const csvRows = [
      '"rank","name","symbol"',
      '1,"Custom Stable","CUS"',
      '2,"Project","AAA"',
    ];
    const csvPath = createTempCsv(csvRows);

    const customStables = new Set([...DEFAULT_STABLE_SYMBOLS, 'AAA']);
    const entries = loadCoinMarketCapTopEntries(csvPath, {
      stableSymbols: customStables,
    });

    expect(entries).toHaveLength(1);
    expect(entries[0]?.symbol).toBe('CUS');
    expect(entries.some((entry) => entry.symbol === 'AAA')).toBe(false);

    fs.unlinkSync(csvPath);
    fs.rmdirSync(path.dirname(csvPath));
  });
});

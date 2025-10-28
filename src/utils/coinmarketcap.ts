import fs from 'fs';
import path from 'path';

export interface CoinMarketCapEntry {
  rank: number;
  name: string;
  symbol: string;
}

export const DEFAULT_STABLE_SYMBOLS = new Set([
  'USDT',
  'USDC',
  'FDUSD',
  'TUSD',
  'DAI',
  'BUSD',
  'USDD',
  'USDP',
  'GUSD',
  'LUSD',
  'USDX',
  'EURT',
  'PYUSD',
]);

interface LoadOptions {
  limit?: number;
  excludeSymbols?: Set<string>;
  stableSymbols?: Set<string>;
}

export function loadCoinMarketCapTopEntries(
  filePath: string,
  options: LoadOptions = {}
): CoinMarketCapEntry[] {
  const resolvedPath = path.resolve(filePath);
  const content = fs.readFileSync(resolvedPath, 'utf8');

  const lines = content
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter((line) => line.length > 0);

  if (lines.length <= 1) {
    return [];
  }

  const excludeSet = new Set(
    [
      'BTC',
      ...(options.excludeSymbols ? Array.from(options.excludeSymbols) : []),
    ].map((symbol) => symbol.toUpperCase())
  );
  const stableSet = new Set(
    Array.from(options.stableSymbols ?? DEFAULT_STABLE_SYMBOLS).map((symbol) =>
      symbol.toUpperCase()
    )
  );

  const limit = options.limit ?? Number.POSITIVE_INFINITY;
  const entries: CoinMarketCapEntry[] = [];

  for (let i = 1; i < lines.length; i += 1) {
    const line = lines[i];
    if (!line) {
      continue;
    }

    const fields = parseCsvLine(line);
    if (!fields || fields.length < 3) {
      continue;
    }

    const [rankField = '', nameField = '', symbolField = ''] = fields;

    if (!rankField || !symbolField) {
      continue;
    }

    const rank = Number(rankField);
    const name = stripQuotes(nameField);
    const symbol = stripQuotes(symbolField).toUpperCase();

    if (Number.isNaN(rank) || !symbol) {
      continue;
    }

    if (excludeSet.has(symbol) || stableSet.has(symbol)) {
      continue;
    }

    entries.push({ rank, name, symbol });

    if (entries.length >= limit) {
      break;
    }
  }

  return entries;
}

function parseCsvLine(line: string): string[] | null {
  const result: string[] = [];
  let current = '';
  let inQuotes = false;

  for (let i = 0; i < line.length; i += 1) {
    const char = line[i];

    if (char === '"') {
      if (inQuotes && line[i + 1] === '"') {
        current += '"';
        i += 1;
      } else {
        inQuotes = !inQuotes;
      }
    } else if (char === ',' && !inQuotes) {
      result.push(current);
      current = '';
    } else {
      current += char;
    }
  }

  result.push(current);
  return result;
}

function stripQuotes(value: string): string {
  return value.replace(/^"+|"+$/g, '');
}

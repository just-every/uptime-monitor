export interface Sql { sql: string; params?: any[] }
export const RPC_LIMIT: number;
export function wireSql(statement: Sql): { sql: string; params: any[] };
export function wireBatch(statements: Sql[]): Sql[];
export function rpcBytes(statements: Sql[]): number;
export interface RpcSize { utf8: number; units: number; hasWide: boolean }
export function rpcSize(statements: Sql[]): RpcSize;
export function rpcSizeBytes(size: RpcSize): number;
export function wireDatabase<T extends { prepare(sql: string): any; batch(statements: any[]): Promise<any> }>(database: T): T;

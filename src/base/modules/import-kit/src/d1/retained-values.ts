import { guard, guardPredicate } from './guards.ts';
import type { D1Sql } from './types.ts';
/** A structural placeholder descriptor, supplied only for safe normalized values.
 * SQL is assembled from literal parts; no SQL lexer or question-mark rewriting
 * guesses whether text is a quoted literal, comment or numbered parameter. */
export function retainedSql(parts: string[], params: unknown[], retain: number[]): D1Sql {
  if (parts.length !== params.length + 1 || retain.some(i=>!Number.isSafeInteger(i)||i<0||i>=params.length||typeof params[i]!=='string')) throw new Error('IMPORT_RETAINED_VALUE_DESCRIPTOR');
  return { sql: parts.join('?'), params, retainedValues: { parts, indexes: retain } };
}
export function replaceRetained(statement: D1Sql, reference: (value: string) => D1Sql): D1Sql {
  const scalar = guardPredicate(statement);
  if (scalar) {
    const replaced = guard(replaceRetained(scalar, reference));
    replaced.expectedSnapshots = statement.expectedSnapshots;
    replaced.expectedChanges = statement.expectedChanges;
    replaced.finalChecks = statement.finalChecks;
    replaced.resultKey = statement.resultKey;
    return replaced;
  }
  const descriptor = statement.retainedValues;
  if (!descriptor) return statement;
  const { parts, indexes } = descriptor, params = statement.params ?? [];
  if (parts.length!==params.length+1 || parts.join('?')!==statement.sql || indexes.some(i=>!Number.isSafeInteger(i)||i<0||i>=params.length||typeof params[i]!=='string')) throw new Error('IMPORT_RETAINED_VALUE_DESCRIPTOR');
  const result: D1Sql = {...statement,sql:parts[0],params:[],retainedValues:undefined};
  for(let i=0;i<params.length;i++){
    const value=indexes.includes(i)?reference(params[i]):{sql:'?',params:[params[i]]};
    result.sql+=value.sql+parts[i+1];result.params!.push(...value.params??[]);
  }
  return result;
}
/** Internal tagged SQL for already validated normalized JSON and identifiers.
 * The template's literal boundaries, rather than a lexer, locate bindings. */
export function retainedTemplate(parts:TemplateStringsArray,...params:unknown[]):D1Sql{
  return retainedSql([...parts],params,params.flatMap((value,index)=>typeof value==='string'?[index]:[]));
}

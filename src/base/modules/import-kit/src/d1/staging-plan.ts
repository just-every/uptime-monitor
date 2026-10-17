import type { D1PlanCodec } from './types.ts';
import { expandCompressedEnvelope } from './compressed-plan.ts';
import { canonicalJson } from '../provenance/identity.ts';
import { SNAPSHOT_BYTES } from './sets.ts';

/** Preserve existing public inline custody for small pages. The private stage
 * still registers its complete privacy index before receiving these bytes. */
export function stagingPlan(body: string, codec?: D1PlanCodec): { body: string; inline: number } {
  const envelope = JSON.parse(body);
  const inline = Number((envelope.expandedSize ?? envelope.size) <= SNAPSHOT_BYTES);
  return { body: inline && envelope.d1Plan !== 3 ? canonicalJson(expandCompressedEnvelope(body, envelope, codec)) : body, inline };
}

/** Only the bounded inline page is joined inside the publication transaction. */
export const publishedPlan = (page = 'p') => `CASE WHEN ${page}.inline_plan=1 THEN (SELECT group_concat(body,'') FROM (SELECT c.body FROM ik_d1_staging_chunks c WHERE c.stage_id=${page}.stage_id AND c.page_ordinal=${page}.ordinal ORDER BY c.ordinal)) ELSE ${page}.plan_json END`;

import { liveStreamOrigin, needsStreamOrigin } from './stream-origin.ts';
import type { ImportAction } from '../types.ts';
import { guard, sql } from './database.ts';
/** Ordinary child-job entry points obey the same sealed parent direction and
 * page ordering as the stream API; an ID cannot bypass a parent's reversal. */
export function streamBoundary(id: string, action: ImportAction) {
  const origin = guard(sql(`NOT EXISTS(SELECT 1 FROM ik_jobs child WHERE child.id=? AND ${needsStreamOrigin('child.id')} AND NOT ${liveStreamOrigin('child.id')})`,id));
  if (!['apply','undo','rollback'].includes(action)) return [origin];
  const forward=action==='apply';
  return [origin,guard(sql(`NOT EXISTS(SELECT 1 FROM ik_d1_stream_pages p JOIN ik_d1_streams s ON s.id=p.stream_id WHERE p.job_id=? AND (s.state ${forward?'=':'<>'} 'undoing' OR EXISTS(SELECT 1 FROM ik_d1_stream_pages peer JOIN ik_jobs j ON j.id=peer.job_id WHERE peer.stream_id=p.stream_id AND peer.ordinal ${forward?'<':'>'} p.ordinal AND j.state ${forward?"<>'completed'":"NOT IN ('undone','rolled_back')"})))`,id))];
}

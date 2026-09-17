// Exercise the real view filters with backlog, merged history and saved moves.
const fs=require('node:fs'), vm=require('node:vm'), assert=require('node:assert/strict');
const c={window:{}};vm.runInNewContext(fs.readFileSync('sprint_launch_data.js','utf8'),c);
const d=c.window.LAUNCH_DATA, html=fs.readFileSync('sprint_tracker_launch.html','utf8');
const records=[...d.tickets,...d.backlog];const state={};
const ctx={all:()=>records, STATE:state, TODAY:'2026-09-16', st:id=>state[id]||{status:records.find(t=>t.id===id)?.status||'todo'}, sp:t=>state[t.id]?.sprint||t.sprint};
for(const name of ['counted','inSprints','isOpen','isOver','laneStats']) {
 const line=html.split('\n').find(l=>l.trim().startsWith('function '+name+'('));assert(line,name);vm.runInNewContext(line,ctx);
}
const start=html.indexOf('  function fstats('),end=html.indexOf('  function ring(',start);vm.runInNewContext(html.slice(start,end),ctx);
const allIds=records.map(t=>t.id);
assert.equal(ctx.fstats({tickets:allIds}).ts.length,d.tickets.length);
assert.equal(ctx.fstats({tickets:d.backlog.map(t=>t.id)}).ts.length,0);
assert.equal(ctx.laneStats({tickets:allIds,scope:'deferred'}).ts.length,0);
state['N1-AS-11']={status:'todo',sprint:'BL'};
assert.equal(ctx.fstats({tickets:['N1-AS-11']}).ts.length,0,'Saved move to backlog must apply');
delete state['N1-AS-11'];
state['N1-AS-04']={status:'todo',sprint:'N1'};
assert.equal(ctx.fstats({tickets:['N1-AS-04']}).ts.length,0,'Merged history cannot inflate work');
for(const id of ['discovery','wins','analytics']) assert.equal(d.features.find(f=>f.id===id).scope,'deferred');
for(const f of d.features) for(const id of f.tickets) assert(records.some(t=>t.id===id),id);
for(const tr of d.tracks) for(const lane of tr.lanes) {
 const stats=ctx.laneStats(lane);assert(stats.ts.every(t=>t.sprint!=='BL'));
}
assert(!html.includes("role:'Design to 10 Sep, then PM'"));
assert(!html.includes('Saad\\u2019s 10 Sep pack supersedes this.'));
assert(html.includes('Current gates and release milestones'));
assert.equal(d.tab_review.tabs.length,13);
console.log('PASS: shared view filters exclude backlog/merged work, respect saved moves, and distinguish deferred features across 13 tabs.');
const models=d.tracks.find(t=>t.id==='models');
for(const id of ['postnlp','commentnlp']) {
  const lane=models.lanes.find(l=>l.id===id), stats=ctx.laneStats(lane);
  assert.equal(lane.delivery_state,'Built and running');
  assert.equal(stats.pct,100,`${id}: shipped foundation must not be mixed with open follow-ups`);
  assert(stats.ts.every(t=>t.sprint==='P0'));
}
assert(models.lanes.find(l=>l.id==='commentnlp').tickets.includes('P0-FH-08'));
assert(models.lanes.find(l=>l.id==='commentnlp-followups').tickets.includes('N2-FH-09'));
console.log('PASS: completed post/comment NLP foundations separate from integration and quality follow-ups.');
const resolution={D:d};
const resolveStart=html.indexOf('  function resolveState('),resolveEnd=html.indexOf('  function st(',resolveStart);
vm.runInNewContext(html.slice(resolveStart,resolveEnd),resolution);
vm.runInNewContext(html.split('\n').find(l=>l.trim().startsWith('function supersededSprintEdit(')),resolution);
const base={status:'done',done_at:'2026-09-14',status_updated_at:'2026-09-16T12:00:00Z',checklist_revised_at:'2026-09-16T12:00:00Z'};
const old={status:'todo',updated_at:'2026-09-15T12:00:00Z',checks:{0:true}};
assert.equal(resolution.resolveState(base,old).status,'done');
assert.equal(resolution.resolveState(base,{status:'todo'}).status,'done');
assert.equal(Object.keys(resolution.resolveState(base,old).checks).length,0);
assert.equal(old.checks[0],true,'Preserve the saved record');
const fresh={status:'inprog',updated_at:'2026-09-17T12:00:00Z',checks:{0:true}};
assert.equal(resolution.resolveState(base,fresh).status,'inprog');
assert.equal(resolution.resolveState(base,fresh).checks[0],true);
const override=d.reconciliation.superseded_sprint_overrides.N1;
assert(resolution.supersededSprintEdit('N1',override));
assert(!resolution.supersededSprintEdit('N1',{...override,at:'2026-09-17T12:00:00Z'}));
assert(!resolution.supersededSprintEdit('N2',override));
assert.equal(d.tickets.filter(t=>t.status==='done').length,96);
console.log('PASS: verified status wins over stale cache; newer updates apply; revised checklists require fresh checks; only the exact superseded calendar override is ignored.');

// Stale saved moves/custom records must not resurrect removed assignments.
const allContext={D:d,BACKLOG:d.backlog,CUSTOM:[{id:'N1-FZ-13',sprint:'N1'},{id:'N1-AS-17',sprint:'N1'}],applyEdits:t=>t};
vm.runInNewContext(html.split('\n').find(l=>l.trim().startsWith('function all(')),allContext);
assert(!allContext.all().some(t=>d.scope_revision.removed_ids.includes(t.id)));
for(const tr of d.tracks) for(const lane of tr.lanes) for(const id of lane.tickets) assert(!d.scope_revision.removed_ids.includes(id));
assert(!html.includes('Submission is Fri 2 Oct'));
assert(!JSON.stringify(d.tracks.find(t=>t.id==='qa').spec).includes('72h'));
console.log('PASS: removed work stays out of every feature/lane and cannot return through saved custom records.');

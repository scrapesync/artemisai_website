// Structural checks for the fixed December plan; passing is not a capacity sign-off.
const fs = require('node:fs');
const vm = require('node:vm');
const context = { window: {} };
vm.runInNewContext(fs.readFileSync('sprint_launch_data.js', 'utf8'), context);
const d = context.window.LAUNCH_DATA;
const assert = require('node:assert/strict');
assert.equal(d.launch.public, '2026-12-15');
assert.equal(d.launch.hard_deadline, '2026-12-15');
assert(d.launch.beta < d.launch.public);
const all = [...d.tickets, ...d.backlog];
const index = new Map(all.map(t => [t.id, t]));
assert.equal(index.size, all.length, 'Duplicate ticket IDs');
const sprints = new Map(d.sprints.map(s => [s.id, s]));
const isTicketId = id => /^(P0|N\d|LW|BL)-[A-Z]+-\d+$/.test(id);
for (const s of d.sprints) {
  assert(s.start <= s.gate_date && s.gate_date <= s.end, `${s.id}: gate outside sprint`);
}
for (const t of all) {
  for (const dep of (t.depends_on || []).filter(isTicketId)) assert(index.has(dep), `${t.id}: missing ${dep}`);
  if (t.sprint === 'BL' || t.sprint === 'P0') continue;
  const s = sprints.get(t.sprint);
  assert(s && s.start <= t.due && t.due <= s.end, `${t.id}: outside sprint`);
  assert(t.due <= d.launch.hard_deadline, `${t.id}: beyond hard limit`);
  assert(t.estimated_days > 0, `${t.id}: missing effort estimate`);
  assert(['Asad', 'Muteeb', 'Faheem', 'Saad', 'Alex', 'Jill', 'Lewis', 'Filza'].includes(t.assignee), `${t.id}: extra staff`);
  for (const dep of (t.depends_on || []).filter(isTicketId)) {
    const q = index.get(dep);
    assert(q.sprint !== 'BL', `${t.id}: depends on parked ${dep}`);
    assert(t.status === 'done' || !q.due || q.due <= t.due, `${t.id}: before ${dep}`);
  }
}
const seen = new Set(), visiting = new Set();
function visit(id) {
  assert(!visiting.has(id), `Dependency cycle at ${id}`);
  if (seen.has(id)) return;
  visiting.add(id);
  for (const dep of (index.get(id).depends_on || []).filter(isTicketId)) visit(dep);
  visiting.delete(id); seen.add(id);
}
all.forEach(t => visit(t.id));
for (const tr of d.tracks) for (const lane of tr.lanes) {
  for (const id of lane.tickets) assert(index.has(id), `Missing lane ticket ${id}`);
}
// Protect the full agreed plan from another blanket deletion.
const receipt=JSON.parse(fs.readFileSync('docs/scope-restoration.json','utf8'));
assert.deepEqual([...index.keys()].sort(),receipt.expected_ids);
const removed=new Set(d.scope_revision.removed_ids);
assert.equal(all.length+removed.size,receipt.before_total);
assert.deepEqual([...removed].sort(),['N1-AS-17','N1-FZ-13']);
for(const id of removed) assert(!index.has(id));
assert.equal(d.backlog.length,270);
assert.equal(d.tickets.filter(t=>t.status!=='done'&&t.sprint!=='P0').length,676);
for(const id of ['N2-AS-01','N2-AS-06','N1-MT-09','N2-FH-06','N2-FH-14','N2-MT-16','N2-SD-19','N3-SD-14','N2-AX-13','N2-AX-16','N6-AX-08','N3-FZ-13','N4-FZ-12','N1-JL-06']) assert(index.has(id),`Agreed work lost: ${id}`);
assert.equal(index.get('LW-AS-04').due,d.launch.public);
assert.equal(index.get('N2-SD-07').sprint,'N4');
assert(index.get('N2-SD-07').depends_on.includes('N2-SD-04'));
assert(index.get('N2-SD-04').depends_on.includes('N1-MT-11'));
const report=JSON.parse(require('node:child_process').execFileSync(process.execPath,['scripts/report_launch_capacity.js'],{encoding:'utf8'}));
assert.deepEqual(JSON.parse(JSON.stringify(d.capacity_review)),report);
assert(report.total_effort_days>0);
console.log(`PASS: ${all.length} retained, ${removed.size} removed; dates, dependency graph, disposition coverage and capacity agree.`);

// Saad's landing milestone dates are distinct from product build completion.
const handoff = new Map(d.design_handoff.milestones.map(m=>[m.id,m]));
assert.equal(handoff.get('landing-handoff').date,'2026-09-19');
assert.equal(handoff.get('app-home').date,'2026-09-23');
for (const t of d.tickets) for (const id of (t.design_milestone_dependencies||[])) {
  assert(handoff.has(id)); assert(handoff.get(id).date <= t.due);
  if(t.not_before) assert(handoff.get(id).date <= t.not_before);
}
for (const id of ['N2-AS-04','N2-AS-06']) {
  assert(index.get(id).depends_on.includes('N2-SD-02'));
  assert(index.get('N2-SD-02').due < index.get(id).due);
}
assert(!index.get('N1-SD-20').depends_on.includes('N1-SD-09'));
assert(!index.get('N1-AS-11').depends_on.includes('N1-SD-06'));
console.log('PASS: landing/product handoffs separated; acceptance precedes implementation completion.');

assert.equal(sprints.get("N1").end,"2026-09-25");
assert.equal(sprints.get("LW").end,"2026-12-15");
for(const t of d.tickets.filter(t=>t.sprint==="N1"&&t.status!=="done"&&!d.schedule_revision.urgent_exceptions.includes(t.id))) assert.equal(t.due,"2026-09-25",t.id);

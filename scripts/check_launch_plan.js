// Structural checks for the fixed December plan; passing is not a capacity sign-off.
const fs = require('node:fs');
const vm = require('node:vm');
const context = { window: {} };
vm.runInNewContext(fs.readFileSync('sprint_launch_data.js', 'utf8'), context);
const d = context.window.LAUNCH_DATA;
const assert = require('node:assert/strict');
assert.equal(d.launch.public, '2026-12-15');
assert.equal(d.launch.hard_deadline, '2026-12-20');
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
    assert(!q.due || q.due <= t.due, `${t.id}: before ${dep}`);
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
for (const id of ['N2-SD-07', 'N2-FZ-07', 'N2-AS-09', 'N2-SD-04']) {
  assert.equal(index.get(id).due, '2026-10-02', `${id}: Meta anchor moved`);
}
assert.equal(index.get('LW-AS-04').due, d.launch.public);
assert.equal(index.get('N6-AX-07').due, '2026-12-11');
for (const id of ['N3-JL-18', 'N3-JL-19', 'LW-JL-05']) assert.equal(index.get(id).sprint, 'BL');
console.log(`PASS: ${d.tickets.length} scheduled records, ${d.backlog.length} backlog records; dates, dependencies, gates and existing owners checked. Capacity is a separate unresolved planning check.`);
// Audit invariants: consolidation must not silently discard scope or reviewer effort.
const audit = JSON.parse(fs.readFileSync('docs/ticket-audit.json', 'utf8'));
assert.equal(audit.tickets.length, 906);
assert.equal(all.length, 906);
assert.equal(new Set(audit.tickets.map(t => t.id)).size, 906);
for (const t of all) {
  assert(audit.tickets.some(q => q.id === t.id), `Unreviewed record ${t.id}`);
  if (t.effort_allocations) {
    assert.equal(Object.values(t.effort_allocations).reduce((a,b)=>a+b,0), t.estimated_days, `${t.id}: reviewer effort lost`);
    for (const name of Object.keys(t.effort_allocations)) assert(d.capacity_review.owners.some(o=>o.owner===name));
  }
  if (t.superseded_by) {
    const target = index.get(t.superseded_by);
    assert(target && target.sprint !== 'BL');
    assert(target.absorbed_requirements.some(q=>q.id===t.id && q.acceptance===t.acceptance), `${t.id}: merged acceptance lost`);
    assert(!d.tickets.some(q=>(q.depends_on||[]).includes(t.id)), `${t.id}: active dependency still uses archive`);
  }
}
const report = JSON.parse(require('node:child_process').execFileSync(process.execPath, ['scripts/report_launch_capacity.js'], {encoding:'utf8'}));
assert.deepEqual(JSON.parse(JSON.stringify(d.capacity_review)), report, 'Displayed capacity differs from calculated effort');
assert.equal(report.total_effort_days, 609.25);
assert.equal(report.total_capacity_days, 312);
console.log('PASS: 906-record conservation, merge acceptance, reviewer effort and displayed capacity reconciliation.');

for (const id of ["N1-SD-13","N3-JL-11","N3-SD-16","N2-AX-14","N5-AX-08"]) assert.equal(index.get(id).sprint,"BL");
assert.equal(d.tickets.filter(t=>t.sprint!=="P0").length,545);

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

// Conservative baseline: inherited estimates and focus rates, no new hires/overtime.
const fs = require('node:fs');
const vm = require('node:vm');
const context = { window: {} };
vm.runInNewContext(fs.readFileSync('sprint_launch_data.js', 'utf8'), context);
const d = context.window.LAUNCH_DATA;
const start = '2026-09-17';
const rates = {Asad: .6, Muteeb: .7, Faheem: .7, Saad: .7, Alex: .4, Jill: .6, Lewis: .6, Filza: .5};
function weekdays(end) {
  let n = 0;
  for (let date = new Date(start+'T00:00:00Z'); date.toISOString().slice(0,10) <= end; date.setUTCDate(date.getUTCDate()+1)) {
    if (date.getUTCDay() > 0 && date.getUTCDay() < 6) n++;
  }
  return n;
}
const round = n => Math.round(n*100)/100;
const owners = Object.entries(rates).map(([owner, rate]) => {
  const tickets = d.tickets.filter(t => t.sprint !== 'P0' && t.status !== 'done' && t.assignee === owner);
  const effort = d.tickets.filter(t => t.sprint !== 'P0' && t.status !== 'done').reduce((n,t) => n + (t.effort_allocations ? (t.effort_allocations[owner] || 0) : (t.assignee === owner ? t.estimated_days : 0)), 0);
  const capacity = weekdays(d.launch.public)*rate;
  return {owner, tickets: tickets.length, focus_rate: rate, effort_days: round(effort), capacity_days: round(capacity), gap_days: round(Math.max(0, effort-capacity)), capacity_at_hard_limit: round(weekdays(d.launch.hard_deadline)*rate)};
});
console.log(JSON.stringify({as_of:start, target:d.launch.public, hard_limit:d.launch.hard_deadline,
  basis:'Audited scope with inherited estimates and focus rates, including allocated reviewer effort; excludes deferred/merged work and recorded done/Phase 0. Shared-board status snapshot 2026-09-16T12:15:43.591Z applied; remaining effort, leave and availability still require owner confirmation. No new hires, overtime or assumed external counsel. Weekends add no capacity.',
  owners, total_effort_days:round(owners.reduce((n,p)=>n+p.effort_days,0)), total_capacity_days:round(owners.reduce((n,p)=>n+p.capacity_days,0))},null,2));

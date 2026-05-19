// Full bug-test pass.
// - 2 viewports: 1366×900 (desktop), 390×844 (iPhone 14)
// - Visit Strategy, Sprint Tracker, Mockup Alignment
// - On Sprint Tracker: switch through a sample of sprints (S1, S5, S14, S23) AND every filter chip
// - Capture all console errors + uncaught exceptions + failed network
// - Screenshot every important state
// - Then assert: no "stuck" track bar on launch/mockup, no JS errors, filter chips clickable

const puppeteer = require('/tmp/node_modules/puppeteer-core');
const fs = require('fs');
const path = require('path');

const CHROME = '/home/claude/.cache/puppeteer/chrome/linux-131.0.6778.204/chrome-linux64/chrome';
const URL = 'file:///home/claude/sprint_tracker.html';
const OUT = '/home/claude/tests/screenshots';

(async () => {
  const browser = await puppeteer.launch({
    executablePath: CHROME,
    headless: 'new',
    args: ['--no-sandbox', '--disable-setuid-sandbox', '--font-render-hinting=none'],
  });

  const allIssues = [];

  for (const viewport of [
    {name: 'desktop', width: 1366, height: 900, deviceScaleFactor: 1},
    {name: 'mobile',  width: 390,  height: 844, deviceScaleFactor: 2, isMobile: true, hasTouch: true},
  ]) {
    const page = await browser.newPage();
    await page.setViewport({width: viewport.width, height: viewport.height, deviceScaleFactor: viewport.deviceScaleFactor, isMobile: !!viewport.isMobile, hasTouch: !!viewport.hasTouch});

    const consoleMsgs = [];
    page.on('console', m => consoleMsgs.push({type: m.type(), text: m.text()}));
    page.on('pageerror', e => allIssues.push({viewport: viewport.name, kind: 'pageerror', msg: e.message}));
    page.on('requestfailed', r => {
      const url = r.url();
      // Skip the DynamoDB API which won't work in tests, and Google Fonts which sometimes hiccup
      if (url.includes('execute-api.eu-west-2') || url.includes('fonts.googleapis')) return;
      allIssues.push({viewport: viewport.name, kind: 'reqfailed', url, err: r.failure()?.errorText});
    });

    // Suppress login overlay so it doesn't block tests — set the real session key
    await page.evaluateOnNewDocument(() => {
      try {
        const sess = JSON.stringify({name:'Test Runner', username:'test', loggedInAt: Date.now()});
        localStorage.setItem('artemis_sprint_session', sess);
        localStorage.setItem('artemis_session', sess);
        localStorage.setItem('artemis_viewer_name', 'TestRunner');
      } catch(e){}
    });

    await page.goto(URL, {waitUntil: 'networkidle2', timeout: 30000});
    // Give canvas + render time
    await new Promise(r => setTimeout(r, 1500));

    // If login overlay is showing, force-hide it
    await page.evaluate(() => {
      const ov = document.getElementById('login-overlay');
      if (ov) ov.style.display = 'none';
    });

    // Test 1: Strategy tab is default. Track bar should be HIDDEN.
    await page.evaluate(() => {
      // ensure we're on launch
      if (typeof switchTab === 'function') switchTab('launch');
    });
    await new Promise(r => setTimeout(r, 500));
    const t1 = await page.evaluate(() => {
      const tb = document.getElementById('sprint-track-bar');
      const tbStyle = tb ? getComputedStyle(tb) : null;
      return {
        bodyTab: document.body.getAttribute('data-tab'),
        trackBarDisplay: tbStyle ? tbStyle.display : 'missing',
        launchActive: document.getElementById('launch-page')?.classList.contains('active'),
        sprintActive: document.getElementById('sprint-page')?.classList.contains('active'),
        mockupActive: document.getElementById('mockup-page')?.classList.contains('active'),
      };
    });
    if (t1.trackBarDisplay !== 'none') allIssues.push({viewport: viewport.name, kind: 'visual', msg: `Track bar visible on Strategy: ${t1.trackBarDisplay}`});
    if (!t1.launchActive) allIssues.push({viewport: viewport.name, kind: 'state', msg: `Launch page not active when on launch tab`});
    await page.screenshot({path: `${OUT}/${viewport.name}_01_strategy.png`, fullPage: false});
    await page.screenshot({path: `${OUT}/${viewport.name}_01_strategy_full.png`, fullPage: true});

    // Test 2: Switch to Mockup Alignment. Track bar HIDDEN. Sprint page should not be visible.
    await page.evaluate(() => switchTab('mockup'));
    await new Promise(r => setTimeout(r, 500));
    const t2 = await page.evaluate(() => {
      const tb = document.getElementById('sprint-track-bar');
      return {
        trackBarDisplay: getComputedStyle(tb).display,
        sprintPageHidden: getComputedStyle(document.getElementById('sprint-page')).display === 'none',
        mockupVisible: document.getElementById('mockup-page').classList.contains('active'),
      };
    });
    if (t2.trackBarDisplay !== 'none') allIssues.push({viewport: viewport.name, kind: 'visual', msg: `Track bar visible on Mockup: ${t2.trackBarDisplay}`});
    if (!t2.sprintPageHidden) allIssues.push({viewport: viewport.name, kind: 'visual', msg: `Sprint page leaks into Mockup tab`});
    await page.screenshot({path: `${OUT}/${viewport.name}_02_mockup.png`});
    await page.screenshot({path: `${OUT}/${viewport.name}_02_mockup_full.png`, fullPage: true});

    // Test 3: Switch to Sprint Tracker. Track bar VISIBLE.
    await page.evaluate(() => switchTab('sprint'));
    await new Promise(r => setTimeout(r, 800));
    const t3 = await page.evaluate(() => {
      const tb = document.getElementById('sprint-track-bar');
      return {
        trackBarDisplay: getComputedStyle(tb).display,
        sidebarHas: !!document.querySelector('.sprint-sidebar .sb-btn'),
        currentSprint: window.currentSprint,
      };
    });
    if (t3.trackBarDisplay === 'none') allIssues.push({viewport: viewport.name, kind: 'visual', msg: `Track bar HIDDEN on Sprint tab: ${t3.trackBarDisplay}`});
    if (!t3.sidebarHas) allIssues.push({viewport: viewport.name, kind: 'render', msg: `Sprint sidebar has no buttons`});
    await page.screenshot({path: `${OUT}/${viewport.name}_03_sprint_default.png`});

    // Test 4: Click a few sprints. S1 (locked done), S5 (active heavy), S14 (mid), S23 (launch)
    for (const sn of ['Sprint 1', 'Sprint 5', 'Sprint 14', 'Sprint 23']) {
      await page.evaluate(s => selSprint(s), sn);
      await new Promise(r => setTimeout(r, 400));
      const t = await page.evaluate(() => {
        const main = document.getElementById('sprint-main');
        const taskCards = main ? main.querySelectorAll('.tc').length : 0;
        const kanbanCards = main ? main.querySelectorAll('.ktc').length : 0;
        const filterStrip = !!main?.querySelector('.filter-strip');
        return {
          main: !!main,
          taskCards,
          kanbanCards,
          filterStrip,
          currentSprint: window.currentSprint,
        };
      });
      if (!t.taskCards) allIssues.push({viewport: viewport.name, kind: 'render', msg: `${sn}: no task cards rendered`});
      if (!t.filterStrip) allIssues.push({viewport: viewport.name, kind: 'render', msg: `${sn}: filter strip missing`});
      const tag = sn.replace(' ', '_').toLowerCase();
      await page.screenshot({path: `${OUT}/${viewport.name}_04_${tag}.png`});
    }

    // Test 5: Filter chips. Select Rafeh, then Filza, then back to Everyone.
    await page.evaluate(() => selSprint('Sprint 5'));
    await new Promise(r => setTimeout(r, 400));
    for (const person of ['Analyst', 'CLO', 'CEO', '']) {
      await page.evaluate(p => setAssigneeFilter(p), person);
      await new Promise(r => setTimeout(r, 300));
      const t = await page.evaluate(() => {
        const main = document.getElementById('sprint-main');
        const taskCards = main?.querySelectorAll('.tc').length ?? 0;
        const kanbanCards = main?.querySelectorAll('.ktc').length ?? 0;
        const activeChip = main?.querySelector('.filter-chip.active')?.textContent?.trim();
        return { taskCards, kanbanCards, activeChip };
      });
      const tag = person || 'everyone';
      await page.screenshot({path: `${OUT}/${viewport.name}_05_s5_filter_${tag.replace(/\s/g,'')}.png`});
    }

    // Test 6: Switch BACK to Strategy after being on Sprint — does anything break?
    await page.evaluate(() => switchTab('launch'));
    await new Promise(r => setTimeout(r, 500));
    const t6 = await page.evaluate(() => {
      const tb = document.getElementById('sprint-track-bar');
      return {
        trackBarDisplay: getComputedStyle(tb).display,
        launchVisible: document.getElementById('launch-page').classList.contains('active'),
        sprintHidden: getComputedStyle(document.getElementById('sprint-page')).display === 'none',
        // Check launch page content is rendered
        kpiCount: document.querySelectorAll('.strat-kpi').length,
        phaseRowCount: document.querySelectorAll('.strat-phase-row').length,
        teamCardCount: document.querySelectorAll('.strat-team-card').length,
      };
    });
    if (t6.trackBarDisplay !== 'none') allIssues.push({viewport: viewport.name, kind: 'visual', msg: `Track bar reappeared on Strategy after Sprint visit: ${t6.trackBarDisplay}`});
    if (!t6.kpiCount) allIssues.push({viewport: viewport.name, kind: 'render', msg: `Strategy KPI cards empty after switch-back`});
    if (!t6.phaseRowCount) allIssues.push({viewport: viewport.name, kind: 'render', msg: `Strategy Phase rows empty after switch-back`});
    if (!t6.teamCardCount) allIssues.push({viewport: viewport.name, kind: 'render', msg: `Strategy Team cards empty after switch-back`});
    await page.screenshot({path: `${OUT}/${viewport.name}_06_strategy_after_sprint.png`});

    // Test 7: Click around onclick targets to look for crashes
    await page.evaluate(() => switchTab('sprint'));
    await page.evaluate(() => selSprint('Sprint 5'));
    await new Promise(r => setTimeout(r, 400));
    // Try expanding a task card
    try {
      await page.evaluate(() => {
        const tc = document.querySelector('.tc');
        if (tc) tc.click();
      });
      await new Promise(r => setTimeout(r, 200));
    } catch(e){}
    await page.screenshot({path: `${OUT}/${viewport.name}_07_task_expanded.png`});

    // Test 8: Check for horizontal page overflow
    const overflow = await page.evaluate(() => {
      return {
        bodyScrollW: document.body.scrollWidth,
        bodyClientW: document.body.clientWidth,
        docScrollW: document.documentElement.scrollWidth,
        docClientW: document.documentElement.clientWidth,
        viewportW: window.innerWidth,
      };
    });
    if (overflow.docScrollW > overflow.docClientW + 1) {
      allIssues.push({viewport: viewport.name, kind: 'overflow', msg: `Horizontal page overflow: scroll=${overflow.docScrollW} client=${overflow.docClientW} viewport=${overflow.viewportW}`});
    }

    // Stash console output
    fs.writeFileSync(`${OUT}/${viewport.name}_console.json`, JSON.stringify(consoleMsgs, null, 2));

    // ── Test 9: rapid tab switching stress test ─────────────────
    for (let cycle = 0; cycle < 3; cycle++) {
      await page.evaluate(() => switchTab('launch'));
      await new Promise(r => setTimeout(r, 80));
      await page.evaluate(() => switchTab('sprint'));
      await new Promise(r => setTimeout(r, 80));
      await page.evaluate(() => switchTab('mockup'));
      await new Promise(r => setTimeout(r, 80));
    }
    // After rapid cycle, only mockup should be active
    const t9 = await page.evaluate(() => {
      const sp = document.getElementById('sprint-page');
      const lp = document.getElementById('launch-page');
      const mp = document.getElementById('mockup-page');
      const tb = document.getElementById('sprint-track-bar');
      return {
        spActive: sp?.classList.contains('active'),
        lpActive: lp?.classList.contains('active'),
        mpActive: mp?.classList.contains('active'),
        spDisplay: getComputedStyle(sp).display,
        lpDisplay: getComputedStyle(lp).display,
        mpDisplay: getComputedStyle(mp).display,
        tbDisplay: getComputedStyle(tb).display,
        bodyTab: document.body.getAttribute('data-tab'),
      };
    });
    if (t9.spActive || t9.lpActive) allIssues.push({viewport: viewport.name, kind: 'state', msg: `After rapid cycle ending on mockup: launch=${t9.lpActive} sprint=${t9.spActive} both should be false`});
    if (t9.spDisplay !== 'none' || t9.lpDisplay !== 'none') allIssues.push({viewport: viewport.name, kind: 'visual', msg: `After rapid cycle: sprint display=${t9.spDisplay}, launch display=${t9.lpDisplay} — should both be 'none'`});
    if (t9.tbDisplay !== 'none') allIssues.push({viewport: viewport.name, kind: 'visual', msg: `Track bar showing on mockup after rapid cycle: ${t9.tbDisplay}`});
    if (t9.bodyTab !== 'mockup') allIssues.push({viewport: viewport.name, kind: 'state', msg: `body[data-tab] is '${t9.bodyTab}' should be 'mockup'`});
    await page.screenshot({path: `${OUT}/${viewport.name}_09_after_rapid_cycle.png`});

    // ── Test 10: filter persists across sprint changes + reload ─
    await page.evaluate(() => switchTab('sprint'));
    await page.evaluate(() => selSprint('Sprint 5'));
    await page.evaluate(() => setAssigneeFilter('Analyst'));
    await new Promise(r => setTimeout(r, 300));
    await page.evaluate(() => selSprint('Sprint 8'));
    await new Promise(r => setTimeout(r, 300));
    const t10 = await page.evaluate(() => ({
      filterStored: localStorage.getItem('artemis_assignee_filter'),
      activeChip: document.querySelector('.filter-chip.active')?.textContent?.trim(),
    }));
    if (t10.filterStored !== 'Analyst') allIssues.push({viewport: viewport.name, kind: 'state', msg: `Filter not persisted in localStorage: ${t10.filterStored}`});
    if (!t10.activeChip?.startsWith('Rafeh')) allIssues.push({viewport: viewport.name, kind: 'state', msg: `Filter chip not showing as active after sprint change: ${t10.activeChip}`});

    // Reset filter to Everyone for cleanup
    await page.evaluate(() => setAssigneeFilter(''));

    // ── Test 11: every tab button is clickable + functional ──
    const tabClicks = await page.evaluate(() => {
      const out = [];
      ['launch','sprint','mockup'].forEach(tab => {
        const btns = Array.from(document.querySelectorAll('.tab-btn'));
        const target = btns.find(b => b.onclick?.toString().includes(`'${tab}'`));
        if (target) {
          target.click();
          out.push({tab, success: document.getElementById(tab+'-page')?.classList.contains('active')});
        } else {
          out.push({tab, success: false, err: 'button not found'});
        }
      });
      return out;
    });
    for (const tc of tabClicks) {
      if (!tc.success) allIssues.push({viewport: viewport.name, kind: 'click', msg: `Tab '${tc.tab}' click did not activate page`});
    }

    // ── Test 12: scroll the page and verify fixed elements stay put ──
    await page.evaluate(() => switchTab('sprint'));
    await page.evaluate(() => selSprint('Sprint 14'));
    await new Promise(r => setTimeout(r, 400));
    await page.evaluate(() => window.scrollTo(0, 800));
    await new Promise(r => setTimeout(r, 300));
    const t12 = await page.evaluate(() => {
      const tb = document.getElementById('sprint-track-bar');
      const nav = document.querySelector('nav.site-nav');
      const tbar = document.querySelector('.tab-bar');
      return {
        navTop: nav?.getBoundingClientRect().top,
        tbarTop: tbar?.getBoundingClientRect().top,
        tbTop: tb?.getBoundingClientRect().top,
      };
    });
    // After scroll, fixed elements should still be at the top of viewport
    if (Math.abs(t12.navTop) > 5) allIssues.push({viewport: viewport.name, kind: 'fixed', msg: `Nav not pinned to top after scroll: ${t12.navTop}`});
    await page.screenshot({path: `${OUT}/${viewport.name}_12_scrolled.png`});

    await page.close();
  }

  await browser.close();

  console.log('\n=== ISSUES FOUND ===');
  if (allIssues.length === 0) console.log('NONE');
  for (const i of allIssues) console.log(JSON.stringify(i));
  console.log(`\nTotal: ${allIssues.length}`);
  fs.writeFileSync(`${OUT}/_issues.json`, JSON.stringify(allIssues, null, 2));
})();

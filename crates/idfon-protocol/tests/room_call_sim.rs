//! Group-call signaling simulator (idfon-home docs/group-live-chat.md, G1).
//!
//! Many members' `RoomCall` state machines run against a modeled network:
//! random latency (so reordering), loss, duplicates, partitions, crashes that
//! lose a member's state, restarts, simultaneous starts, cap races, and room
//! member lists that disagree. Each member's sends fan out to its own view of
//! the room, and a receiver drops controls from senders outside its view, as
//! the daemon does. After the scripted chaos the network heals and time runs
//! on; then invariants are judged:
//!
//! 1. Everyone in the call shares one `call_id`.
//! 2. Everyone in the call has everyone else in the call on its roster, and
//!    plays exactly its roster (when the call holds at most 12).
//! 3. Nobody lists a member who is not in the call (no ghosts), observers included.
//! 4. Nobody who wants in is left out, unless the call is full.
//! 5. At most 12 people are in the call.
//! 6. With auto-close, an empty room has no session anywhere.
//!
//! Randomized runs are seeded; a failure prints its seed, scenario and trace.
//! `ROOM_CALL_SIM_RUNS` sets how many random runs (default 2000);
//! `ROOM_CALL_SIM_SEED` replays one.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap};

use idfon_protocol::room_call::{
    Effect, LocalJoin, Refusal, RoomCall, RoomCallControl, MAX_CALL_MEMBERS,
};

const ROOM: &str = "r_sim";
/// Ticks every member gets, in ms.
const TICK_MS: u64 = 1_000;
/// How long the healed network runs before judging.
const SETTLE_MS: u64 = 90_000;

struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
    fn range(&mut self, lo: u64, hi: u64) -> u64 {
        lo + self.below(hi.saturating_sub(lo) + 1)
    }
    fn chance(&mut self, p: f64) -> bool {
        (self.next() % 1_000_000) as f64 / 1_000_000.0 < p
    }
}

#[derive(Clone, Debug)]
enum Act {
    Join,
    Leave,
    /// Lose all state; others notice the media transport drop after a delay.
    Crash,
    /// Come back with fresh state; re-join if it wanted in.
    Restart,
}

#[derive(Clone, Debug)]
struct Partition {
    from_ms: u64,
    until_ms: u64,
    /// Members on one side; messages across are dropped.
    side: BTreeSet<usize>,
}

#[derive(Clone, Debug)]
struct Scenario {
    name: String,
    members: usize,
    auto_close: bool,
    latency_ms: (u64, u64),
    loss: f64,
    duplicate: f64,
    partitions: Vec<Partition>,
    /// (time, member, action)
    script: Vec<(u64, usize, Act)>,
    /// Per member, the members its room lists (default: everyone).
    views: Option<Vec<BTreeSet<usize>>>,
    /// Delay before others notice a crashed member's media is gone.
    loss_detect_ms: u64,
    /// Per-member clock offset in ms (wall clocks disagree), and how far a
    /// restart may move it (a clock reset or adjustment).
    skew_ms: u64,
    restart_jump_ms: u64,
    /// How long the healed network runs before judging.
    settle_ms: u64,
}

impl Scenario {
    fn new(name: &str, members: usize) -> Self {
        Self {
            name: name.into(),
            members,
            auto_close: true,
            latency_ms: (10, 200),
            loss: 0.0,
            duplicate: 0.0,
            partitions: Vec::new(),
            script: Vec::new(),
            views: None,
            loss_detect_ms: 5_000,
            skew_ms: 0,
            restart_jump_ms: 0,
            settle_ms: SETTLE_MS,
        }
    }
}

struct Member {
    id: String,
    call: RoomCall,
    alive: bool,
    wants_in: bool,
    playing: BTreeSet<String>,
    view: BTreeSet<usize>,
    next_call: u64,
    /// This member's clock: sim time plus this (never below zero).
    clock_offset: i64,
}

#[derive(Debug)]
enum Event {
    Deliver { from: usize, to: usize, text: String },
    Act(usize, Act),
    Tick(usize),
    PeerLost { at: usize, peer: usize },
}

struct Sim<'a> {
    sc: &'a Scenario,
    rng: Rng,
    now: u64,
    seq: u64,
    queue: BinaryHeap<Reverse<(u64, u64, usize)>>,
    events: Vec<Option<Event>>,
    members: Vec<Member>,
    healed: bool,
    trace: Vec<String>,
}

fn name(i: usize) -> String {
    format!("m{i:02}")
}

impl<'a> Sim<'a> {
    fn new(sc: &'a Scenario, seed: u64) -> Self {
        let members = (0..sc.members)
            .map(|i| Member {
                id: name(i),
                call: RoomCall::new(ROOM, name(i), sc.auto_close),
                alive: true,
                wants_in: false,
                playing: BTreeSet::new(),
                view: sc
                    .views
                    .as_ref()
                    .map(|v| v[i].clone())
                    .unwrap_or_else(|| (0..sc.members).collect()),
                next_call: 0,
                clock_offset: 0,
            })
            .collect::<Vec<_>>();
        let mut sim = Self {
            sc,
            rng: Rng::new(seed),
            now: 0,
            seq: 0,
            queue: BinaryHeap::new(),
            events: Vec::new(),
            members,
            healed: false,
            trace: Vec::new(),
        };
        for i in 0..sc.members {
            let skew = sim.rng.below(2 * sc.skew_ms + 1) as i64 - sc.skew_ms as i64;
            // Start clocks far enough from zero that offsets stay positive.
            sim.members[i].clock_offset = 10_000_000 + skew;
        }
        for (at, who, act) in &sc.script {
            sim.schedule(*at, Event::Act(*who, act.clone()));
        }
        for i in 0..sc.members {
            let phase = sim.rng.below(TICK_MS);
            sim.schedule(phase, Event::Tick(i));
        }
        sim
    }

    fn schedule(&mut self, at: u64, event: Event) {
        self.seq += 1;
        self.events.push(Some(event));
        self.queue.push(Reverse((at, self.seq, self.events.len() - 1)));
    }

    /// Member `who`'s clock reading now.
    fn clock(&self, who: usize) -> u64 {
        (self.now as i64 + self.members[who].clock_offset).max(0) as u64
    }

    fn log(&mut self, line: String) {
        self.trace.push(format!("{:>7} {line}", self.now));
    }

    fn partitioned(&self, a: usize, b: usize) -> bool {
        !self.healed
            && self.sc.partitions.iter().any(|p| {
                (p.from_ms..p.until_ms).contains(&self.now) && (p.side.contains(&a) != p.side.contains(&b))
            })
    }

    fn send(&mut self, from: usize, to: usize, text: String) {
        if from == to {
            return;
        }
        if !self.healed && self.rng.chance(self.sc.loss) {
            self.log(format!("{} -> {} LOST {}", name(from), name(to), summary(&text)));
            return;
        }
        if self.partitioned(from, to) {
            self.log(format!("{} -> {} PARTITIONED {}", name(from), name(to), summary(&text)));
            return;
        }
        let copies = if !self.healed && self.rng.chance(self.sc.duplicate) { 2 } else { 1 };
        for _ in 0..copies {
            let latency = self.rng.range(self.sc.latency_ms.0, self.sc.latency_ms.1);
            self.schedule(self.now + latency, Event::Deliver { from, to, text: text.clone() });
        }
    }

    fn apply(&mut self, who: usize, effects: Vec<Effect>) {
        for effect in effects {
            match effect {
                Effect::SendRoom(text) => {
                    let view: Vec<usize> = self.members[who].view.iter().copied().collect();
                    for to in view {
                        self.send(who, to, text.clone());
                    }
                }
                Effect::SendDirect { peer, text } => {
                    if let Some(to) = self.members.iter().position(|m| m.id == peer) {
                        self.send(who, to, text);
                    }
                }
                Effect::Subscribe(member) => {
                    self.members[who].playing.insert(member.peer);
                }
                Effect::Unsubscribe(peer) => {
                    self.members[who].playing.remove(&peer);
                }
                Effect::Ended => self.log(format!("{} session ended", name(who))),
            }
        }
    }

    fn join(&mut self, who: usize) {
        let member = &mut self.members[who];
        member.next_call += 1;
        let call_id = format!("c{:06}-{}", self.rng.below(1_000_000), member.id);
        let local = LocalJoin {
            ticket: format!("ilv1:{}:{}", member.id, member.next_call),
            addr: None,
            audio_codec: Some("liquid".into()),
        };
        let now = (self.now as i64 + member.clock_offset).max(0) as u64;
        let effects = member.call.join(local, now, || call_id);
        self.apply(who, effects);
    }

    fn run(&mut self, until: u64) {
        while let Some(Reverse((at, _, index))) = self.queue.peek().copied() {
            if at > until {
                break;
            }
            self.queue.pop();
            self.now = at;
            let Some(event) = self.events[index].take() else { continue };
            match event {
                Event::Deliver { from, to, text } => {
                    if !self.members[to].alive {
                        continue;
                    }
                    // The daemon accepts a room control only from someone its room lists.
                    if !self.members[to].view.contains(&from) {
                        self.log(format!("{} drops {} from outside its room", name(to), name(from)));
                        continue;
                    }
                    let control = RoomCallControl::decode(&text).expect("valid control");
                    self.log(format!("{} -> {} {}", name(from), name(to), summary(&text)));
                    let sender = name(from);
                    let now = self.clock(to);
                    let effects = self.members[to].call.receive(&sender, &control, now);
                    self.apply(to, effects);
                }
                Event::Act(who, act) => {
                    self.log(format!("{} {:?}", name(who), act));
                    match act {
                        Act::Join => {
                            if !self.members[who].alive {
                                continue;
                            }
                            self.members[who].wants_in = true;
                            self.join(who);
                        }
                        Act::Leave => {
                            if !self.members[who].alive {
                                continue;
                            }
                            self.members[who].wants_in = false;
                            let now = self.clock(who);
                            let effects = self.members[who].call.leave(now);
                            self.apply(who, effects);
                        }
                        Act::Crash => {
                            if !self.members[who].alive {
                                continue;
                            }
                            self.members[who].alive = false;
                            self.members[who].playing.clear();
                            // Anyone playing it notices its media drop.
                            for at in 0..self.members.len() {
                                if self.members[at].playing.contains(&name(who)) {
                                    let when = self.now + self.sc.loss_detect_ms;
                                    self.schedule(when, Event::PeerLost { at, peer: who });
                                }
                            }
                        }
                        Act::Restart => {
                            if self.members[who].alive {
                                continue;
                            }
                            let jump = self.sc.restart_jump_ms;
                            let delta = self.rng.below(2 * jump + 1) as i64 - jump as i64;
                            let member = &mut self.members[who];
                            member.clock_offset += delta;
                            member.alive = true;
                            member.call = RoomCall::new(ROOM, name(who), self.sc.auto_close);
                            if member.wants_in {
                                self.join(who);
                            }
                        }
                    }
                }
                Event::Tick(who) => {
                    if self.members[who].alive {
                        let now = self.clock(who);
                        let effects = self.members[who].call.tick(now);
                        self.apply(who, effects);
                    }
                    self.schedule(self.now + TICK_MS, Event::Tick(who));
                }
                Event::PeerLost { at, peer } => {
                    if self.members[at].alive && !self.members[peer].alive {
                        self.log(format!("{} lost {}'s media", name(at), name(peer)));
                        let now = self.clock(at);
                        let effects = self.members[at].call.peer_lost(&name(peer), now);
                        self.apply(at, effects);
                    }
                }
            }
        }
        self.now = until;
    }

    /// Run the script, heal the network, settle, and judge.
    fn judge(&mut self) -> Result<(), String> {
        let end = self.sc.script.iter().map(|(t, _, _)| *t).max().unwrap_or(0)
            .max(self.sc.partitions.iter().map(|p| p.until_ms).max().unwrap_or(0));
        self.run(end + 1);
        self.healed = true;
        self.log("network healed".into());
        self.run(end + 1 + self.sc.settle_ms);

        let alive: Vec<usize> = (0..self.members.len()).filter(|&i| self.members[i].alive).collect();
        let joined: BTreeSet<String> = alive
            .iter()
            .filter(|&&i| self.members[i].call.joined.is_some())
            .map(|&i| name(i))
            .collect();
        // Two members hear each other only if each one's room lists the other.
        let mutual = |a: usize, b: usize| self.members[a].view.contains(&b) && self.members[b].view.contains(&a);
        let full_views = (0..self.members.len()).all(|a| (0..self.members.len()).all(|b| mutual(a, b)));
        let mut problems = Vec::new();
        if full_views && joined.len() > MAX_CALL_MEMBERS {
            problems.push(format!("{} people in the call (max {MAX_CALL_MEMBERS})", joined.len()));
        }
        // With mismatched rooms nobody sees everyone; each member's view is capped.
        for &i in &alive {
            if self.members[i].call.people() > MAX_CALL_MEMBERS {
                problems.push(format!("{} sees {} people in the call (max {MAX_CALL_MEMBERS})", name(i), self.members[i].call.people()));
            }
        }
        let call_ids: BTreeSet<Option<String>> = alive
            .iter()
            .filter(|&&i| self.members[i].call.joined.is_some())
            .map(|&i| self.members[i].call.call_id.clone())
            .collect();
        if full_views && call_ids.len() > 1 {
            problems.push(format!("split sessions among the joined: {call_ids:?}"));
        }
        for &i in &alive {
            let m = &self.members[i];
            let roster: BTreeSet<String> = m.call.members.keys().cloned().collect();
            let index = |id: &String| id[1..].parse::<usize>().unwrap();
            let reachable: BTreeSet<String> = joined.iter().filter(|j| mutual(i, index(j))).cloned().collect();
            let ghosts: Vec<&String> = roster.difference(&reachable).collect();
            if !ghosts.is_empty() {
                problems.push(format!("{} lists ghosts {ghosts:?} (in call: {joined:?})", m.id));
            }
            if m.call.joined.is_some() {
                let mut expected = reachable.clone();
                expected.remove(&m.id);
                // With mismatched rooms, a member reachable only through a different
                // session (glare across a one-way link) cannot converge; judge
                // full views strictly and mismatched ones for ghosts only.
                if full_views && joined.len() <= MAX_CALL_MEMBERS && roster != expected {
                    let missing: Vec<&String> = expected.difference(&roster).collect();
                    problems.push(format!("{} misses {missing:?}", m.id));
                }
                if m.playing != roster {
                    problems.push(format!("{} plays {:?} but lists {:?}", m.id, m.playing, roster));
                }
            } else {
                if !m.playing.is_empty() {
                    problems.push(format!("{} plays {:?} while out of the call", m.id, m.playing));
                }
                if full_views && m.wants_in && !(m.call.refused == Some(Refusal::Full) && joined.len() >= MAX_CALL_MEMBERS) {
                    problems.push(format!("{} wants in but is out (refused {:?})", m.id, m.call.refused));
                }
            }
            if reachable.is_empty() && self.sc.auto_close && m.call.call_id.is_some() {
                problems.push(format!("{} keeps session {:?} in an empty auto-close room", m.id, m.call.call_id));
            }
        }
        if problems.is_empty() { Ok(()) } else { Err(problems.join("\n")) }
    }
}

fn summary(text: &str) -> String {
    let control = RoomCallControl::decode(text).unwrap();
    format!(
        "{:?}{} {}",
        control.action,
        if control.reply { " reply" } else { "" },
        control.call_id
    )
}

fn check(sc: &Scenario, seed: u64) {
    let mut sim = Sim::new(sc, seed);
    if let Err(problems) = sim.judge() {
        let tail = sim.trace.len().saturating_sub(400);
        panic!(
            "scenario '{}' seed {seed} failed:\n{problems}\n--- scenario ---\n{sc:#?}\n--- trace (last {}) ---\n{}",
            sc.name,
            sim.trace.len() - tail,
            sim.trace[tail..].join("\n")
        );
    }
}

fn random_scenario(seed: u64) -> Scenario {
    let mut rng = Rng::new(seed ^ 0xA5A5);
    let members = rng.range(2, 15) as usize;
    let mut sc = Scenario::new("random", members);
    sc.auto_close = rng.chance(0.7);
    sc.latency_ms = (rng.range(1, 50), rng.range(50, 800));
    sc.loss = if rng.chance(0.5) { rng.below(25) as f64 / 100.0 } else { 0.0 };
    sc.duplicate = if rng.chance(0.3) { rng.below(15) as f64 / 100.0 } else { 0.0 };
    sc.loss_detect_ms = rng.range(1_000, 15_000);
    if rng.chance(0.3) {
        sc.skew_ms = rng.range(1_000, 300_000);
    }
    if rng.chance(0.2) {
        sc.restart_jump_ms = rng.range(1_000, 600_000);
    }
    if rng.chance(0.1) {
        sc.loss = rng.range(30, 60) as f64 / 100.0;
    }
    if rng.chance(0.15) {
        // Room lists that disagree: each member drops some others from its view.
        let views = (0..members)
            .map(|me| (0..members).filter(|&o| o == me || !rng.chance(0.15)).collect())
            .collect();
        sc.views = Some(views);
    }
    let horizon = rng.range(2_000, 40_000);
    if rng.chance(0.25) {
        let from = rng.below(horizon);
        let side = (0..members).filter(|_| rng.chance(0.5)).collect();
        sc.partitions.push(Partition { from_ms: from, until_ms: from + rng.range(1_000, 80_000), side });
    }
    let actions = rng.range(1, members as u64 * 4);
    for _ in 0..actions {
        let at = rng.below(horizon);
        let who = rng.below(members as u64) as usize;
        let act = match rng.below(100) {
            0..=54 => Act::Join,
            55..=79 => Act::Leave,
            80..=89 => Act::Crash,
            _ => Act::Restart,
        };
        sc.script.push((at, who, act));
    }
    // Simultaneous starts are common in practice: often begin with a burst.
    if rng.chance(0.3) {
        for who in 0..members.min(rng.range(2, 5) as usize) {
            sc.script.push((rng.below(50), who, Act::Join));
        }
    }
    // Everyone crashed comes back eventually, so judging sees live members.
    for who in 0..members {
        sc.script.push((horizon + 1_000, who, Act::Restart));
    }
    sc
}

// ---- Named scenarios ----

fn joins(sc: &mut Scenario, list: &[(u64, usize)]) {
    sc.script.extend(list.iter().map(|&(t, w)| (t, w, Act::Join)));
}

#[test]
fn sequential_joins_then_leaves() {
    let mut sc = Scenario::new("sequential", 5);
    sc.settle_ms = 3_000; // lossless: converges without refresh or expiry
    joins(&mut sc, &[(0, 0), (1_000, 1), (2_000, 2), (3_000, 3), (4_000, 4)]);
    sc.script.extend([(10_000, 2, Act::Leave), (11_000, 4, Act::Leave)]);
    check(&sc, 1);
}

#[test]
fn everyone_starts_at_once() {
    let mut sc = Scenario::new("simultaneous", 6);
    sc.settle_ms = 3_000; // lossless: converges without refresh or expiry
    joins(&mut sc, &(0..6).map(|w| (0, w)).collect::<Vec<_>>());
    check(&sc, 2);
}

#[test]
fn thirteen_join_at_once() {
    let mut sc = Scenario::new("cap race", 13);
    sc.settle_ms = 3_000; // lossless: converges without refresh or expiry
    joins(&mut sc, &(0..13).map(|w| (w as u64 % 3, w)).collect::<Vec<_>>());
    check(&sc, 3);
}

#[test]
fn twelve_then_a_thirteenth() {
    let mut sc = Scenario::new("cap", 13);
    sc.settle_ms = 3_000; // lossless: converges without refresh or expiry
    joins(&mut sc, &(0..12).map(|w| (w as u64 * 300, w)).collect::<Vec<_>>());
    joins(&mut sc, &[(10_000, 12)]);
    check(&sc, 4);
}

#[test]
fn a_seat_frees_and_the_refused_member_retries() {
    let mut sc = Scenario::new("cap retry", 13);
    joins(&mut sc, &(0..12).map(|w| (w as u64 * 300, w)).collect::<Vec<_>>());
    joins(&mut sc, &[(10_000, 12)]);
    sc.script.push((15_000, 3, Act::Leave));
    joins(&mut sc, &[(20_000, 12)]);
    check(&sc, 5);
}

#[test]
fn a_crashed_member_without_a_leave() {
    let mut sc = Scenario::new("crash", 4);
    joins(&mut sc, &[(0, 0), (500, 1), (1_000, 2), (1_500, 3)]);
    sc.script.push((5_000, 2, Act::Crash));
    check(&sc, 6);
}

#[test]
fn a_crashed_member_comes_back() {
    let mut sc = Scenario::new("crash and back", 4);
    joins(&mut sc, &[(0, 0), (500, 1), (1_000, 2), (1_500, 3)]);
    sc.script.extend([(5_000, 2, Act::Crash), (8_000, 2, Act::Restart)]);
    check(&sc, 7);
}

#[test]
fn lossy_network() {
    for seed in 0..50 {
        let mut sc = Scenario::new("lossy", 6);
        sc.loss = 0.2;
        sc.duplicate = 0.1;
        sc.latency_ms = (5, 900);
        joins(&mut sc, &(0..6).map(|w| (w as u64 * 200, w)).collect::<Vec<_>>());
        sc.script.push((5_000, 1, Act::Leave));
        check(&sc, seed);
    }
}

#[test]
fn a_partition_heals() {
    let mut sc = Scenario::new("partition", 6);
    sc.partitions.push(Partition { from_ms: 0, until_ms: 10_000, side: [0, 1, 2].into() });
    joins(&mut sc, &(0..6).map(|w| (w as u64 * 100, w)).collect::<Vec<_>>());
    check(&sc, 8);
}

#[test]
fn the_last_two_leave_together() {
    let mut sc = Scenario::new("empty", 3);
    sc.settle_ms = 3_000; // lossless: converges without refresh or expiry
    joins(&mut sc, &[(0, 0), (500, 1), (1_000, 2)]);
    sc.script.extend([(5_000, 0, Act::Leave), (5_000, 1, Act::Leave), (5_001, 2, Act::Leave)]);
    check(&sc, 9);
}

#[test]
fn keep_open_room_is_re_entered() {
    let mut sc = Scenario::new("keep open", 3);
    sc.settle_ms = 3_000; // lossless: converges without refresh or expiry
    sc.auto_close = false;
    joins(&mut sc, &[(0, 0), (500, 1)]);
    sc.script.extend([(3_000, 0, Act::Leave), (3_500, 1, Act::Leave)]);
    joins(&mut sc, &[(6_000, 2), (6_500, 0)]);
    check(&sc, 10);
}

#[test]
fn rejoin_quickly_after_leaving() {
    let mut sc = Scenario::new("flap", 4);
    sc.settle_ms = 3_000; // lossless: converges without refresh or expiry
    joins(&mut sc, &[(0, 0), (100, 1), (200, 2)]);
    for k in 0..10 {
        sc.script.push((1_000 + k * 150, 1, if k % 2 == 0 { Act::Leave } else { Act::Join }));
    }
    check(&sc, 11);
}

#[test]
fn random_runs() {
    let runs: u64 = std::env::var("ROOM_CALL_SIM_RUNS").ok().and_then(|v| v.parse().ok()).unwrap_or(2_000);
    if let Some(seed) = std::env::var("ROOM_CALL_SIM_SEED").ok().and_then(|v| v.parse().ok()) {
        return check(&random_scenario(seed), seed);
    }
    let mut failures = Vec::new();
    for seed in 0..runs {
        let sc = random_scenario(seed);
        let mut sim = Sim::new(&sc, seed);
        if let Err(problems) = sim.judge() {
            failures.push((seed, problems.lines().next().unwrap_or("").to_string()));
        }
    }
    if !failures.is_empty() {
        let mut kinds: BTreeMap<String, Vec<u64>> = BTreeMap::new();
        for (seed, problem) in &failures {
            // The kind of failure, without member names or sets.
            let kind: String = problem
                .split_whitespace()
                .skip(1)
                .filter(|w| !w.contains("m0") && !w.contains("m1") && !w.contains('{') && !w.contains('"'))
                .take(4)
                .collect::<Vec<_>>()
                .join(" ");
            kinds.entry(kind).or_default().push(*seed);
        }
        let summary: Vec<String> = kinds
            .iter()
            .map(|(kind, seeds)| format!("  {:>4}x {kind} (e.g. seed {})", seeds.len(), seeds[0]))
            .collect();
        panic!(
            "{} of {runs} random runs failed; replay one with ROOM_CALL_SIM_SEED=<seed>:\n{}",
            failures.len(),
            summary.join("\n")
        );
    }
}

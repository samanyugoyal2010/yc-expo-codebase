const $ = (id) => document.getElementById(id);
let current = null;
let auto = null;
const inflight = new Map(); // receipt -> delivery
 
async function api(path, opts = {}) {
  const res = await fetch(path, {
    headers: { "Content-Type": "application/json" },
    ...opts,
  });
  const text = await res.text();
  const body = text ? JSON.parse(text) : null;
  if (!res.ok) throw new Error((body && body.error) || res.statusText);
  return body;
}
 
function log(msg, kind = "") {
  const el = $("log");
  el.textContent = msg;
  el.className = "log " + kind;
}
 
async function refreshQueues() {
  const queues = await api("/v1/queues");
  const list = $("queue-list");
  list.innerHTML = "";
  for (const q of queues) {
    const li = document.createElement("li");
    li.className = q.name === current ? "active" : "";
    li.innerHTML = `<span>${q.name}</span><span class="tag">${q.config.order}</span>
      <span class="meta">${q.stats.ready}R ${q.stats.delayed}D ${q.stats.inflight}I</span>`;
    li.onclick = () => select(q.name);
    list.appendChild(li);
  }
  if (current && !queues.some((q) => q.name === current)) select(null);
}
 
async function select(name) {
  current = name;
  stopAuto();
  inflight.clear();
  renderInflight();
  $("detail").hidden = !name;
  if (name) {
    const info = await api(`/v1/queues/${name}`);
    $("detail-name").textContent = name;
    $("detail-order").textContent = info.config.order;
  }
  await refreshQueues();
  await refreshDetail();
}
 
async function refreshDetail() {
  if (!current) return;
  const stats = await api(`/v1/queues/${current}/stats`);
  $("stats").innerHTML = [
    ["ready", stats.ready], ["delayed", stats.delayed], ["inflight", stats.inflight],
    ["dead", stats.dead], ["enqueued", stats.enqueued_total], ["delivered", stats.delivered_total],
    ["acked", stats.acked_total], ["nacked", stats.nacked_total], ["expired", stats.expired_total],
    ["replayed", stats.replayed_total],
  ].map(([k, v]) => `<div class="stat"><b>${v}</b><span>${k}</span></div>`).join("");
 
  const dead = await api(`/v1/queues/${current}/dead?limit=50`);
  $("dead").innerHTML = "";
  for (const m of dead.messages) {
    const li = document.createElement("li");
    li.innerHTML = `<span class="body">${escapeHTML(m.body)}</span>
      <span class="meta">id ${m.id} · p${m.priority} · ${m.attempts} attempts</span>`;
    const btn = document.createElement("button");
    btn.className = "ghost small";
    btn.textContent = "Replay";
    btn.onclick = async () => {
      await api(`/v1/queues/${current}/replay`, { method: "POST", body: JSON.stringify({ ids: [m.id] }) });
      log(`replayed ${m.id}`, "ok");
      refreshDetail();
    };
    li.appendChild(btn);
    $("dead").appendChild(li);
  }
}
 
function escapeHTML(s) {
  return s.replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));
}
 
function renderInflight() {
  const ul = $("inflight");
  ul.innerHTML = "";
  for (const [receipt, d] of inflight) {
    const li = document.createElement("li");
    li.innerHTML = `<span class="body">${escapeHTML(d.body)}</span>
      <span class="meta">id ${d.id} · p${d.priority} · attempt ${d.attempt}</span>`;
    const row = document.createElement("span");
    row.className = "row";
    const ack = document.createElement("button");
    ack.className = "small";
    ack.textContent = "Ack";
    ack.onclick = () => finish("ack", receipt, {});
    const nack = document.createElement("button");
    nack.className = "danger small";
    nack.textContent = "Nack";
    nack.onclick = () => finish("nack", receipt, { delay_ms: 1000 });
    row.append(ack, nack);
    li.appendChild(row);
    ul.appendChild(li);
  }
}
 
async function finish(action, receipt, extra) {
  try {
    await api(`/v1/queues/${current}/${action}`, {
      method: "POST",
      body: JSON.stringify({ receipt, ...extra }),
    });
    inflight.delete(receipt);
    renderInflight();
    log(`${action} ok`, "ok");
  } catch (e) {
    // A 409 means the lease expired and someone else owns the message now.
    inflight.delete(receipt);
    renderInflight();
    log(`${action} failed: ${e.message}`, "err");
  }
  refreshDetail();
}
 
async function leaseOnce() {
  const body = JSON.stringify({
    max: Number($("c-max").value),
    wait_ms: Number($("c-wait").value),
  });
  const res = await api(`/v1/queues/${current}/lease`, { method: "POST", body });
  for (const d of res.messages) inflight.set(d.receipt, d);
  renderInflight();
  refreshDetail();
  return res.messages.length;
}
 
function stopAuto() {
  if (auto) clearInterval(auto);
  auto = null;
  $("auto-btn").textContent = "Start auto-consumer";
}
 
$("create-form").onsubmit = async (e) => {
  e.preventDefault();
  try {
    await api("/v1/queues", {
      method: "POST",
      body: JSON.stringify({
        name: $("q-name").value.trim(),
        order: $("q-order").value,
        visibility_ms: Number($("q-vis").value),
        max_attempts: Number($("q-attempts").value),
        age_boost_ms: Number($("q-boost").value),
      }),
    });
    await select($("q-name").value.trim());
    $("q-name").value = "";
    log("queue created", "ok");
  } catch (err) {
    log(err.message, "err");
  }
};
 
$("enqueue-form").onsubmit = async (e) => {
  e.preventDefault();
  try {
    await api(`/v1/queues/${current}/messages`, {
      method: "POST",
      body: JSON.stringify({
        body: $("m-body").value,
        priority: Number($("m-priority").value),
        delay_ms: Number($("m-delay").value),
      }),
    });
    $("m-body").value = "";
    log("enqueued", "ok");
    refreshDetail();
  } catch (err) {
    log(err.message, "err");
  }
};
 
$("burst").onclick = async () => {
  const messages = Array.from({ length: 10 }, (_, i) => ({
    body: `job-${Date.now() % 100000}-${i}`,
    priority: Math.floor(Math.random() * 5),
    delay_ms: i % 3 === 0 ? 2000 : 0,
  }));
  await api(`/v1/queues/${current}/messages`, { method: "POST", body: JSON.stringify({ messages }) });
  log("enqueued 10 (every third delayed 2s)", "ok");
  refreshDetail();
};
 
$("lease-btn").onclick = async () => {
  try {
    const n = await leaseOnce();
    log(n ? `leased ${n}` : "nothing ready", n ? "ok" : "");
  } catch (err) {
    log(err.message, "err");
  }
};
 
$("auto-btn").onclick = () => {
  if (auto) return stopAuto();
  $("auto-btn").textContent = "Stop auto-consumer";
  auto = setInterval(async () => {
    try {
      const res = await api(`/v1/queues/${current}/lease`, {
        method: "POST",
        body: JSON.stringify({ max: 1, wait_ms: 0 }),
      });
      for (const d of res.messages) {
        await api(`/v1/queues/${current}/ack`, { method: "POST", body: JSON.stringify({ receipt: d.receipt }) });
        log(`auto-consumed ${d.body}`, "ok");
      }
      refreshDetail();
    } catch (err) {
      log(err.message, "err");
    }
  }, 500);
};
 
$("replay-all").onclick = async () => {
  await api(`/v1/queues/${current}/replay`, { method: "POST", body: JSON.stringify({}) });
  log("replayed all dead letters", "ok");
  refreshDetail();
};
 
refreshQueues();
setInterval(() => {
  refreshQueues().catch(() => {});
  refreshDetail().catch(() => {});
}, 2000);

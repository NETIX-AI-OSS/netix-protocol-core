// Deletes the configured workspace paths when the job ends and prints how much room it gave back.
const fs = require('fs');
const path = require('path');

const workspace = process.env.GITHUB_WORKSPACE;
const paths = (process.env['INPUT_PATHS'] || 'target').split('\n').map((p) => p.trim()).filter(Boolean);

function size(p) {
  let total = 0;
  const stack = [p];
  while (stack.length) {
    const cur = stack.pop();
    let st;
    try { st = fs.lstatSync(cur); } catch { continue; }
    if (st.isDirectory()) {
      for (const e of fs.readdirSync(cur)) stack.push(path.join(cur, e));
    } else {
      total += st.size;
    }
  }
  return total;
}

for (const rel of paths) {
  const abs = path.resolve(workspace, rel);
  if (abs !== workspace && !abs.startsWith(workspace + path.sep)) {
    console.log(`::warning::clean-workspace: ${rel} is outside the workspace, skipped`);
    continue;
  }
  if (!fs.existsSync(abs)) continue;
  const bytes = size(abs);
  try {
    fs.rmSync(abs, { recursive: true, force: true, maxRetries: 3 });
    console.log(`clean-workspace: deleted ${rel} (${(bytes / 2 ** 30).toFixed(2)} GiB)`);
  } catch (e) {
    // Never fail the job over cleanup; the next checkout's git clean still removes it.
    console.log(`::warning::clean-workspace: could not delete ${rel}: ${e.message}`);
  }
}
